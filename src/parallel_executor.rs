extern crate fibers;
extern crate futures;
extern crate trackable;

use fibers::sync::mpsc;
use futures::future::Either;
use futures::{Async, Future, Poll, Stream};

/// ParallelExecutorで用いる3つの内部状態。
///
/// + Processing: 通常状態。問題なく処理が進行している。
/// + NoTasks: 処理対象のstreamから値を全て取得してspawn済みである。ただしspawnしたが終了していないものがある。
/// + Finished: NoTasksの後に遷移する状態であり、spawn済みのfutureたちから値を回収しきったことを意味する。
#[derive(PartialEq, Debug)]
enum InnerState {
    Progress,
    NoTasks,
    Finished,
}

/// ParallelExecutorが発生しうる２つのエラーの列挙体。
///
/// + `PollError(error)`: `ParallelExecutor`が抱えるstreamそのものをpollした際に生じたエラー
/// + `InnerFutureError(error)`: `ParallelExecutor`が抱えるstreamをpollして得たfutureを実行した際に生じたエラー
pub enum ParallelExecutorError<E1, E2> {
    PollError(E1),
    InnerFutureError(E2),
}

/// 与えられたStream `stream` から、値`v`を順次取り出し、
/// 値からfutureへの射 `map` を通して新たなfuture `map(v)`を作り、
/// それらを並列度 `concurrency` のもとで実行するための構造体。
///
/// この構造体そのものが Stream となり、pollを用いたインタフェイスで操作することができる。
///
/// # 並列度`concurrency`に関する注意
/// `ParallelExecutor`は、ある瞬間に最大`concurrency`個の実行結果を持つことを目指している。  
/// したがって、`concurrency`個のfuturekをspawnし、その全ての実行が終了したという状況で、
/// `ParallelExecutor::poll`が呼び出されずに値がconsumeされない場合には、
/// 内部streamに対する`poll`を行わない。
pub struct ParallelExecutor<Handler: fibers::Spawn, T, E, Map, Strm> {
    handle: Handler,
    concurrency: usize,

    // streamから取り出した値からfutureを作る射
    map: Map,

    // spawnはしたが値を受け取っていないfutureの総数
    // 0 <= spawned_futures < concurrencyを満たす
    spawned_futures: usize,

    // spawn済みのfutureとの間でやり取りするためのchannel
    // T=futureの正常値の型; E=futureのエラーの型
    sender: mpsc::Sender<Either<T, E>>,
    receiver: mpsc::Receiver<Either<T, E>>,

    stream: Strm,

    // spawnしたfutureの実行結果を蓄えるバッファ
    buffer: Vec<T>,

    state: InnerState,
}

impl<Handler, T1, E1, T2, E2, F, Map, Strm> ParallelExecutor<Handler, T2, E2, Map, Strm>
where
    Handler: fibers::Spawn,
    T1: Send + 'static,
    E1: Send + 'static,
    T2: Send + 'static,
    E2: Send + 'static,
    F: Future<Item = T2, Error = E2> + Send + 'static,
    Map: FnMut(T1) -> F,
    Strm: Stream<Item = T1, Error = E1>,
{
    /// `ParallelExecutor`のコンストラクタ。
    ///
    /// `stream`から値を取り出し、これに`map`を適用して得られる`future`たちを、
    /// `handle`を用いて`concurrency`並列度のもとで順次実行していく。
    pub fn map(
        handle: Handler,
        concurrency: usize,
        stream: Strm,
        map: Map,
    ) -> ParallelExecutor<Handler, T2, E2, Map, Strm> {
        let (sender, receiver) = mpsc::channel();
        ParallelExecutor {
            spawned_futures: 0,
            handle,
            concurrency,
            sender,
            receiver,
            stream,
            buffer: Vec::new(),
            state: InnerState::Progress,
            map,
        }
    }
}

impl<Handler, T1, E1, T2, E2, F, Map, Strm> Stream for ParallelExecutor<Handler, T2, E2, Map, Strm>
where
    Handler: fibers::Spawn,
    T1: Send + 'static,
    E1: Send + 'static,
    T2: Send + 'static,
    E2: Send + 'static,
    F: Future<Item = T2, Error = E2> + Send + 'static,
    Map: FnMut(T1) -> F,
    Strm: Stream<Item = T1, Error = E1>,
{
    type Item = T2;
    type Error = ParallelExecutorError<E1, E2>;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        if self.state == InnerState::Finished {
            return Ok(Async::Ready(None));
        }

        loop {
            // spawn済みfutureからの返信処理
            match self.receiver.poll() {
                Ok(Async::Ready(Some(v_or_e))) => {
                    self.spawned_futures -= 1;
                    match v_or_e {
                        Either::A(v) => self.buffer.push(v), // 計算が成功した場合
                        Either::B(e) => return Err(ParallelExecutorError::InnerFutureError(e)), // エラーが発生した場合
                    }
                }
                Ok(Async::Ready(None)) => {
                    // このreceiverがこの先に値を受け取ることができない場合。
                    // このreceiverを以後使えない場合。
                    // この構造体ではreceiverの対になるsenderをholdしているので
                    // ここに来ることはないはずである。
                    unreachable!(
                        r###"
The self.receiver becomes the state `disconnected`.
However, we should not reach here since we hold the corresponding sender.
"###
                    );
                }
                Ok(Async::NotReady) => break,
                Err(_) => {
                    // https://docs.rs/fibers/0.1.12/fibers/sync/mpsc/struct.Receiver.html
                    // Receiverはエラーを返さないということなので、unreachableとする
                    unreachable!("mpsc::Receiver should not return errors");
                }
            }
        }

        while self.state == InnerState::Progress
            && (self.buffer.len() + self.spawned_futures) < self.concurrency
        // バッファ長がconcurrencyを超えないための調整
        {
            match self.stream.poll() {
                Ok(Async::Ready(Some(value))) => {
                    // streamからの値の取り出しに成功した場合
                    let tx = self.sender.clone();
                    // TODO: 本来であればspawnした先でfutureを作った方が良い
                    let future = (self.map)(value);
                    let future = future.then(move |v_or_e| match v_or_e {
                        Ok(v) => {
                            let _ = tx.send(Either::A(v));
                            Ok(())
                        }
                        Err(e) => {
                            let _ = tx.send(Either::B(e));
                            Err(())
                        }
                    });
                    self.handle.spawn(future);
                    self.spawned_futures += 1;
                }
                Ok(Async::Ready(None)) => {
                    // 全ての値を取り出し終わっている場合
                    self.state = InnerState::NoTasks;
                    break;
                }
                Ok(Async::NotReady) => {
                    // 取り出せる値がない場合
                    break;
                }
                Err(e2) => {
                    // stream側で値の生成時にエラーが生じた場合
                    return Err(ParallelExecutorError::PollError(e2));
                }
            }
        }

        if let Some(v) = self.buffer.pop() {
            Ok(Async::Ready(Some(v)))
        } else if self.spawned_futures == 0 && self.state == InnerState::NoTasks {
            self.state = InnerState::Finished;
            Ok(Async::Ready(None))
        } else {
            Ok(Async::NotReady)
        }
    }
}

#[cfg(test)]
mod tests {
    use self::CollectorError::*;
    use super::super::collector::{Collector, CollectorError};
    use super::ParallelExecutorError::*;
    use super::*;
    use fibers::time::timer;
    use fibers::{Executor, InPlaceExecutor, ThreadPoolExecutor};
    use futures::future;
    use std::time::Duration;
    use trackable::result::TestResult;

    fn id<Arg>(arg: Arg) -> Arg {
        arg
    }

    #[test]
    fn parallel_executor_as_map() -> TestResult {
        let v: Vec<usize> = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let stream = futures::stream::iter_ok::<_, ()>(v);

        let mut executor = ThreadPoolExecutor::new().unwrap();

        let pr1 = ParallelExecutor::map(executor.handle(), 1, stream, |x: usize| {
            future::ok::<usize, ()>(x * x)
        });
        let pr2 = ParallelExecutor::map(executor.handle(), 2, pr1, |x: usize| {
            future::ok::<usize, ()>(x * x)
        });
        let pr3 = ParallelExecutor::map(executor.handle(), 4, pr2, |x: usize| {
            future::ok::<usize, ()>(x * x)
        });
        let future = Collector::new(pr3);

        let mut result = executor.run_future(future).unwrap().unwrap();
        result.sort();
        assert_eq!(
            result,
            vec![1, 256, 6561, 65536, 390625, 1679616, 5764801, 16777216, 43046721, 100000000]
        );

        Ok(())
    }

    fn secs(s: u64) -> Duration {
        Duration::from_secs(s)
    }

    fn millis(s: u64) -> Duration {
        Duration::from_millis(s)
    }

    // 与えられたduration時刻経過すると
    // 値もしくはエラーを生成する。
    // 値を生成したい場合は TestFuture::new を使ってこの構造体を作り、
    // エラーを生成したい場合は TestFuture::err を使ってこの構造体を作る。
    struct TestFuture {
        inner: String,
        error: bool,
        duration: Duration,
        timer: Option<timer::Timeout>, // timerは、このFutureが初めてpollされた段階で、`duration`期限で動く
    }
    impl TestFuture {
        pub fn new(inner: &str, duration: Duration) -> Self {
            TestFuture {
                inner: inner.to_owned(),
                error: false,
                duration,
                timer: None,
            }
        }

        pub fn err(inner: &str, duration: Duration) -> Self {
            TestFuture {
                inner: inner.to_owned(),
                error: true,
                duration,
                timer: None,
            }
        }
    }
    impl Future for TestFuture {
        type Item = String;
        type Error = String;

        fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
            if let Some(ref mut timer) = self.timer {
                match timer.poll() {
                    Ok(Async::Ready(_)) => {
                        if !self.error {
                            Ok(Async::Ready(self.inner.clone()))
                        } else {
                            Err(self.inner.clone())
                        }
                    }
                    Ok(Async::NotReady) => Ok(Async::NotReady),
                    Err(_) => unreachable!("In this test module, we should not reach here"),
                }
            } else {
                self.timer = Some(timer::timeout(self.duration));
                Ok(Async::NotReady)
            }
        }
    }

    fn convert_to_tasks<T, E, F: Future<Item = T, Error = E>>(
        v: Vec<F>,
    ) -> impl Stream<Item = F, Error = ()> {
        futures::stream::iter_ok::<_, ()>(v)
    }

    // 並列度1で、4つのtaskからなるtask streamを実行する。
    // 各taskは1秒要するので、総計で4秒必要となる。
    #[test]
    fn check_concurrency_level1_works() -> TestResult {
        let mut executor = ThreadPoolExecutor::new().unwrap();
        let task = convert_to_tasks(vec![
            TestFuture::new("A", secs(1)),
            TestFuture::new("B", secs(1)),
            TestFuture::new("C", secs(1)),
            TestFuture::new("D", secs(1)),
        ]);

        let pr = ParallelExecutor::map(executor.handle(), 1, task, id);

        let future = Collector::new(pr);

        let start = std::time::Instant::now();
        let mut result = executor.run_future(future).unwrap().unwrap();
        result.sort();
        assert_eq!(result, ["A", "B", "C", "D"]);
        let end = start.elapsed();
        let exec_time = end.as_millis();
        // 5秒以内には終わるだろうという雑な値
        assert!(4000 <= exec_time && exec_time <= 5000);

        Ok(())
    }

    // 並列度4で、4つのtaskからなるtask streamを実行する。
    // 各taskは1秒要するので、1秒あれば良い。
    #[test]
    fn check_concurrency_level4_works() -> TestResult {
        let mut executor = ThreadPoolExecutor::new().unwrap();
        let task = convert_to_tasks(vec![
            TestFuture::new("A", secs(1)),
            TestFuture::new("B", secs(1)),
            TestFuture::new("C", secs(1)),
            TestFuture::new("D", secs(1)),
        ]);

        let pr = ParallelExecutor::map(executor.handle(), 4, task, id);
        let future = Collector::new(pr);

        let start = std::time::Instant::now();
        let mut result = executor.run_future(future).unwrap().unwrap();
        result.sort();
        assert_eq!(result, ["A", "B", "C", "D"]);
        let end = start.elapsed();
        let exec_time = end.as_millis();
        // 2秒以内には終わるだろうという雑な値
        assert!(1000 <= exec_time && exec_time <= 2000);

        Ok(())
    }

    #[test]
    fn one_level_parallel_executor_with_error() -> TestResult {
        let mut executor = ThreadPoolExecutor::new().unwrap();
        let task = convert_to_tasks(vec![
            TestFuture::new("A", secs(1)),
            TestFuture::new("B", secs(1)),
            TestFuture::err("C", millis(200)),
            TestFuture::new("D", secs(1)),
        ]);

        let pr = ParallelExecutor::map(executor.handle(), 4, task, id);
        let future = Collector::new(pr);

        let start = std::time::Instant::now();
        let result = executor.run_future(future).unwrap();
        let error = result.unwrap_err();

        let mut continuation = None;
        assert!(match error {
            CollectorError::InnerError(InnerFutureError(err), values, rest) => {
                continuation = Some(rest);
                err == "C" && values.is_empty()
            }
            _ => false,
        });

        let future = Collector::new(continuation.unwrap());
        let mut result = executor.run_future(future).unwrap().unwrap();
        result.sort();
        assert_eq!(result, ["A", "B", "D"]);
        let end = start.elapsed();
        let exec_time = end.as_millis();
        // 2秒以内には終わるだろうという雑な値
        assert!(1000 <= exec_time && exec_time <= 2000);

        Ok(())
    }

    // TestFutureを生成する構造体で、これ自体がFutureになっている。
    // Generator::new(duration, closure)で作られた場合は
    // duration時刻経過した段階でclosureを実行してTestFutureを作る。
    // Generator::err(duration, msg)で作られた場合は
    // duration時刻経過した段階でエラーとしてmsgを返す。
    struct Generator {
        duration: Duration,
        generator: Box<Fn() -> TestFuture + Send + 'static>,
        timer: Option<timer::Timeout>, // timerは、このFutureが初めてpollされた段階で、duration時刻を設定して動き始める
        error: Option<String>,
    }
    impl Generator {
        pub fn new<T: Fn() -> TestFuture + Send + 'static>(
            duration: Duration,
            generator: T,
        ) -> Self {
            Generator {
                duration,
                generator: Box::new(generator),
                timer: None,
                error: None,
            }
        }
        pub fn err(duration: Duration, msg: &str) -> Self {
            Generator {
                duration,
                generator: Box::new(|| TestFuture::new("dummy", Duration::from_secs(0))), // 実際には使わないダミーfuture
                timer: None,
                error: Some(msg.to_owned()),
            }
        }
    }
    impl Future for Generator {
        type Item = TestFuture;
        type Error = String;

        fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
            if let Some(ref mut timer) = self.timer {
                match timer.poll() {
                    Ok(Async::Ready(_)) => {
                        if let Some(err_msg) = &self.error {
                            Err(err_msg.to_owned())
                        } else {
                            Ok(Async::Ready((self.generator)()))
                        }
                    }
                    Ok(Async::NotReady) => Ok(Async::NotReady),
                    Err(_) => Err("timer.poll".to_owned()),
                }
            } else {
                self.timer = Some(timer::timeout(self.duration));
                Ok(Async::NotReady)
            }
        }
    }

    /*
    ParallelExecutorを二段建てた上で、
    concurrencyパラメータを変更して実行速度を計測することで、
    パラメータが有効であることを調べる。
     */
    #[test]
    fn two_level_parallel_executor_works1() -> TestResult {
        let mut executor = InPlaceExecutor::new().unwrap();

        /*
        1段目(pr1)で4並列4オブジェクトを生成し、
        2段目(pr2)で1並列で消費する。

        . 最初の1秒で1段目の処理が全て終わり4つ登録され、
        . 次の最初の1秒で2段目が1つ消費する。
        . 次の1秒で2段目が1つ消費する。
        . 次の1秒で2段目が1つ消費する。
        . 次の1秒で2段目が1つ消費する。
        したがって、合計で5秒程度必要となる。
         */

        let task = convert_to_tasks(vec![
            Generator::new(secs(1), || TestFuture::new("A", secs(1))),
            Generator::new(secs(1), || TestFuture::new("B", secs(1))),
            Generator::new(secs(1), || TestFuture::new("C", secs(1))),
            Generator::new(secs(1), || TestFuture::new("D", secs(1))),
        ]);

        let pr1 = ParallelExecutor::map(executor.handle(), 4, task, id);
        let pr2 = ParallelExecutor::map(executor.handle(), 1, pr1, id);

        let start = std::time::Instant::now();
        let mut result = executor.run_future(Collector::new(pr2)).unwrap().unwrap();
        result.sort();
        assert_eq!(result, ["A", "B", "C", "D"]);
        let end = start.elapsed();
        let exec_time = end.as_millis();
        // 6秒以内には終わるだろうという雑な値
        assert!(5000 <= exec_time && exec_time <= 6000);

        Ok(())
    }

    #[test]
    fn two_level_parallel_executor_works2() -> TestResult {
        let mut executor = InPlaceExecutor::new().unwrap();

        /*
        1段目(pr1)で4並列4オブジェクトを生成し、
        2段目(pr2)で2並列で消費する。

        . 最初の1秒で1段目の処理が全て終わり4つ登録され、
        . 次の最初の1秒で2段目が2つ消費する。
        . 次の1秒で2段目が2つ消費する。
        したがって、合計で3秒程度必要となる。
         */

        let task = convert_to_tasks(vec![
            Generator::new(secs(1), || TestFuture::new("A", secs(1))),
            Generator::new(secs(1), || TestFuture::new("B", secs(1))),
            Generator::new(secs(1), || TestFuture::new("C", secs(1))),
            Generator::new(secs(1), || TestFuture::new("D", secs(1))),
        ]);

        let pr1 = ParallelExecutor::map(executor.handle(), 4, task, id);
        let pr2 = ParallelExecutor::map(executor.handle(), 2, pr1, id);
        let future = Collector::new(pr2);

        let start = std::time::Instant::now();
        let mut result = executor.run_future(future).unwrap().unwrap();
        result.sort();
        assert_eq!(result, ["A", "B", "C", "D"]);
        let end = start.elapsed();
        let exec_time = end.as_millis();
        // 4秒以内には終わるだろうという雑な値
        assert!(3000 <= exec_time && exec_time <= 4000);

        Ok(())
    }

    #[test]
    fn two_level_parallel_executor_works3() -> TestResult {
        /*
        1段目(pr1)で4並列6オブジェクトを生成し、
        2段目(pr2)で2並列で消費する。

        . 最初の1秒で1段目の処理が4つ終わり4つ登録される。2つ残っていることに注意。
        . 次の最初の1秒で2段目が2つ消費する。同時に1段目の処理が走り、残った2つが登録される。
        . 次の1秒で2段目が2つ消費する。
        . 次の1秒で2段目が2つ消費する。
        したがって、合計で4秒程度必要となる。
         */
        let mut executor = InPlaceExecutor::new().unwrap();

        let task = convert_to_tasks(vec![
            Generator::new(secs(1), || TestFuture::new("A", secs(1))),
            Generator::new(secs(1), || TestFuture::new("B", secs(1))),
            Generator::new(secs(1), || TestFuture::new("C", secs(1))),
            Generator::new(secs(1), || TestFuture::new("D", secs(1))),
            Generator::new(secs(1), || TestFuture::new("E", secs(1))),
            Generator::new(secs(1), || TestFuture::new("F", secs(1))),
        ]);

        let pr1 = ParallelExecutor::map(executor.handle(), 4, task, id);
        let pr2 = ParallelExecutor::map(executor.handle(), 2, pr1, id);
        let future = Collector::new(pr2);

        let start = std::time::Instant::now();
        let mut result = executor.run_future(future).unwrap().unwrap();
        result.sort();
        assert_eq!(result, ["A", "B", "C", "D", "E", "F"]);
        let end = start.elapsed();
        let exec_time = end.as_millis();
        assert!(4000 <= exec_time && exec_time <= 5000);

        Ok(())
    }

    #[test]
    fn two_level_parallel_executor_works4() -> TestResult {
        /*
        1段目(pr1)で1並列で3オブジェクトを生成し、
        2段目(pr2)で1並列で消費する。

        A) 最初の1秒で1段目の処理が1つ終わり1つ登録される。2つタスクが残っていることに注意。
        B) 次の3秒で2段目が1つ消費する。
        C) 消費し終わったあとに、1秒使って1段目の処理が終わり、次のタスクが追加される。
        .. 直感的には2段目に余裕ができて初めて1段目の計算が行われる要求型の計算である。
        .. こうしなければ、2段目の計算に時間がかかるものの1段目の計算が早急に終り、かつメモリを大量に消費する場合に
        メモリの大量消費が発生してしまう。
        D) 次の3秒で2段目が1つ消費する。
        E) 次の1秒で1段目が1つ追加する。
        F) 次の3秒で2段目が1つ消費する。
        したがって、1+3+1+3+1+3=12秒ほど必要になる。

        もし2段目の処理中に1段目の処理を無制限にやる場合は、
        この場合はBの処理中に、1段目が2つタスクを追加できることになるので、
        1+3+3+3=10秒で済む。
         */

        let mut executor = InPlaceExecutor::new().unwrap();
        let task = convert_to_tasks(vec![
            Generator::new(secs(1), || TestFuture::new("A", secs(3))),
            Generator::new(secs(1), || TestFuture::new("B", secs(3))),
            Generator::new(secs(1), || TestFuture::new("C", secs(3))),
        ]);

        let pr1 = ParallelExecutor::map(executor.handle(), 1, task, id);
        let pr2 = ParallelExecutor::map(executor.handle(), 1, pr1, id);
        let future = Collector::new(pr2);

        let start = std::time::Instant::now();
        let mut result = executor.run_future(future).unwrap().unwrap();
        result.sort();
        assert_eq!(result, ["A", "B", "C"]);
        let end = start.elapsed();
        let exec_time = end.as_millis();
        assert!(12000 <= exec_time && exec_time <= 13000);

        Ok(())
    }

    #[test]
    fn two_level_parallel_executor_works5() -> TestResult {
        /*
        1段目(pr1)で1並列で3オブジェクトを生成し、
        2段目(pr2)で1並列で消費する。

        A) 最初の1秒で1段目の処理が1つ終わり1つ登録される。2つタスクが残っていることに注意。
        B) 次の3秒で2段目が1つ消費する。
        C) 消費し終わったあとに、1秒使って1段目の処理が終わり、次のタスクが追加される。
        .. 直感的には2段目に余裕ができて初めて1段目の計算が行われる要求型の計算である。
        .. こうしなければ、2段目の計算に時間がかかるものの1段目の計算が早急に終り、かつメモリを大量に消費する場合に
        メモリの大量消費が発生してしまう。
        D) 次の3秒で2段目が1つ消費する。
        E) 次の1秒で1段目が1つ追加する。
        F) 次の3秒で2段目が1つ消費する。
        したがって、1+3+1+3+1+3=12秒ほど必要になる。

        もし2段目の処理中に1段目の処理を無制限にやる場合は、
        この場合はBの処理中に、1段目が2つタスクを追加できることになるので、
        1+3+3+3=10秒で済む。
         */

        let mut executor = InPlaceExecutor::new().unwrap();

        let task = convert_to_tasks(vec![
            Generator::new(secs(1), || TestFuture::new("A", secs(3))),
            Generator::new(secs(1), || TestFuture::new("B", secs(3))),
            Generator::new(secs(1), || TestFuture::new("C", secs(3))),
        ]);

        let pr1 = ParallelExecutor::map(executor.handle(), 1, task, id);
        let pr2 = ParallelExecutor::map(executor.handle(), 1, pr1, id);
        let future = Collector::new(pr2);

        let start = std::time::Instant::now();
        let mut result = executor.run_future(future).unwrap().unwrap();
        result.sort();
        assert_eq!(result, ["A", "B", "C"]);
        let end = start.elapsed();
        let exec_time = end.as_millis();
        assert!(12000 <= exec_time && exec_time <= 13000);

        Ok(())
    }

    // Generatorを作る一段目のParallelExecutor pr1と
    // その結果のTestFutureたちを処理する二段目のParallelExecutor pr2について
    // pr2側でエラーが起こる場合の挙動を検証
    #[test]
    fn two_level_parallel_executor_with_errors1() -> TestResult {
        let mut executor = ThreadPoolExecutor::new().unwrap();

        let task = convert_to_tasks(vec![
            Generator::new(millis(100), || TestFuture::new("A", millis(2000))),
            Generator::new(millis(100), || TestFuture::new("B", millis(2000))),
            Generator::new(millis(100), || TestFuture::err("C", millis(30))),
            Generator::new(millis(100), || TestFuture::err("D", millis(30))),
        ]);

        let pr1 = ParallelExecutor::map(executor.handle(), 4, task, id);
        let pr2 = ParallelExecutor::map(executor.handle(), 4, pr1, id);

        let future = Collector::new(pr2);
        let err = executor.run_future(future).unwrap().unwrap_err();
        let mut errors = Vec::new();
        let mut rest_stream = None;
        assert!(match err {
            InnerError(InnerFutureError(error), current_values, rest) => {
                // pr2がInnerFutureErrorを投げるのは、
                // pr1の作ったtaskをpollすることに成功したが、それをspawnした結果でエラーになったことを意味する。
                // 今回は "C" または "D" のどちらかが返っているはずである。
                errors.push(error.clone());
                rest_stream = Some(rest);
                (error == "C" || error == "D") && current_values.is_empty()
            }
            _ => false,
        });

        let future = Collector::new(rest_stream.unwrap());
        let err = executor.run_future(future).unwrap().unwrap_err();
        let mut rest_stream = None;
        assert!(match err {
            InnerError(InnerFutureError(error), current_values, rest) => {
                errors.push(error.clone());
                rest_stream = Some(rest);
                (error == "C" || error == "D") && current_values.is_empty()
            }
            _ => false,
        });

        errors.sort();
        assert_eq!(errors, vec!["C", "D"]);

        let future = Collector::new(rest_stream.unwrap());
        let mut result = executor.run_future(future).unwrap().unwrap();
        result.sort();
        assert_eq!(result, vec!["A", "B"]);

        Ok(())
    }

    // Generatorを作る一段目のParallelExecutor pr1と
    // その結果のTestFutureたちを処理する二段目のParallelExecutor pr2について
    // pr1側でエラーが起こる場合の挙動を検証
    #[test]
    fn two_level_parallel_executor_with_errors2() -> TestResult {
        let mut executor = ThreadPoolExecutor::new().unwrap();

        let task = convert_to_tasks(vec![
            Generator::new(millis(1000), || TestFuture::new("A", millis(1000))),
            Generator::new(millis(1000), || TestFuture::new("B", millis(1000))),
            Generator::err(millis(200), "pr1_error"),
            Generator::new(millis(1000), || TestFuture::new("D", millis(1000))),
        ]);

        let pr1 = ParallelExecutor::map(executor.handle(), 4, task, id);
        let pr2 = ParallelExecutor::map(executor.handle(), 4, pr1, id);

        let future = Collector::new(pr2);

        let err = executor.run_future(future).unwrap().unwrap_err();
        let mut rest_stream = None;
        assert!(match err {
            InnerError(PollError(InnerFutureError(error)), vec, rest) => {
                // この場合は
                // pr2のpoll中に、pr1でpollしたtaskをspawnして実行した際のエラーを受け取った
                // ということを意味している。
                // すなわち `V::err_new(500, "pr1_error")` のエラーが上がってきている。
                rest_stream = Some(rest);
                error == "pr1_error" && vec.is_empty()
            }
            _ => false,
        });

        let rest_stream = rest_stream.unwrap();
        let future = Collector::new(rest_stream);
        let mut result = executor.run_future(future).unwrap().unwrap();
        result.sort();
        assert_eq!(result, ["A", "B", "D"]);

        Ok(())
    }

    // Generatorを作る一段目のParallelExecutor pr1と
    // その結果のTestFutureたちを処理する二段目のParallelExecutor pr2について
    // pr1側とpr2側のどちらでもエラーが起こる場合の挙動を検証
    #[test]
    fn two_level_parallel_executor_with_errors3() -> TestResult {
        let mut executor = ThreadPoolExecutor::new().unwrap();

        let task = convert_to_tasks(vec![
            Generator::new(millis(1000), || TestFuture::new("A", millis(1000))),
            Generator::new(millis(1000), || TestFuture::new("B", millis(1000))),
            Generator::err(millis(200), "pr1_error"),
            Generator::new(millis(1000), || TestFuture::new("D", millis(1000))),
            Generator::new(millis(1000), || TestFuture::err("E", millis(10))),
        ]);

        let pr1 = ParallelExecutor::map(executor.handle(), 4, task, id);
        let pr2 = ParallelExecutor::map(executor.handle(), 4, pr1, id);

        let future = Collector::new(pr2);

        let err = executor.run_future(future).unwrap().unwrap_err();

        let mut rest_stream = None;
        assert!(match err {
            InnerError(PollError(InnerFutureError(error)), current_values, rest) => {
                rest_stream = Some(rest);
                error == "pr1_error" && current_values.is_empty()
            }
            _ => false,
        });

        let future = Collector::new(rest_stream.unwrap());
        let err = executor.run_future(future).unwrap().unwrap_err();
        let mut rest_stream = None;
        assert!(match err {
            InnerError(InnerFutureError(error), current_values, rest) => {
                rest_stream = Some(rest);
                error == "E" && current_values.is_empty()
            }
            _ => false,
        });

        let future = Collector::new(rest_stream.unwrap());
        let mut result = executor.run_future(future).unwrap().unwrap();
        result.sort();

        assert_eq!(result, vec!["A", "B", "D"]);

        Ok(())
    }
}
