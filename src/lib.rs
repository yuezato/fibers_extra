#![allow(unused_imports)]
#![allow(dead_code)]

extern crate chrono;
extern crate fibers;
extern crate futures;
extern crate trackable;

use fibers::executor::ThreadPoolExecutorHandle;
use fibers::sync::mpsc;
use fibers::Spawn;
use futures::future::Either;
use futures::{Async, Future, Poll, Stream};
use std::fmt::{self, Debug};
use std::marker::PhantomData;
use std::sync::Arc;

/// Stream futureを受け取り、全ての値を回収しようとする。
/// 全ての値を回収できた場合にはAsync::Ready(vec)でVecとして値を返す。
/// 値を回収している途中はAsync::NotReadyを返す。
pub struct Collector<T, S> {
    inner: Vec<T>,
    stream: Option<S>,
}

/*
impl<T, S> Drop for Collector<T, S>
    where T: std::fmt::Debug
{
    fn drop(&mut self) {
        println!("[Drop] Collector inner = {:?}", self.inner);
    }
}
*/

impl<T, E, S> Collector<T, S>
where
    T: Clone,
    S: Stream<Item = T, Error = E>,
{
    pub fn new(stream: S) -> Self {
        Collector {
            inner: Vec::new(),
            stream: Some(stream),
        }
    }

    pub fn current_values(&self) -> Vec<T> {
        self.inner.clone()
    }
}

pub enum CollectorError<E, T: Clone, S> {
    InnerError(E, Vec<T>, S),
    AlreadyFinished,
}
impl<E, T: Clone, S> Debug for CollectorError<E, T, S> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            CollectorError::InnerError(_, _, _) => write!(f, "InnerError"),
            CollectorError::AlreadyFinished => write!(f, "AlreadyFinished"),
        }
    }
}

impl<T, E, S> Future for Collector<T, S>
where
    T: Clone,
    S: Stream<Item = T, Error = E>,
{
    type Item = Vec<T>;
    type Error = CollectorError<E, T, S>;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        if self.stream.is_none() {
            return Err(CollectorError::AlreadyFinished);
        }
        let mut stream = self.stream.take().unwrap();

        match stream.poll() {
            Ok(Async::Ready(Some(e))) => {
                self.stream = Some(stream);
                self.inner.push(e);
                Ok(Async::NotReady)
            }
            Ok(Async::Ready(None)) => {
                self.stream = Some(stream);
                Ok(Async::Ready(self.inner.clone()))
            }
            Ok(Async::NotReady) => {
                self.stream = Some(stream);
                Ok(Async::NotReady)
            }
            Err(e) => Err(CollectorError::InnerError(e, self.inner.clone(), stream)),
        }
    }
}

#[derive(PartialEq, Debug)]
enum InnerState {
    Processing,
    NoTasks,
    Finished,
    ErrorOccurred,
}

pub struct ParallelExecutor<Handler, T, E, Tasks> {
    handle: Handler,
    concurrency: usize,
    current_spawned: usize,
    sender: mpsc::Sender<Either<T, E>>,
    receiver: mpsc::Receiver<Either<T, E>>,
    tasks: Tasks,
    buffer: Vec<T>,
    error_buffer: Vec<E>,
    state: InnerState,
}

impl<Handler, T, E1, E2, F, Tasks> ParallelExecutor<Handler, T, E1, Tasks>
where
    Handler: fibers::Spawn,
    T: Send + 'static,
    E1: Send + 'static,
    E2: Send + 'static,
    F: Future<Item = T, Error = E1> + Send + 'static,
    Tasks: Stream<Item = F, Error = E2>,
{
    pub fn new(
        handle: Handler,
        concurrency: usize,
        tasks: Tasks,
    ) -> ParallelExecutor<Handler, T, E1, Tasks> {
        let (sender, receiver) = mpsc::channel();
        ParallelExecutor {
            current_spawned: 0,
            handle,
            concurrency,
            sender,
            receiver,
            tasks,
            buffer: Vec::new(),
            error_buffer: Vec::new(),
            state: InnerState::Processing,
        }
    }
}

impl<H, T, E1, E2, F, Tasks> Stream for ParallelExecutor<H, T, E1, Tasks>
where
    H: fibers::Spawn,
    T: Send + 'static,
    E1: Send + 'static,
    E2: Send + 'static,
    F: Future<Item = T, Error = E1> + Send + 'static,
    Tasks: Stream<Item = F, Error = E2>,
{
    type Item = T;
    type Error = Either<Vec<E1>, E2>;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        if self.state == InnerState::Finished {
            return Ok(Async::Ready(None));
        }

        loop {
            match self.receiver.poll() {
                Ok(Async::Ready(Some(v_or_e))) => {
                    match v_or_e {
                        Either::A(v) => {
                            // 値が返ってきた場合
                            self.current_spawned -= 1;
                            self.buffer.push(v);
                        }
                        Either::B(e) => {
                            self.current_spawned -= 1;
                            self.error_buffer.push(e);
                            self.state = InnerState::ErrorOccurred;
                        }
                    }
                }
                Ok(Async::Ready(None)) => {
                    // このreceiverがこの先に値を受け取ることができない場合。
                    // このreceiverを以後使えない場合。
                    // この構造体ではreceiverの対になるsenderをholdしているので
                    // ここに来ることはないはずである
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

        while self.state != InnerState::ErrorOccurred
            && (self.buffer.len() + self.current_spawned) < self.concurrency
        {
            match self.tasks.poll() {
                Ok(Async::Ready(Some(future))) => {
                    let tx = self.sender.clone();
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
                    self.current_spawned += 1;
                }
                Ok(Async::Ready(None)) => {
                    self.state = InnerState::NoTasks;
                    break;
                }
                Ok(Async::NotReady) => {
                    break;
                }
                Err(e2) => {
                    // このエラーが上がってしまった場合は
                    // futureの取り出しそのものに失敗しているため。
                    // retryで回復する余地は多くの場合に存在しないと思われる。
                    return Err(Either::B(e2));
                }
            }
        }

        if self.current_spawned == 0 && self.state == InnerState::ErrorOccurred {
            let errors = std::mem::replace(&mut self.error_buffer, Vec::new());
            self.state = InnerState::Processing;
            return Err(Either::A(errors));
        }

        if let Some(v) = self.buffer.pop() {
            Ok(Async::Ready(Some(v)))
        } else if self.current_spawned == 0 && self.state == InnerState::NoTasks {
            self.state = InnerState::Finished;
            Ok(Async::Ready(None))
        } else {
            Ok(Async::NotReady)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fibers::time::timer;
    use fibers::{Executor, InPlaceExecutor, Spawn, ThreadPoolExecutor};
    use std::cmp::PartialEq;
    use std::thread;
    use std::time::{self, Duration};
    use trackable::error::TestError;
    use trackable::result::TestResult;

    struct VecWrap<T> {
        inner: Vec<T>,
    }

    impl<T, E, F> Stream for VecWrap<F>
    where
        F: Future<Item = T, Error = E>,
    {
        type Item = F;
        type Error = ();

        fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
            if let Some(v) = self.inner.pop() {
                return Ok(Async::Ready(Some(v)));
            } else {
                return Ok(Async::Ready(None));
            }
        }
    }

    fn convert_to_tasks<T, E, F: Future<Item = T, Error = E>>(
        v: Vec<F>,
    ) -> impl Stream<Item = Box<F>, Error = ()> {
        let v = v.into_iter().map(Box::new).collect();
        VecWrap { inner: v }
    }

    struct U {
        inner: String,
        error: bool,
        howlong: u64,
        timer: Option<timer::Timeout>, // timerは、このFuture Uが初めてpollされた段階で、`howlong`-millisで動き始める
    }
    impl U {
        pub fn new(inner: &str, howlong: u64) -> Self {
            U {
                inner: inner.to_owned(),
                error: false,
                howlong,
                timer: None,
            }
        }

        pub fn err_new(inner: &str, howlong: u64) -> Self {
            U {
                inner: inner.to_owned(),
                error: true,
                howlong,
                timer: None,
            }
        }
    }
    impl Future for U {
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
                self.timer = Some(timer::timeout(Duration::from_millis(self.howlong)));
                Ok(Async::NotReady)
            }
        }
    }

    struct V {
        howlong: u64,
        generator: Box<Fn() -> U + Send + 'static>,
        timer: Option<timer::Timeout>, // timerは、このFuture Vが初めてpollされた段階で、`howlong`-millisで動き始める
    }
    impl V {
        pub fn new<T: Fn() -> U + Send + 'static>(howlong: u64, generator: T) -> Self {
            V {
                howlong,
                generator: Box::new(generator),
                timer: None,
            }
        }
    }
    impl Future for V {
        type Item = U;
        type Error = ();

        fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
            if let Some(ref mut timer) = self.timer {
                match timer.poll() {
                    Ok(Async::Ready(_)) => Ok(Async::Ready((self.generator)())),
                    Ok(Async::NotReady) => Ok(Async::NotReady),
                    Err(_) => Err(()),
                }
            } else {
                self.timer = Some(timer::timeout(Duration::from_millis(self.howlong)));
                Ok(Async::NotReady)
            }
        }
    }

    #[test]
    fn handle_errors() -> TestResult {
        use self::CollectorError::*;

        let mut executor = ThreadPoolExecutor::new().unwrap();

        let task = convert_to_tasks(vec![
            U::new("A", 1000),
            U::new("B", 1000),
            U::err_new("C", 2000),
            U::new("D", 3000),
        ]);

        let pr = ParallelExecutor::new(executor.handle(), 4, task);

        let future = Collector::new(pr);

        let result;
        {
            result = executor.run_future(future);
            std::thread::sleep(std::time::Duration::from_millis(3000));
        }

        // run_future自体は成功する。
        // run_futureがエラーを返すのは、executorのレベルで何かしら問題があった時である。
        assert!(result.is_ok());

        // prが抱えている内部のfutureのエラーが無事に取れている。
        // （内部のfutureではなくpr固有のエラーであれば、Either::Bでアクセスできる）
        let result = result.unwrap();
        let mut rest_stream = None;
        assert!(match result {
            Err(InnerError(Either::A(errors), mut values, rest)) => {
                rest_stream = Some(rest);
                values.sort();
                let e = &errors[0];

                values == ["A", "B"] && e == "C"
            }
            _ => false,
        });

        let rest_stream = rest_stream.unwrap();
        let future = Collector::new(rest_stream);
        let result;
        {
            result = executor.run_future(future);
            std::thread::sleep(std::time::Duration::from_millis(3000));
        }
        let result = result.unwrap().unwrap();

        assert_eq!(result, ["D"]);

        Ok(())
    }

    #[test]
    fn collector_works() {
        let mut executor = ThreadPoolExecutor::new().unwrap();
        let task = convert_to_tasks(vec![
            U::new("A", 1000),
            U::new("B", 1000),
            U::new("C", 1000),
            U::new("D", 100),
        ]);

        let pr = ParallelExecutor::new(executor.handle(), 4, task);
        let future = Collector::new(pr);

        let result = executor.run_future(future);

        let mut vec = result.unwrap().unwrap();
        vec.sort();
        assert_eq!(vec, ["A", "B", "C", "D"]);
    }

    #[test]
    fn check_concurrency_test1_works() {
        let mut executor = ThreadPoolExecutor::new().unwrap();
        let task = convert_to_tasks(vec![
            U::new("A", 1000),
            U::new("B", 1000),
            U::new("C", 1000),
            U::new("D", 100),
        ]);

        let pr = ParallelExecutor::new(executor.handle(), 4, task);

        let start = std::time::Instant::now();
        let result = executor.run_future(Box::new(Collector::new(pr)));
        assert!(result.is_ok());
        let mut result = result.unwrap().unwrap();
        result.sort();
        assert_eq!(result, ["A", "B", "C", "D"]);
        let end = start.elapsed();
        let exec_time = end.as_millis();
        // 2秒以内には終わるだろうという雑な値
        assert!(1000 <= exec_time && exec_time <= 2000);
    }

    /*
    ParallelExecutorを二段建てた上で、
    concurrencyパラメータを変更して実行速度を計測することで、
    パラメータが有効であることを調べる。
     */
    #[test]
    fn two_level_parallel_executor_works1() -> TestResult {
        let mut executor = InPlaceExecutor::new().unwrap();

        {
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
                V::new(1000, || U::new("A", 1000)),
                V::new(1000, || U::new("B", 1000)),
                V::new(1000, || U::new("C", 1000)),
                V::new(1000, || U::new("D", 1000)),
            ]);

            let pr1 = ParallelExecutor::new(executor.handle(), 4, task);
            let pr2 = ParallelExecutor::new(executor.handle(), 1, pr1);

            let start = std::time::Instant::now();
            let result = executor.run_future(Box::new(Collector::new(pr2)));
            assert!(result.is_ok());
            let mut result = result.unwrap().unwrap();
            result.sort();
            assert_eq!(result, ["A", "B", "C", "D"]);
            let end = start.elapsed();
            let exec_time = end.as_millis();
            // 6秒以内には終わるだろうという雑な値
            assert!(5000 <= exec_time && exec_time <= 6000);
        }

        Ok(())
    }

    #[test]
    fn two_level_parallel_executor_works2() -> TestResult {
        let mut executor = InPlaceExecutor::new().unwrap();

        {
            /*
            1段目(pr1)で4並列4オブジェクトを生成し、
            2段目(pr2)で2並列で消費する。

            . 最初の1秒で1段目の処理が全て終わり4つ登録され、
            . 次の最初の1秒で2段目が2つ消費する。
            . 次の1秒で2段目が2つ消費する。
            したがって、合計で3秒程度必要となる。
             */

            let task = convert_to_tasks(vec![
                V::new(1000, || U::new("A", 1000)),
                V::new(1000, || U::new("B", 1000)),
                V::new(1000, || U::new("C", 1000)),
                V::new(1000, || U::new("D", 1000)),
            ]);

            let pr1 = ParallelExecutor::new(executor.handle(), 4, task);
            let pr2 = ParallelExecutor::new(executor.handle(), 2, pr1);

            let start = std::time::Instant::now();
            let result = executor.run_future(Box::new(Collector::new(pr2)));
            assert!(result.is_ok());
            let mut result = result.unwrap().unwrap();
            result.sort();
            assert_eq!(result, ["A", "B", "C", "D"]);
            let end = start.elapsed();
            let exec_time = end.as_millis();
            // 4秒以内には終わるだろうという雑な値
            assert!(3000 <= exec_time && exec_time <= 4000);
        }

        Ok(())
    }

    #[test]
    fn two_level_parallel_executor_works3() -> TestResult {
        let mut executor = InPlaceExecutor::new().unwrap();

        {
            /*
            1段目(pr1)で4並列6オブジェクトを生成し、
            2段目(pr2)で2並列で消費する。

            . 最初の1秒で1段目の処理が4つ終わり4つ登録される。2つ残っていることに注意。
            . 次の最初の1秒で2段目が2つ消費する。同時に1段目の処理が走り、残った2つが登録される。
            . 次の1秒で2段目が2つ消費する。
            . 次の1秒で2段目が2つ消費する。
            したがって、合計で4秒程度必要となる。
             */

            let task = convert_to_tasks(vec![
                V::new(1000, || U::new("A", 1000)),
                V::new(1000, || U::new("B", 1000)),
                V::new(1000, || U::new("C", 1000)),
                V::new(1000, || U::new("D", 1000)),
                V::new(1000, || U::new("E", 1000)),
                V::new(1000, || U::new("F", 1000)),
            ]);

            let pr1 = ParallelExecutor::new(executor.handle(), 4, task);
            let pr2 = ParallelExecutor::new(executor.handle(), 2, pr1);

            let start = std::time::Instant::now();
            let result = executor.run_future(Box::new(Collector::new(pr2)));
            assert!(result.is_ok());
            let mut result = result.unwrap().unwrap();
            result.sort();
            assert_eq!(result, ["A", "B", "C", "D", "E", "F"]);
            let end = start.elapsed();
            let exec_time = end.as_millis();
            assert!(4000 <= exec_time && exec_time <= 5000);
        }

        Ok(())
    }

    #[test]
    fn two_level_parallel_executor_works4() -> TestResult {
        let mut executor = InPlaceExecutor::new().unwrap();
        {
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

            let task = convert_to_tasks(vec![
                V::new(1000, || U::new("A", 3000)),
                V::new(1000, || U::new("B", 3000)),
                V::new(1000, || U::new("C", 3000)),
            ]);

            let pr1 = ParallelExecutor::new(executor.handle(), 1, task);
            let pr2 = ParallelExecutor::new(executor.handle(), 1, pr1);

            let start = std::time::Instant::now();
            let result = executor.run_future(Box::new(Collector::new(pr2)));
            assert!(result.is_ok());
            let mut result = result.unwrap().unwrap();
            result.sort();
            assert_eq!(result, ["A", "B", "C"]);
            let end = start.elapsed();
            let exec_time = end.as_millis();
            assert!(12000 <= exec_time && exec_time <= 13000);
        }

        Ok(())
    }

    #[test]
    fn two_level_parallel_executor_works5() -> TestResult {
        let mut executor = InPlaceExecutor::new().unwrap();
        {
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

            let task = convert_to_tasks(vec![
                V::new(1000, || U::new("A", 3000)),
                V::new(1000, || U::new("B", 3000)),
                V::new(1000, || U::new("C", 3000)),
            ]);

            let pr1 = ParallelExecutor::new(executor.handle(), 1, task);
            let pr2 = ParallelExecutor::new(executor.handle(), 1, pr1);

            let start = std::time::Instant::now();
            let result = executor.run_future(Box::new(Collector::new(pr2)));
            assert!(result.is_ok());
            let mut result = result.unwrap().unwrap();
            result.sort();
            assert_eq!(result, ["A", "B", "C"]);
            let end = start.elapsed();
            let exec_time = end.as_millis();
            assert!(12000 <= exec_time && exec_time <= 13000);
        }

        Ok(())
    }
}
