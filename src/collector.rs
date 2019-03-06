extern crate fibers;
extern crate futures;
extern crate trackable;

use futures::{Async, Future, Poll, Stream};
use std::fmt::{self, Debug};

/// Collector futureに対するpollで返りうるエラーの列挙体。
///
/// 以下二種類のエラーがある:
/// * `InnerError(e, vec, stream)`
///     * `e` = エラー内容
///     * `vec` = エラーが発生するまでに獲得した内容
///     * `stream` = エラーが発生した段階での残りのストリーム（継続）
/// * `AlreadyFinished`
///     * このCollectorは役目を終えており、pollに何ら意味がないことを表す。
pub enum CollectorError<E, T, S> {
    InnerError(E, Vec<T>, S),
    AlreadyFinished,
}
impl<E, T, S> Debug for CollectorError<E, T, S> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            CollectorError::InnerError(_, _, _) => write!(f, "InnerError"),
            CollectorError::AlreadyFinished => write!(f, "AlreadyFinished"),
        }
    }
}

/// Streamを受け取り、その全ての値を回収するためのFuture。
///
/// 正確には以下のように振る舞う:
/// * 全ての値を回収できた場合: `Async::Ready(vec)`でVecとして値を返す。
/// * 値を回収している途中: `Async::NotReady`を返す。
/// * 値の回収中にエラーが起きた場合: `CollectorError::InnerError(エラー内容, 現在までに計算済みの値, 残りのstream)`を返す。
///
/// また、Async::Ready(vec) ないし InnerError を返した後はこのCollectorは使用不能となり、
/// `poll()`された際に`CollectorError::AlreadyFinished`を返す。
pub struct Collector<T, S> {
    inner: Vec<T>,     // 現在回収済みの値を蓄える。
    stream: Option<S>, // 処理対象のstream; エラー時にOption::takeを使い実態を返すためにOptionで包んでいる。
}

impl<T, E, S> Collector<T, S>
where
    S: Stream<Item = T, Error = E>,
{
    pub fn new(stream: S) -> Self {
        Collector {
            inner: Vec::new(),
            stream: Some(stream),
        }
    }
}

impl<T, E, S> Future for Collector<T, S>
where
    S: Stream<Item = T, Error = E>,
{
    type Item = Vec<T>;
    type Error = CollectorError<E, T, S>;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        if self.stream.is_none() {
            // stream == None となるのは値返却済みもしくはエラー発生時
            // この場合にはこのCollectorを使うことはできない。
            return Err(CollectorError::AlreadyFinished);
        }
        let mut stream = self
            .stream
            .take()
            .expect("this never fails since stream.is_ok() == true");

        match stream.poll() {
            Ok(Async::Ready(Some(e))) => {
                // 値が無事に取り出せた場合はstreamを復旧し、回収した値を貯め込む。
                self.stream = Some(stream);
                self.inner.push(e);
                Ok(Async::NotReady)
            }
            Ok(Async::Ready(None)) => {
                // streamがもう値を返すことがないため、このCollectorもReadyによって役目を終える。
                let contents = std::mem::replace(&mut self.inner, Vec::new());
                Ok(Async::Ready(contents))
            }
            Ok(Async::NotReady) => {
                // streamが準備できていないため、このCollectorもNotReadyを返す。
                self.stream = Some(stream);
                Ok(Async::NotReady)
            }
            Err(e) => {
                // 値の取り出しで何かしらエラーが生じたので
                // エラーと現在までに得ている内容及び継続を返却する。
                let contents = std::mem::replace(&mut self.inner, Vec::new());
                Err(CollectorError::InnerError(e, contents, stream))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Collector, CollectorError};
    use fibers::{Executor, ThreadPoolExecutor};
    use futures::stream;
    use trackable::result::TestResult;

    #[test]
    fn collect_test1() -> TestResult {
        let stream = stream::iter_result(vec![Ok(1), Err(false), Ok(2), Ok(3), Err(false), Ok(4)]);

        let future = Collector::new(stream);
        let mut executor = ThreadPoolExecutor::new().unwrap();
        let result = executor.run_future(future).unwrap();

        let mut continuation = None;
        assert!(
            if let Err(CollectorError::InnerError(e, values, rest)) = result {
                // 1つ目のfalseでInnerErrorが発生した
                continuation = Some(rest);
                !e && values == vec![1]
            } else {
                false
            }
        );

        let future = Collector::new(continuation.unwrap());
        let result = executor.run_future(future).unwrap();

        let mut continuation = None;
        assert!(
            if let Err(CollectorError::InnerError(e, values, rest)) = result {
                // ２つ目のfalseでErrorが発生した
                continuation = Some(rest);
                !e && values == vec![2, 3]
            } else {
                false
            }
        );

        let future = Collector::new(continuation.unwrap());
        let result = executor.run_future(future).unwrap().unwrap();
        assert_eq!(result, vec![4]);

        Ok(())
    }
}
