use std::thread;

fn main() {
    println!("Hello, world!");
}

enum Async<T> {
    Ready(T),
    NotReady,
}

type Poll<T, E> = Result<Async<T>, E>;

pub trait Future {
    type Item;
    type Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error>;
}

type FutureType = Box<dyn Future<Item = i32, Error = String>>;

struct Executor;

impl Executor {
    fn run_all(&self, f: Vec<FutureType>) -> Vec<Result<FutureType::Item, FutureType::Error>> {
        let f_len = f.len();
        let mut results = Vec::with_capacity(f_len);

        let mut done = 0;
        while done != f_len {
            for (idx, fut) in f.iter_mut().enumerate() {
                match fut.poll() {
                    Ok(Async::Ready(t)) => {
                        results[idx] = Ok(t);
                        done += 1;
                    }
                    Ok(Async::NotReady) => {
                        // Find some mechanism in which we can again tell the executor to run this
                        // task
                    }
                    Err(e) => {
                        results[idx] = Err(e);
                        done += 1;
                    }
                }
            }

            /** considering we are making a blocking executor. So somehow the future has to
             * remove this sleep and wake the executor to again poll .
             * */
            thread::sleep(5);
        }
        results
    }
}
