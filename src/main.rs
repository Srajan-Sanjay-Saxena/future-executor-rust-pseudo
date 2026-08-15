use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

// ── Core Future types ────────────────────────────────────────────────────────

enum Async<T> {
    Ready(T),
    NotReady,
}

type Poll<T, E> = Result<Async<T>, E>;

trait Future {
    type Item;
    type Error;
    fn poll(&mut self, waker: &Waker) -> Poll<Self::Item, Self::Error>;
}

// ── Waker ────────────────────────────────────────────────────────────────────
// The reactor holds a Waker per task. When I/O is ready it calls wake(),
// which pushes the task id back onto the executor's run queue.

struct Waker {
    task_id: usize,
    queue: Arc<Mutex<VecDeque<usize>>>, // shared with Executor
}

impl Waker {
    fn wake(&self) {
        self.queue.lock().unwrap().push_back(self.task_id);
    }
}

// ── Task ─────────────────────────────────────────────────────────────────────
// A Task owns a Future and the Waker that can reschedule it.

struct Task {
    id: usize,
    future: Box<dyn Future<Item = i32, Error = String>>,
    waker: Waker,
}

// ── Reactor ──────────────────────────────────────────────────────────────────
// Simulates an event source (e.g. epoll). External I/O calls register() to
// say "wake task X after N ms". The reactor runs on its own thread.

struct Reactor {
    // (wake_after_ms, waker)
    pending: Arc<Mutex<Vec<(u64, Waker)>>>,
}

impl Reactor {
    fn new() -> Self {
        Reactor {
            pending: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Called by a Future inside poll() to register interest in an event.
    fn register(&self, after_ms: u64, waker: Waker) {
        self.pending.lock().unwrap().push((after_ms, waker));
    }

    /// Spawns the reactor thread. In a real runtime this would be epoll/kqueue.
    fn run(pending: Arc<Mutex<Vec<(u64, Waker)>>>) {
        thread::spawn(move || loop {
            thread::sleep(Duration::from_millis(10));
            let mut p = pending.lock().unwrap();
            p.retain(|(after_ms, waker)| {
                // Pseudo-check: just wake immediately after the delay expires.
                // A real reactor would check fd readiness here.
                thread::sleep(Duration::from_millis(*after_ms));
                waker.wake();
                false // remove after waking
            });
        });
    }
}

// ── Executor ─────────────────────────────────────────────────────────────────
// Maintains a run-queue of task ids. Polls each ready task; if NotReady the
// task is parked until the reactor wakes it via Waker::wake().

struct Executor {
    tasks: Vec<Option<Task>>,
    run_queue: Arc<Mutex<VecDeque<usize>>>,
}

impl Executor {
    fn new() -> Self {
        Executor {
            tasks: Vec::new(),
            run_queue: Arc::new(Mutex::new(VecDeque::new())),
        }
    }

    fn spawn(&mut self, future: Box<dyn Future<Item = i32, Error = String>>) {
        let id = self.tasks.len();
        let waker = Waker {
            task_id: id,
            queue: Arc::clone(&self.run_queue),
        };
        self.tasks.push(Some(Task { id, future, waker }));
        self.run_queue.lock().unwrap().push_back(id);
    }

    fn run(&mut self) {
        let mut completed = 0;
        let total = self.tasks.len();

        while completed < total {
            // Drain the run-queue; park if nothing is ready.
            let ready: Vec<usize> = {
                let mut q = self.run_queue.lock().unwrap();
                q.drain(..).collect()
            };

            if ready.is_empty() {
                // Nothing to do — yield the thread until the reactor wakes us.
                thread::sleep(Duration::from_millis(1));
                continue;
            }

            for id in ready {
                let task = match self.tasks[id].as_mut() {
                    Some(t) => t,
                    None => continue, // already finished
                };

                match task.future.poll(&task.waker) {
                    Ok(Async::Ready(val)) => {
                        println!("Task {id} finished with: {val}");
                        self.tasks[id] = None;
                        completed += 1;
                    }
                    Ok(Async::NotReady) => {
                        // The future must have called reactor.register() inside poll()
                        // so the reactor will call waker.wake() when ready.
                        println!("Task {id} not ready — parked");
                    }
                    Err(e) => {
                        println!("Task {id} errored: {e}");
                        self.tasks[id] = None;
                        completed += 1;
                    }
                }
            }
        }
    }
}

// ── Demo Future ───────────────────────────────────────────────────────────────
// Simulates an async I/O op: first poll registers with the reactor and returns
// NotReady; second poll (after reactor wakes it) returns Ready.

struct DelayFuture {
    reactor: Arc<Mutex<Reactor>>,
    delay_ms: u64,
    registered: bool,
}

impl Future for DelayFuture {
    type Item = i32;
    type Error = String;

    fn poll(&mut self, waker: &Waker) -> Poll<Self::Item, Self::Error> {
        if !self.registered {
            // Register interest: "wake me after delay_ms"
            let waker = Waker {
                task_id: waker.task_id,
                queue: Arc::clone(&waker.queue),
            };
            self.reactor
                .lock()
                .unwrap()
                .register(self.delay_ms, waker);
            self.registered = true;
            return Ok(Async::NotReady);
        }
        // Second poll — reactor already fired, we're done.
        Ok(Async::Ready(42))
    }
}

// ── main ──────────────────────────────────────────────────────────────────────

fn main() {
    let reactor = Arc::new(Mutex::new(Reactor::new()));

    // Start the reactor thread.
    let pending = Arc::clone(&reactor.lock().unwrap().pending);
    Reactor::run(pending);

    let mut executor = Executor::new();

    executor.spawn(Box::new(DelayFuture {
        reactor: Arc::clone(&reactor),
        delay_ms: 50,
        registered: false,
    }));
    executor.spawn(Box::new(DelayFuture {
        reactor: Arc::clone(&reactor),
        delay_ms: 20,
        registered: false,
    }));

    executor.run();
}
