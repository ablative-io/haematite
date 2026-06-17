// WASM-001: web-worker runtime adapter.
//
// In the browser, shard actors must run on web workers so that database work
// never blocks the UI thread (R3, C5). beamr's native scheduler does not compile
// to wasm32 (it depends on crossbeam and num_cpus), so the browser runtime is
// built directly on the platform primitives: a pool of `web_sys::Worker`s for
// parallelism across threads, and `wasm_bindgen_futures::spawn_local` to drive
// each actor's async IndexedDB work cooperatively on its worker's event loop.
//
// This adapter owns the worker pool, the dispatch policy, and the result/error
// return path (`spawn_actor` posts to a worker; `set_message_handler` receives
// what the worker posts back). The shard-actor *message protocol* — the request
// envelope, response correlation, and serialisation of get/put/range/fork/merge
// — is deliberately out of scope here and is layered on top by the shard cluster
// (CORE actor briefs). Constructing the runtime is the seam that guarantees
// actors are placed off the main thread (R3, C5).

use std::cell::Cell;
use std::future::Future;

use js_sys::Function;
use wasm_bindgen::{JsCast, JsValue};
use web_sys::{Worker, WorkerOptions, WorkerType};

/// A pool of web workers that shard actors are dispatched onto.
#[derive(Debug)]
pub struct WorkerRuntime {
    workers: Vec<Worker>,
    next: Cell<usize>,
}

impl WorkerRuntime {
    /// Spawn `count` workers from `script_url` (clamped to at least one).
    ///
    /// Each worker runs the haematite worker entry module; shard actors are then
    /// distributed across them so multiple actors execute concurrently on
    /// separate threads (R3).
    ///
    /// # Errors
    /// Returns an error if the browser refuses to construct a worker.
    pub fn with_workers(script_url: &str, count: usize) -> Result<Self, RuntimeError> {
        let options = WorkerOptions::new();
        options.set_type(WorkerType::Module);

        let target = count.max(1);
        let mut workers = Vec::with_capacity(target);
        for _ in 0..target {
            let worker = Worker::new_with_options(script_url, &options)
                .map_err(|error| RuntimeError::from_js("spawn worker", &error))?;
            workers.push(worker);
        }

        Ok(Self {
            workers,
            next: Cell::new(0),
        })
    }

    /// Number of workers in the pool.
    pub const fn worker_count(&self) -> usize {
        self.workers.len()
    }

    /// Dispatch a shard actor onto the next worker in round-robin order by
    /// posting its init message. The actor then runs entirely on that worker,
    /// keeping its operations off the main thread (R3, C5).
    ///
    /// # Errors
    /// Returns an error if the pool is empty or the message cannot be posted.
    pub fn spawn_actor(&self, message: &JsValue) -> Result<(), RuntimeError> {
        let worker = self
            .next_worker()
            .ok_or_else(|| RuntimeError::new("worker pool is empty"))?;
        worker
            .post_message(message)
            .map_err(|error| RuntimeError::from_js("post message", &error))
    }

    /// Register a handler for messages workers post back to the main thread,
    /// closing the return path for shard-actor results and errors. The handler
    /// is a JS function so the shard cluster can decode its own response envelope
    /// (C5 result/error propagation across the worker boundary).
    pub fn set_message_handler(&self, handler: &Function) {
        for worker in &self.workers {
            worker.set_onmessage(Some(handler));
        }
    }

    fn next_worker(&self) -> Option<&Worker> {
        if self.workers.is_empty() {
            return None;
        }
        let index = self.next.get() % self.workers.len();
        self.next.set(index.wrapping_add(1));
        self.workers.get(index)
    }
}

/// Drive an actor's async task on the current worker's event loop.
///
/// Used inside a worker to run an actor's IndexedDB-backed work cooperatively:
/// the task makes progress on the microtask queue and yields at every `await`,
/// so the thread is never blocked waiting on a transaction (CN4).
pub fn drive<F>(task: F)
where
    F: Future<Output = ()> + 'static,
{
    wasm_bindgen_futures::spawn_local(task);
}

/// Cast the current global scope to a worker scope, confirming this code is
/// running on a web worker rather than the main thread.
///
/// # Errors
/// Returns an error if called on the main thread.
pub fn require_worker_scope() -> Result<web_sys::WorkerGlobalScope, RuntimeError> {
    js_sys::global()
        .dyn_into::<web_sys::WorkerGlobalScope>()
        .map_err(|_| RuntimeError::new("expected to run on a web worker, not the main thread"))
}

/// Error raised by the web-worker runtime adapter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeError(String);

impl RuntimeError {
    fn new(message: &str) -> Self {
        Self(message.to_owned())
    }

    fn from_js(context: &str, value: &JsValue) -> Self {
        Self(format!("{context}: {value:?}"))
    }
}

impl std::fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "WASM runtime error: {}", self.0)
    }
}

impl std::error::Error for RuntimeError {}
