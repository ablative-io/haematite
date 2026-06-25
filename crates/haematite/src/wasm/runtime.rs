//! WASM runtime adapter (R3).
//!
//! [`WasmRuntime`] runs on the main thread and owns one [`WasmShardHandle`] per
//! shard. Each handle owns a [`web_sys::Worker`] running a dedicated web worker;
//! shard actors execute *inside* those workers via [`WasmShardRuntime`], which
//! wraps the `beamr-wasm` scheduler. Shard actor work therefore never runs on
//! the main thread — booting a [`WasmShardRuntime`] outside a worker scope is a
//! hard error ([`WasmRuntimeError::ShardRuntimeOnMainThread`]).

use std::fmt;

use wasm_bindgen::JsValue;

const DEFAULT_WORKER_NAME: &str = "haematite-shard";

/// Main-thread handle to the pool of shard workers.
#[derive(Debug)]
pub struct WasmRuntime {
    shards: Vec<WasmShardHandle>,
}

impl WasmRuntime {
    /// Spawn one web worker per shard, each loading `worker_script_url`.
    pub fn spawn_workers(
        worker_script_url: &str,
        shard_count: usize,
    ) -> Result<Self, WasmRuntimeError> {
        if shard_count == 0 {
            return Err(WasmRuntimeError::InvalidShardCount);
        }
        let mut shards = Vec::with_capacity(shard_count);
        for shard_id in 0..shard_count {
            shards.push(WasmShardHandle::spawn(worker_script_url, shard_id)?);
        }
        Ok(Self { shards })
    }

    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    pub fn shard(&self, shard_id: usize) -> Option<&WasmShardHandle> {
        self.shards.get(shard_id)
    }

    pub fn shards(&self) -> &[WasmShardHandle] {
        &self.shards
    }

    /// Ask every shard worker to drive its scheduler one cooperative step.
    pub fn broadcast_run_step(&self) -> Result<(), WasmRuntimeError> {
        for shard in &self.shards {
            shard.post_command(&WorkerCommand::RunStep.to_js())?;
        }
        Ok(())
    }
}

/// A single shard's web worker, owned by the main thread.
#[derive(Debug)]
pub struct WasmShardHandle {
    shard_id: usize,
    worker: web_sys::Worker,
}

impl WasmShardHandle {
    pub fn spawn(worker_script_url: &str, shard_id: usize) -> Result<Self, WasmRuntimeError> {
        let options = web_sys::WorkerOptions::new();
        options.set_type(web_sys::WorkerType::Module);
        options.set_name(&format!("{DEFAULT_WORKER_NAME}-{shard_id}"));
        let worker = web_sys::Worker::new_with_options(worker_script_url, &options)
            .map_err(WasmRuntimeError::from_js)?;
        let handle = Self { shard_id, worker };
        handle.post_command(&WorkerCommand::Boot { shard_id }.to_js())?;
        Ok(handle)
    }

    pub const fn shard_id(&self) -> usize {
        self.shard_id
    }

    pub fn post_command(&self, command: &JsValue) -> Result<(), WasmRuntimeError> {
        self.worker
            .post_message(command)
            .map_err(WasmRuntimeError::from_js)
    }

    pub fn worker(&self) -> &web_sys::Worker {
        &self.worker
    }
}

/// The shard runtime that runs *inside* a web worker, wrapping the `beamr-wasm`
/// scheduler. Construction fails on the main thread (R3 boundary).
pub struct WasmShardRuntime {
    vm: beamr_wasm::WasmVm,
    shard_id: usize,
}

impl WasmShardRuntime {
    pub fn boot(shard_id: usize) -> Result<Self, WasmRuntimeError> {
        if !is_worker_scope() {
            return Err(WasmRuntimeError::ShardRuntimeOnMainThread);
        }
        let vm = beamr_wasm::create_vm().map_err(WasmRuntimeError::from_js)?;
        Ok(Self { vm, shard_id })
    }

    pub const fn shard_id(&self) -> usize {
        self.shard_id
    }

    pub fn load_module(&mut self, bytes: &[u8]) -> Result<JsValue, WasmRuntimeError> {
        self.vm.load_module(bytes).map_err(WasmRuntimeError::from_js)
    }

    pub fn register_async_nif(
        &mut self,
        module: &str,
        function: &str,
        arity: u8,
        callback: js_sys::Function,
    ) -> Result<(), WasmRuntimeError> {
        self.vm
            .register_async_nif(module, function, arity, callback)
            .map_err(WasmRuntimeError::from_js)
    }

    pub fn spawn_actor(
        &mut self,
        module: &str,
        function: &str,
        args_json: &str,
    ) -> Result<u64, WasmRuntimeError> {
        self.vm
            .spawn(module, function, args_json)
            .map_err(WasmRuntimeError::from_js)
    }

    pub fn send_message(&mut self, pid: u64, value: JsValue) -> Result<(), WasmRuntimeError> {
        self.vm
            .send_message(pid, value)
            .map_err(WasmRuntimeError::from_js)
    }

    pub fn run_step(&mut self) -> Result<JsValue, WasmRuntimeError> {
        self.vm.run_step().map_err(WasmRuntimeError::from_js)
    }

    pub fn take_exit_result(&mut self, pid: u64) -> Result<JsValue, WasmRuntimeError> {
        self.vm
            .take_exit_result(pid)
            .map_err(WasmRuntimeError::from_js)
    }
}

impl fmt::Debug for WasmShardRuntime {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WasmShardRuntime")
            .field("shard_id", &self.shard_id)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WasmRuntimeError {
    InvalidShardCount,
    ShardRuntimeOnMainThread,
    Browser(String),
}

impl WasmRuntimeError {
    fn from_js(value: JsValue) -> Self {
        Self::Browser(js_error_message(value))
    }
}

impl fmt::Display for WasmRuntimeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidShardCount => write!(formatter, "at least one shard worker is required"),
            Self::ShardRuntimeOnMainThread => write!(
                formatter,
                "WASM shard runtimes must be booted inside a web worker"
            ),
            Self::Browser(message) => write!(formatter, "browser runtime error: {message}"),
        }
    }
}

impl std::error::Error for WasmRuntimeError {}

enum WorkerCommand {
    Boot { shard_id: usize },
    RunStep,
}

impl WorkerCommand {
    fn to_js(&self) -> JsValue {
        match self {
            Self::Boot { shard_id } => {
                let command = js_sys::Object::new();
                set_property(&command, "type", &JsValue::from_str("boot"));
                set_property(&command, "shardId", &JsValue::from_f64(*shard_id as f64));
                command.into()
            }
            Self::RunStep => {
                let command = js_sys::Object::new();
                set_property(&command, "type", &JsValue::from_str("runStep"));
                command.into()
            }
        }
    }
}

fn is_worker_scope() -> bool {
    js_sys::global().is_instance_of::<web_sys::WorkerGlobalScope>()
}

fn set_property(target: &js_sys::Object, name: &str, value: &JsValue) {
    drop(js_sys::Reflect::set(target, &JsValue::from_str(name), value));
}

fn js_error_message(value: JsValue) -> String {
    value
        .as_string()
        .or_else(|| js_sys::Error::from(value).message().as_string())
        .unwrap_or_else(|| "unknown JavaScript error".to_owned())
}
