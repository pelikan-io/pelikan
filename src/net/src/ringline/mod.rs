mod completion;
mod launch;

pub use ::ringline::{AsyncEventHandler, ConnCtx, DriverCtx, Error, ParseResult, WakeHandle};
pub use completion::{Completion, CompletionCanceled, CompletionId, CompletionTable};
pub use launch::{
    launch, launch_with_bootstraps, take_worker_bootstrap, RinglineRuntime, RinglineShutdown,
};

/// Settings for a Ringline cache-server runtime.
#[derive(Clone, Copy, Debug)]
pub struct RinglineRuntimeConfig {
    pub workers: usize,
    pub max_connections: u32,
    pub recv_buffers: u16,
    pub recv_buffer_size: u32,
    pub pin_to_core: bool,
}

impl RinglineRuntimeConfig {
    fn build(self) -> Result<::ringline::Config, ::ringline::Error> {
        ::ringline::ConfigBuilder::new()
            .workers(self.workers)
            .pin_to_core(self.pin_to_core)
            .max_connections(self.max_connections)
            .recv_buffer(self.recv_buffers, self.recv_buffer_size)
            .build()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_config_builds_ringline_config() {
        let config = RinglineRuntimeConfig {
            workers: 1,
            max_connections: 128,
            recv_buffers: 64,
            recv_buffer_size: 4096,
            pin_to_core: false,
        };
        assert!(config.build().is_ok());
    }

    #[test]
    fn facade_reexports_ringline_api() {
        let _: Option<WakeHandle> = None;
    }
}
