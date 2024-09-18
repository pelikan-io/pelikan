use crate::tokio::RUNNING;
use core::sync::atomic::Ordering;
use std::sync::Arc;
use tonic::transport::Server as TonicServer;
use tonic::{Request as TonicRequest, Response as TonicResponse, Status as TonicStatus};

use pingpong::ping_server::{Ping, PingServer};
use pingpong::{PingRequest, PongResponse};

pub mod pingpong {
    tonic::include_proto!("pingpong");
}

use crate::config::Config;

#[derive(Debug, Default)]
pub struct Server {}

#[tonic::async_trait]
impl Ping for Server {
    async fn ping(
        &self,
        _request: TonicRequest<PingRequest>,
    ) -> Result<TonicResponse<PongResponse>, TonicStatus> {
        Ok(TonicResponse::new(PongResponse {}))
    }
}

pub async fn run(config: Arc<Config>) {
    tokio::spawn(async move {
        if let Err(e) = TonicServer::builder()
            .add_service(PingServer::new(Server::default()))
            .serve(config.listen())
            .await
        {
            error!("{e}");
        };

        RUNNING.store(false, Ordering::Relaxed);
    });
}
