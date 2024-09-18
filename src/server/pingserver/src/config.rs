use config::*;
use core::net::SocketAddr;
use core::time::Duration;
use serde::{Deserialize, Serialize};
use std::io::Read;

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct Config {
    pub general: General,
    pub metrics: Metrics,

    // application modules
    #[serde(default)]
    pub admin: Admin,
    #[serde(default)]
    pub server: Server,
    #[serde(default)]
    pub worker: Worker,
    #[serde(default)]
    pub time: Time,
    #[serde(default)]
    pub tls: Tls,

    // ccommon
    #[serde(default)]
    pub buf: Buf,
    #[serde(default)]
    pub debug: Debug,
    #[serde(default)]
    pub klog: Klog,
    #[serde(default)]
    pub sockio: Sockio,
    #[serde(default)]
    pub tcp: Tcp,
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct General {
    pub engine: Engine,
    pub protocol: Protocol,
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct Metrics {
    #[serde(default = "interval")]
    pub interval: String,
}

impl Metrics {
    pub fn interval(&self) -> Duration {
        self.interval.parse::<humantime::Duration>().unwrap().into()
    }
}

fn interval() -> String {
    "1s".into()
}

#[derive(Serialize, Deserialize, Debug, Default, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum Engine {
    #[default]
    Mio,
    Tokio,
}

#[derive(Serialize, Deserialize, Debug, Default, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum Protocol {
    #[default]
    Ascii,
    Grpc,
    Http2,
    Http3,
}

impl Config {
    pub fn load(file: &str) -> Result<Self, std::io::Error> {
        let mut file = std::fs::File::open(file)?;
        let mut content = String::new();
        file.read_to_string(&mut content)?;

        let config: Config = match toml::from_str(&content) {
            Ok(t) => t,
            Err(e) => {
                error!("{}", e);
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Error parsing config",
                ));
            }
        };

        if config.general.protocol == Protocol::Grpc && config.general.engine == Engine::Mio {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "GRPC support requires using the Tokio engine",
            ));
        }

        match config.metrics.interval.parse::<humantime::Duration>() {
            Ok(interval) => {
                if Into::<Duration>::into(interval) < Duration::from_millis(10) {
                    eprintln!("metrics interval cannot be less than 10ms");
                    std::process::exit(1);
                }
            }
            Err(e) => {
                eprintln!("metrics interval is not valid: {e}");
                std::process::exit(1);
            }
        }

        Ok(config)
    }

    pub fn listen(&self) -> SocketAddr {
        self.server
            .socket_addr()
            .map_err(|e| {
                error!("{}", e);
                std::io::Error::new(std::io::ErrorKind::Other, "Bad listen address")
            })
            .map_err(|_| {
                std::process::exit(1);
            })
            .unwrap()
    }
}

impl AdminConfig for Config {
    fn admin(&self) -> &Admin {
        &self.admin
    }
}

impl BufConfig for Config {
    fn buf(&self) -> &Buf {
        &self.buf
    }
}

impl DebugConfig for Config {
    fn debug(&self) -> &Debug {
        &self.debug
    }
}

impl KlogConfig for Config {
    fn klog(&self) -> &Klog {
        &self.klog
    }
}

impl ServerConfig for Config {
    fn server(&self) -> &Server {
        &self.server
    }
}

impl SockioConfig for Config {
    fn sockio(&self) -> &Sockio {
        &self.sockio
    }
}

impl TcpConfig for Config {
    fn tcp(&self) -> &Tcp {
        &self.tcp
    }
}

impl TimeConfig for Config {
    fn time(&self) -> &Time {
        &self.time
    }
}

impl TlsConfig for Config {
    fn tls(&self) -> &Tls {
        &self.tls
    }
}

impl WorkerConfig for Config {
    fn worker(&self) -> &Worker {
        &self.worker
    }

    fn worker_mut(&mut self) -> &mut Worker {
        &mut self.worker
    }
}
