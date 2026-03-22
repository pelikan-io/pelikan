// Copyright 2020 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use log::Level;
use serde::{Deserialize, Serialize};

// constants to define default values
const LOG_LEVEL: Level = Level::Info;
const LOG_FILE: Option<String> = None;
const LOG_MAX_KEEP_FILES: u64 = 7;
const LOG_ROTATION_INTERVAL: LogRotationInterval = LogRotationInterval::Daily;

// helper functions
fn log_level() -> Level {
    LOG_LEVEL
}

fn log_file() -> Option<String> {
    LOG_FILE
}

fn log_max_keep_files() -> u64 {
    LOG_MAX_KEEP_FILES
}

fn log_rotation_interval() -> LogRotationInterval {
    LOG_ROTATION_INTERVAL
}

// struct definitions
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Debug {
    #[serde(with = "LevelDef")]
    #[serde(default = "log_level")]
    log_level: Level,
    #[serde(default = "log_file")]
    log_file: Option<String>,
    #[serde(default = "log_max_keep_files")]
    log_max_keep_files: u64,
    #[serde(default = "log_rotation_interval")]
    log_rotation_interval: LogRotationInterval,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum LogRotationInterval {
    None,
    Minutely,
    Hourly,
    Daily,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
#[serde(remote = "Level")]
#[serde(deny_unknown_fields)]
enum LevelDef {
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}

// implementation
impl Debug {
    pub fn log_level(&self) -> Level {
        self.log_level
    }

    pub fn log_file(&self) -> Option<String> {
        self.log_file.clone()
    }

    pub fn log_max_keep_files(&self) -> u64 {
        self.log_max_keep_files
    }

    pub fn log_rotation_interval(&self) -> LogRotationInterval {
        self.log_rotation_interval
    }
}

// trait implementations
impl Default for Debug {
    fn default() -> Self {
        Self {
            log_level: log_level(),
            log_file: log_file(),
            log_max_keep_files: log_max_keep_files(),
            log_rotation_interval: log_rotation_interval(),
        }
    }
}

// trait definitions
pub trait DebugConfig {
    fn debug(&self) -> &Debug;
}
