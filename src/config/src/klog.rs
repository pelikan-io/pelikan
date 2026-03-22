// Copyright 2021 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use crate::units::*;
use serde::{Deserialize, Serialize};

////////////////////////////////////////////////////////////////////////////////
// constants to define default values
////////////////////////////////////////////////////////////////////////////////

// log to the file path
const FILE: Option<String> = None;

// max log size before rotate in bytes
const MAX_SIZE: u64 = GB as u64;

// log 1 in every N commands
const SAMPLE: usize = 100;

// max number of rotated log files to keep
const MAX_KEEP_FILES: u64 = 3;

////////////////////////////////////////////////////////////////////////////////
// helper functions
////////////////////////////////////////////////////////////////////////////////

fn file() -> Option<String> {
    FILE
}

fn max_size() -> u64 {
    MAX_SIZE
}

fn sample() -> usize {
    SAMPLE
}

fn max_keep_files() -> u64 {
    MAX_KEEP_FILES
}

////////////////////////////////////////////////////////////////////////////////
// struct definitions
////////////////////////////////////////////////////////////////////////////////

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Klog {
    #[serde(default = "file")]
    file: Option<String>,
    #[serde(default = "max_size")]
    #[serde(with = "crate::human_size::as_u64")]
    max_size: u64,
    #[serde(default = "max_keep_files")]
    max_keep_files: u64,
    #[serde(default = "sample")]
    sample: usize,
}

////////////////////////////////////////////////////////////////////////////////
// implementation
////////////////////////////////////////////////////////////////////////////////

impl Klog {
    pub fn file(&self) -> Option<String> {
        self.file.clone()
    }

    pub fn max_size(&self) -> u64 {
        self.max_size
    }

    pub fn sample(&self) -> usize {
        self.sample
    }

    pub fn max_keep_files(&self) -> u64 {
        self.max_keep_files
    }
}

// trait implementations
impl Default for Klog {
    fn default() -> Self {
        Self {
            file: file(),
            max_size: max_size(),
            max_keep_files: max_keep_files(),
            sample: sample(),
        }
    }
}

// trait definitions
pub trait KlogConfig {
    fn klog(&self) -> &Klog;
}
