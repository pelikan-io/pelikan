// Copyright 2022 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use crate::*;

pub trait Storage {
    fn add(&self, request: &Add) -> Response;
    fn append(&self, request: &Append) -> Response;
    fn cas(&self, request: &Cas) -> Response;
    fn decr(&self, request: &Decr) -> Response;
    fn delete(&self, request: &Delete) -> Response;
    fn flush_all(&self, request: &FlushAll) -> Response;
    fn get(&self, request: &Get) -> Response;
    fn gets(&self, request: &Get) -> Response;
    fn incr(&self, request: &Incr) -> Response;
    fn prepend(&self, request: &Prepend) -> Response;
    fn quit(&self, request: &Quit) -> Response;
    fn replace(&self, request: &Replace) -> Response;
    fn set(&self, request: &Set) -> Response;
}
