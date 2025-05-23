// Copyright 2021 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

//! This module defines how `Seg` storage will be used to execute `Memcache`
//! storage commands.

use super::*;
use protocol_common::*;

use protocol_memcache::Value;
use protocol_memcache::*;

use std::time::Duration;

impl Execute<Request, Response> for Seg {
    fn execute(&mut self, request: &Request) -> Response {
        match request {
            Request::Get(get) => {
                if get.cas() {
                    self.gets(get)
                } else {
                    self.get(get)
                }
            }
            Request::Set(set) => self.set(set),
            Request::Add(add) => self.add(add),
            Request::Replace(replace) => self.replace(replace),
            Request::Cas(cas) => self.cas(cas),
            Request::Incr(incr) => self.incr(incr),
            Request::Decr(decr) => self.decr(decr),
            Request::Append(append) => self.append(append),
            Request::Prepend(prepend) => self.prepend(prepend),
            Request::Delete(delete) => self.delete(delete),
            Request::FlushAll(flush_all) => self.flush_all(flush_all),
            Request::Quit(quit) => self.quit(quit),
        }
    }
}

impl Storage for Seg {
    fn get(&mut self, get: &Get) -> Response {
        let mut values = Vec::with_capacity(get.keys().len());
        for key in get.keys().iter() {
            if let Some(item) = self.data.get(key) {
                let o = item.optional().unwrap_or(&[0, 0, 0, 0]);
                let flags = u32::from_be_bytes([o[0], o[1], o[2], o[3]]);
                match item.value() {
                    segcache::Value::Bytes(b) => {
                        values.push(Value::new(item.key(), flags, None, b));
                    }
                    segcache::Value::U64(v) => {
                        values.push(Value::new(
                            item.key(),
                            flags,
                            None,
                            format!("{v}").as_bytes(),
                        ));
                    }
                }
            } else {
                values.push(Value::none(key));
            }
        }
        Values::new(values.into_boxed_slice()).into()
    }

    fn gets(&mut self, get: &Get) -> Response {
        let mut values = Vec::with_capacity(get.keys().len());
        for key in get.keys().iter() {
            if let Some(item) = self.data.get(key) {
                let o = item.optional().unwrap_or(&[0, 0, 0, 0]);
                let flags = u32::from_be_bytes([o[0], o[1], o[2], o[3]]);
                match item.value() {
                    segcache::Value::Bytes(b) => {
                        values.push(Value::new(item.key(), flags, Some(item.cas().into()), b));
                    }
                    segcache::Value::U64(v) => {
                        values.push(Value::new(
                            item.key(),
                            flags,
                            Some(item.cas().into()),
                            format!("{v}").as_bytes(),
                        ));
                    }
                }
            } else {
                values.push(Value::none(key));
            }
        }
        Values::new(values.into_boxed_slice()).into()
    }

    fn set(&mut self, set: &Set) -> Response {
        let ttl = set.ttl().get().unwrap_or(0);

        if ttl < 0 {
            // immediate expire maps to a delete
            self.data.delete(set.key());
            Response::stored(set.noreply())
        } else if let Ok(s) = std::str::from_utf8(set.value()) {
            if let Ok(v) = s.parse::<u64>() {
                if self
                    .data
                    .insert(
                        set.key(),
                        v,
                        Some(&set.flags().to_be_bytes()),
                        Duration::from_secs(ttl as u64),
                    )
                    .is_ok()
                {
                    Response::stored(set.noreply())
                } else {
                    Response::server_error("")
                }
            } else if self
                .data
                .insert(
                    set.key(),
                    set.value(),
                    Some(&set.flags().to_be_bytes()),
                    Duration::from_secs(ttl as u64),
                )
                .is_ok()
            {
                Response::stored(set.noreply())
            } else {
                Response::server_error("")
            }
        } else if self
            .data
            .insert(
                set.key(),
                set.value(),
                Some(&set.flags().to_be_bytes()),
                Duration::from_secs(ttl as u64),
            )
            .is_ok()
        {
            Response::stored(set.noreply())
        } else {
            Response::server_error("")
        }
    }

    fn add(&mut self, add: &Add) -> Response {
        if self.data.get_no_freq_incr(add.key()).is_some() {
            return Response::not_stored(add.noreply());
        }

        let ttl = add.ttl().get().unwrap_or(0);

        if ttl < 0 {
            // immediate expire maps to a delete
            self.data.delete(add.key());
            Response::stored(add.noreply())
        } else if let Ok(s) = std::str::from_utf8(add.value()) {
            if let Ok(v) = s.parse::<u64>() {
                if self
                    .data
                    .insert(
                        add.key(),
                        v,
                        Some(&add.flags().to_be_bytes()),
                        Duration::from_secs(ttl as u64),
                    )
                    .is_ok()
                {
                    Response::stored(add.noreply())
                } else {
                    Response::server_error("")
                }
            } else if self
                .data
                .insert(
                    add.key(),
                    add.value(),
                    Some(&add.flags().to_be_bytes()),
                    Duration::from_secs(ttl as u64),
                )
                .is_ok()
            {
                Response::stored(add.noreply())
            } else {
                Response::server_error("")
            }
        } else if self
            .data
            .insert(
                add.key(),
                add.value(),
                Some(&add.flags().to_be_bytes()),
                Duration::from_secs(ttl as u64),
            )
            .is_ok()
        {
            Response::stored(add.noreply())
        } else {
            Response::server_error("")
        }
    }

    fn replace(&mut self, replace: &Replace) -> Response {
        if self.data.get_no_freq_incr(replace.key()).is_none() {
            return Response::not_stored(replace.noreply());
        }

        let ttl = replace.ttl().get().unwrap_or(0);

        if ttl < 0 {
            // immediate expire maps to a delete
            self.data.delete(replace.key());
            Response::stored(replace.noreply())
        } else if let Ok(s) = std::str::from_utf8(replace.value()) {
            if let Ok(v) = s.parse::<u64>() {
                if self
                    .data
                    .insert(
                        replace.key(),
                        v,
                        Some(&replace.flags().to_be_bytes()),
                        Duration::from_secs(ttl as u64),
                    )
                    .is_ok()
                {
                    Response::stored(replace.noreply())
                } else {
                    Response::server_error("")
                }
            } else if self
                .data
                .insert(
                    replace.key(),
                    replace.value(),
                    Some(&replace.flags().to_be_bytes()),
                    Duration::from_secs(ttl as u64),
                )
                .is_ok()
            {
                Response::stored(replace.noreply())
            } else {
                Response::server_error("")
            }
        } else if self
            .data
            .insert(
                replace.key(),
                replace.value(),
                Some(&replace.flags().to_be_bytes()),
                Duration::from_secs(ttl as u64),
            )
            .is_ok()
        {
            Response::stored(replace.noreply())
        } else {
            Response::server_error("")
        }
    }

    fn append(&mut self, _: &Append) -> Response {
        Response::error()
    }

    fn prepend(&mut self, _: &Prepend) -> Response {
        Response::error()
    }

    fn incr(&mut self, incr: &Incr) -> Response {
        match self.data.wrapping_add(incr.key(), incr.value()) {
            Ok(item) => match item.value() {
                segcache::Value::U64(v) => Response::numeric(v, incr.noreply()),
                _ => Response::server_error(""),
            },
            Err(SegcacheError::NotFound) => Response::not_found(incr.noreply()),
            Err(SegcacheError::NotNumeric) => Response::error(),
            Err(_) => Response::server_error(""),
        }
    }

    fn decr(&mut self, decr: &Decr) -> Response {
        match self.data.saturating_sub(decr.key(), decr.value()) {
            Ok(item) => match item.value() {
                segcache::Value::U64(v) => Response::numeric(v, decr.noreply()),
                _ => Response::server_error(""),
            },
            Err(SegcacheError::NotFound) => Response::not_found(decr.noreply()),
            Err(SegcacheError::NotNumeric) => Response::error(),
            Err(_) => Response::server_error(""),
        }
    }

    fn cas(&mut self, cas: &Cas) -> Response {
        // TTL of None means that it doesn't expire. In `Seg` storage
        // a TTL of zero maps to the longest TTL representable which
        // is ~97 days.

        let ttl = cas.ttl().get().unwrap_or(0);

        // Since we allow specifying Unix timestamps as TTLs, we can actually
        // make a request that says that if the CAS value matches, the item
        // should be immediately expired.
        //
        // However, we cannot check the CAS value without attempting to store,
        // and we can't store items that are already expired.
        //
        // As a hack, we will first attempt the CAS with a near immediate
        // expiration (shortest possible is 1 second). On success we can delete
        // the item so that subsequent reads cannot return an item that
        // should've already been expired.

        let mut delete_after = false;

        let ttl = if ttl < 0 {
            delete_after = true;
            Duration::from_secs(1)
        } else {
            Duration::from_secs(ttl as u64)
        };

        let response = if let Ok(s) = std::str::from_utf8(cas.value()) {
            if let Ok(v) = s.parse::<u64>() {
                match self.data.cas(
                    cas.key(),
                    v,
                    Some(&cas.flags().to_be_bytes()),
                    ttl,
                    cas.cas() as u32,
                ) {
                    Ok(_) => Response::stored(cas.noreply()),
                    Err(SegcacheError::NotFound) => Response::not_found(cas.noreply()),
                    Err(SegcacheError::Exists) => Response::exists(cas.noreply()),
                    Err(_) => Response::error(),
                }
            } else {
                match self.data.cas(
                    cas.key(),
                    cas.value(),
                    Some(&cas.flags().to_be_bytes()),
                    ttl,
                    cas.cas() as u32,
                ) {
                    Ok(_) => Response::stored(cas.noreply()),
                    Err(SegcacheError::NotFound) => Response::not_found(cas.noreply()),
                    Err(SegcacheError::Exists) => Response::exists(cas.noreply()),
                    Err(_) => Response::error(),
                }
            }
        } else {
            match self.data.cas(
                cas.key(),
                cas.value(),
                Some(&cas.flags().to_be_bytes()),
                ttl,
                cas.cas() as u32,
            ) {
                Ok(_) => Response::stored(cas.noreply()),
                Err(SegcacheError::NotFound) => Response::not_found(cas.noreply()),
                Err(SegcacheError::Exists) => Response::exists(cas.noreply()),
                Err(_) => Response::error(),
            }
        };

        // If CAS was successful and TTL was in the past, we now delete the
        // item.
        if delete_after {
            if let Response::Stored(_) = response {
                self.data.delete(cas.key());
            }
        }

        response
    }

    fn delete(&mut self, delete: &Delete) -> Response {
        if self.data.delete(delete.key()) {
            Response::deleted(delete.noreply())
        } else {
            Response::not_found(delete.noreply())
        }
    }

    fn flush_all(&mut self, _flush_all: &FlushAll) -> Response {
        Response::error()
    }

    fn quit(&mut self, _quit: &Quit) -> Response {
        Response::hangup()
    }
}
