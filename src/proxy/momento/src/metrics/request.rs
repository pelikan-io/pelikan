use super::*;

/*
 * GET
 */

#[metric(name = "get")]
pub static GET: Counter = Counter::new();

#[metric(name = "get_ex")]
pub static GET_EX: Counter = Counter::new();

#[metric(name = "get_key")]
pub static GET_KEY: Counter = Counter::new();

#[metric(name = "get_key_hit")]
pub static GET_KEY_HIT: Counter = Counter::new();

#[metric(name = "get_key_miss")]
pub static GET_KEY_MISS: Counter = Counter::new();

#[metric(
    name = "get_cardinality",
    description = "distribution of key cardinality for get requests"
)]
pub static GET_CARDINALITY: AtomicHistogram = AtomicHistogram::new(7, 20);

/*
 * GETS
 */

#[metric(name = "gets")]
pub static GETS: Counter = Counter::new();

#[metric(name = "gets_ex")]
pub static GETS_EX: Counter = Counter::new();

#[metric(name = "gets_key")]
pub static GETS_KEY: Counter = Counter::new();

#[metric(name = "gets_key_hit")]
pub static GETS_KEY_HIT: Counter = Counter::new();

#[metric(name = "gets_key_miss")]
pub static GETS_KEY_MISS: Counter = Counter::new();

#[metric(
    name = "gets_cardinality",
    description = "distribution of key cardinality for gets requests"
)]
pub static GETS_CARDINALITY: AtomicHistogram = AtomicHistogram::new(7, 20);

/*
 * SET
 */

#[metric(name = "set")]
pub static SET: Counter = Counter::new();

#[metric(name = "set_ex")]
pub static SET_EX: Counter = Counter::new();

#[metric(name = "set_stored")]
pub static SET_STORED: Counter = Counter::new();

#[metric(name = "set_not_stored")]
pub static SET_NOT_STORED: Counter = Counter::new();

/*
 * DELETE
 */

#[metric(name = "delete")]
pub static DELETE: Counter = Counter::new();

#[metric(name = "delete_ex")]
pub static DELETE_EX: Counter = Counter::new();

#[metric(name = "delete_deleted")]
pub static DELETE_DELETED: Counter = Counter::new();

#[metric(name = "delete_not_found")]
pub static DELETE_NOT_FOUND: Counter = Counter::new();
