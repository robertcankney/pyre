extern crate test;

use std::collections::{BTreeMap, HashMap};
use std::ops::Index;
use std::sync::{Arc, Mutex};
use std::time;

pub const DEFAULT_PARTITIONS: u32 = 1024;
pub const DEFAULT_TTL: i64 = 300;
pub const DEFAULT_WINDOW: u64 = 60;

pub struct Local {
    partition_count: u32,
    ttl: i64,
    // TODO check if we can remove Arc, since each Local will be behind an Arc
    partitions: Vec<Arc<Mutex<KeyMap>>>,
    clock: fn() -> u64,
}

#[derive(Default)]
pub struct KeyMap {
    ttls: HashMap<String, TTLValues>,
}

pub struct Key<'a> {
    k: &'a str,
    ts: u64,
}

pub struct TTLValues {
    window: u64,
    vals: BTreeMap<u64, u64>,
}

#[derive(Debug, Clone)]
pub struct CacheError {
    msg: String,
}

impl std::fmt::Display for CacheError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "error internal to local cache: {}", self.msg)
    }
}

impl TTLValues {
    fn find_bucket(&self, val: u64) -> u64 {
        match self.vals.iter().next_back() {
            Some(n) => {
                let b = if val.abs_diff(*n.0) < self.window {
                    *n.0
                } else {
                    val
                };

                b
            }
            None => val,
        }
    }
    pub fn get(&self, val: u64) -> u64 {
        let bucket = self.find_bucket(val);
        *self.vals.get(&bucket).unwrap_or(&0)
    }

    pub fn inc(&mut self, val: u64) -> u64 {
        let bucket = self.find_bucket(val);
        let updated = self.get(bucket) + 1;

        self.vals.insert(bucket, updated);
        bucket
    }

    pub fn inc_and_get(&mut self, val: u64) -> u64 {
        let new_key = self.inc(val);
        self.get(new_key)
    }

    pub fn new(window: u64) -> Self {
        Self {
            window,
            vals: BTreeMap::new(),
        }
    }
}

impl Default for TTLValues {
    fn default() -> Self {
        Self {
            window: DEFAULT_WINDOW,
            vals: BTreeMap::new(),
        }
    }
}

#[cfg(test)]
mod ttlvalues_tests {

    use super::*;

    #[test]
    fn test_new_ttlvalue() {
        let val = TTLValues::default();
        assert_eq!(val.window, DEFAULT_WINDOW);

        let val = TTLValues::new(100);
        assert_eq!(val.window, 100);
    }

    #[test]
    fn test_get() {
        let mut val = TTLValues::default();
        val.inc(1000);
        assert_eq!(val.get(1000), 1);
        assert_eq!(val.get(2000), 0)
    }

    #[test]
    fn test_inc_and_get() {
        let mut val = TTLValues::default();
        val.inc(1000);
        assert_eq!(val.inc_and_get(1000), 2);
        assert_eq!(val.inc_and_get(2000), 1)
    }

    #[test]
    fn test_inc() {
        let mut val = TTLValues::new(1000);
        val.inc(1000);
        val.inc(1200);
        val.inc(2000);
        val.inc(2999);
        val.inc(5005);

        assert_eq!(val.get(1000), 2);
        assert_eq!(val.get(2000), 2);
        assert_eq!(val.get(5005), 1);
    }
}

impl KeyMap {
    pub fn new() -> KeyMap {
        KeyMap::default()
    }

    pub fn get_or_create(&mut self, key: Key, create: bool) -> u64 {
        match self.ttls.get_mut(key.k) {
            Some(val) => match create {
                true => val.inc_and_get(key.ts),
                false => val.get(key.ts),
            },
            None => match create {
                true => {
                    let mut val = TTLValues::default();
                    let state = val.inc_and_get(key.ts);
                    self.ttls.insert(key.k.to_string(), val);

                    state
                }
                false => 0,
            },
        }
    }
}

#[cfg(test)]
mod keymap_tests {

    use super::*;

    #[test]
    fn test_get_or_create() {
        let mut km = KeyMap::new();

        km.get_or_create(
            Key {
                k: "foo",
                ts: 10000,
            },
            true,
        );

        let foo_val = km.get_or_create(
            Key {
                k: "foo",
                ts: 10005,
            },
            true,
        );

        let bar_val = km.get_or_create(
            Key {
                k: "bar",
                ts: 10100,
            },
            true,
        );

        assert_eq!(
            km.get_or_create(
                Key {
                    k: "bar",
                    ts: 10101,
                },
                true
            ),
            bar_val + 1
        );

        assert_eq!(
            km.get_or_create(
                Key {
                    k: "foo",
                    ts: 10050,
                },
                true
            ),
            foo_val + 1
        );

        assert_eq!(
            km.get_or_create(
                Key {
                    k: "bar",
                    ts: 10101,
                },
                false
            ),
            bar_val + 1
        );

        assert_eq!(
            km.get_or_create(
                Key {
                    k: "foo",
                    ts: 10050,
                },
                false
            ),
            foo_val + 1
        );

        assert_eq!(
            km.get_or_create(
                Key {
                    k: "foobarfoo",
                    ts: 10050,
                },
                false
            ),
            0
        );

        assert_eq!(
            km.get_or_create(
                Key {
                    k: "foo",
                    ts: 10150,
                },
                true
            ),
            1
        );

        assert_eq!(
            km.get_or_create(
                Key {
                    k: "foobar",
                    ts: 10150,
                },
                true
            ),
            1
        );
    }
}

impl Local {
    fn default_clock() -> u64 {
        time::SystemTime::now()
            .duration_since(time::UNIX_EPOCH)
            .expect("can't get duration since UNIX 0 - this is a bug in the code")
            .as_secs()
    }

    pub fn new(partition_count: u32, ttl: i64) -> Self {
        Self {
            partition_count,
            partitions: {
                let mut v = Vec::with_capacity(partition_count as usize);
                (0..partition_count as usize)
                    .for_each(|_| v.push(Arc::new(Mutex::new(KeyMap::default()))));
                v
            },
            clock: Local::default_clock,
            ttl,
        }
    }

    pub fn ttl(&self) -> i64 {
        self.ttl
    }

    pub fn get_or_create(&self, key: &str, create: bool) -> Result<u64, CacheError> {
        let inner_clone = {
            let partition = twox_hash::xxh3::hash64(key.as_bytes()) as u32 % self.partition_count;
            let inner = self.partitions.index(partition as usize);
            inner.clone()
        };

        let mut lock = match inner_clone.lock() {
            Ok(l) => l,
            Err(e) => {
                return Err(CacheError {
                    msg: format!("failed to get partition lock: {}", e),
                })
            }
        };

        let val = lock.get_or_create(
            Key {
                k: key,
                ts: (self.clock)(),
            },
            create,
        );

        Ok(val)
    }
}

impl Default for Local {
    fn default() -> Self {
        Self {
            partition_count: DEFAULT_PARTITIONS,
            partitions: {
                let mut v = Vec::with_capacity(DEFAULT_PARTITIONS as usize);
                (0..DEFAULT_PARTITIONS as usize)
                    .for_each(|_| v.push(Arc::new(Mutex::new(KeyMap::default()))));
                v
            },
            clock: Local::default_clock,
            ttl: DEFAULT_TTL,
        }
    }
}

unsafe impl Sync for Local {}

#[cfg(test)]
mod local_tests {

    extern crate test;

    use super::*;
    use rand::Rng;

    #[test]
    fn test_new_local() {
        let local = Local::new(5, 30);
        assert_eq!(local.partition_count, 5);
        assert_eq!(local.ttl, 30);

        let local = Local::default();
        assert_eq!(local.partition_count, DEFAULT_PARTITIONS);
        assert_eq!(local.ttl, DEFAULT_TTL);
    }

    #[test]
    fn test_get_or_create() {
        let mut local = Local::new(10, 30);
        let val = local.get_or_create("foo", true);
        let inner = val.unwrap();
        assert_eq!(inner, 1);

        let val = local.get_or_create("foo", true);
        let inner = val.unwrap();
        assert_eq!(inner, 2);

        let val = local.get_or_create("bar", true);
        let inner = val.unwrap();
        assert_eq!(inner, 1);

        let val = local.get_or_create("foobar", false);
        let inner = val.unwrap();
        assert_eq!(inner, 0);
    }

    #[test]
    fn test_get_or_create_concurrent() {
        let local = Arc::new(Local::new(10, 30));
        // let local_protected = Arc::new(Mutex::new(local));

        let mut threads = Vec::new();
        for i in 0..9 {
            println!("thread_num {}", i);
            let lp = local.clone();
            let t = std::thread::spawn(move || {
                // let mut l = lp.lock().expect("unable to get Local lock");
                if let Err(e) = lp.get_or_create("foo", true) {
                    panic!("failed to get get_or_create: {}", e.to_string());
                }
            });
            threads.push(t);
        }

        for i in threads {
            let _ = i.join();
        }

        let val = local
            .get_or_create("foo", true)
            .expect("failed to get Local lock");

        assert_eq!(val, 10);
    }

    #[bench]
    fn bench_get_or_create_concurrent(b: &mut test::Bencher) {
        b.iter(|| {
            let local = Local::new(128, 300);
            let local_protected = Arc::new(local);
            let mut data = vec![Vec::new(); 2];
            let mut rng = rand::thread_rng();

            for i in &mut data {
                for _ in 0..500000 {
                    let val: i32 = rng.gen();
                    i.push(val.to_string());
                }
            }

            let mut data_iter = data.into_iter();
            let mut threads = Vec::new();

            let now = time::SystemTime::now();
            for _i in 0..2 {
                let lp = local_protected.clone();
                let keys = data_iter.next().unwrap();
                let t = std::thread::spawn(move || {
                    for key in keys {
                        // let mut l = lp.lock().expect("unable to get Local lock");
                        if let Err(e) = lp.get_or_create(&key, true) {
                            panic!("failed to get get_or_create: {}", e.to_string());
                        }
                    }
                });
                threads.push(t);
            }

            for i in threads {
                let _ = i.join();
            }

            let done = time::SystemTime::now();
            println!("done: {}", done.duration_since(now).unwrap().as_micros())
        })
    }
}
