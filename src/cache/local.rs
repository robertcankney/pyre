extern crate test;

use std::collections::{BTreeMap, HashMap};
use std::ops::Index;
use std::sync::{atomic::AtomicU64, atomic::Ordering::Relaxed, Mutex};
use std::time;
use tokio;

pub const DEFAULT_PARTITIONS: u32 = 1024;
pub const DEFAULT_TTL: u64 = 300;
pub const DEFAULT_WINDOW: u64 = 60;
pub const DEFAULT_SWEEP: u64 = 30;

#[derive(Debug)]
pub struct Local {
    partition_count: u32,
    ttl: u64,
    sweep: u64,
    partitions: Vec<Mutex<KeyMap>>,
    clock: AtomicU64,
}

#[derive(Default, Debug)]
pub struct KeyMap {
    window: u64,
    ttls: HashMap<String, TTLValues>,
}

pub struct Key<'a> {
    k: &'a str,
    ts: u64,
}

#[derive(Debug)]
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
                if val.abs_diff(*n.0) < self.window {
                    *n.0
                } else {
                    val
                }
            }
            None => val,
        }
    }

    fn get_inner(&self, val: u64) -> u64 {
        let bucket = self.find_bucket(val);
        *self.vals.get(&bucket).unwrap_or(&0)
    }

    pub fn get(&self) -> u64 {
        self.vals.iter().fold(0, |accum, (_, v)| accum + *v)
    }

    pub fn inc(&mut self, val: u64) -> u64 {
        let bucket = self.find_bucket(val);
        let updated = self.get_inner(bucket) + 1;

        self.vals.insert(bucket, updated);
        updated
    }

    pub fn inc_and_get(&mut self, val: u64) -> u64 {
        self.inc(val);
        self.get()
    }

    pub fn new(window: u64) -> Self {
        Self {
            window,
            vals: BTreeMap::new(),
        }
    }

    // very naive solution currently, attempting to keep to log(n).
    fn lru(&mut self, ttl: u64) {
        let mut wrapper = None;

        for (k, _) in self.vals.iter() {
            // inclusive TTL - i.e. if we are at 60 and have a TTL of 30, we will retain 30 and
            // higher
            if *k >= ttl {
                wrapper = Some(*k);
                break;
            }
        }

        let key = match wrapper {
            Some(k) => k,
            None => {
                // all keys are less than ttl, so clear
                self.vals.clear();
                return;
            }
        };

        self.vals = self.vals.split_off(&key);
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
    fn test_get_inner() {
        let mut val = TTLValues::default();
        val.inc(1000);
        val.inc(1010);
        println!("{:?}", val);
        assert_eq!(val.get_inner(1000), 2, "actual bucket");
        assert_eq!(val.get_inner(1050), 2, "within bucket");
        assert_eq!(val.get_inner(1100), 0, "outside bucket");
    }

    #[test]
    fn test_inc_and_get() {
        let mut val = TTLValues::default();
        val.inc(1000);
        assert_eq!(val.inc_and_get(1000), 2);
        assert_eq!(val.inc_and_get(2000), 3)
    }

    #[test]
    fn test_get() {
        struct TestCase {
            name: &'static str,
            inc: u64,
            val: u64,
        }

        let mut val = TTLValues::new(1000);
        let testcases = vec![
            TestCase {
                name: "initial bucket",
                inc: 1000,
                val: 1,
            },
            TestCase {
                name: "same bucket",
                inc: 1200,
                val: 2,
            },
            TestCase {
                name: "new bucket",
                inc: 2000,
                val: 3,
            },
            TestCase {
                name: "several buckets forward",
                inc: 7890,
                val: 4,
            },
        ];

        for tc in testcases {
            val.inc(tc.inc);
            let actual = val.get();
            assert_eq!(
                actual, tc.val,
                "val {} did not match for {}",
                actual, tc.name
            )
        }
    }

    #[test]
    fn test_inc() {
        struct TestCase {
            name: &'static str,
            inc: u64,
            get: u64,
            val: u64,
        }

        let mut val = TTLValues::new(1000);
        let testcases = vec![
            TestCase {
                name: "initial bucket",
                inc: 1000,
                val: 1,
                get: 1000,
            },
            TestCase {
                name: "same bucket",
                inc: 1200,
                val: 2,
                get: 1000,
            },
            TestCase {
                name: "new bucket",
                inc: 2000,
                val: 1,
                get: 2000,
            },
            TestCase {
                name: "test bucket edge",
                inc: 2999,
                val: 2,
                get: 2000,
            },
        ];

        for tc in testcases {
            val.inc(tc.inc);
            let actual = val.get_inner(tc.get);
            assert_eq!(
                actual, tc.val,
                "val {} did not match for {}",
                actual, tc.name
            )
        }
    }

    #[test]
    fn test_lru() {
        struct TestCase {
            name: &'static str,
            vals: Vec<u64>,
            ttl: u64,
            len: usize,
        }

        let cases = vec![
            TestCase {
                name: "empty ttlvalues",
                vals: vec![],
                ttl: 50,
                len: 0,
            },
            TestCase {
                name: "deletes 2, keeps 1",
                vals: vec![10, 20, 50],
                ttl: 30,
                len: 1,
            },
            TestCase {
                name: "deletes none",
                vals: vec![40, 50, 60],
                ttl: 30,
                len: 3,
            },
            TestCase {
                name: "deletes all",
                vals: vec![10, 20, 25],
                ttl: 30,
                len: 0,
            },
        ];

        for tc in cases {
            let mut tvalues = TTLValues::new(5);
            for v in tc.vals {
                tvalues.inc(v);
            }

            tvalues.lru(tc.ttl);
            assert_eq!(tvalues.vals.len(), tc.len, "test_case {}", tc.name);
        }
    }
}

impl KeyMap {
    pub fn new(window: u64) -> KeyMap {
        KeyMap {
            window,
            ttls: HashMap::new(),
        }
    }

    pub fn get_or_create(&mut self, key: Key, inc: bool) -> u64 {
        match self.ttls.get_mut(key.k) {
            Some(val) => match inc {
                true => val.inc_and_get(key.ts),
                false => val.get(),
            },
            None => match inc {
                true => {
                    let mut val = TTLValues::new(self.window);
                    let state = val.inc_and_get(key.ts);
                    self.ttls.insert(key.k.to_string(), val);

                    state
                }
                false => 0,
            },
        }
    }

    fn lru(&mut self, now: u64) {
        self.ttls.retain(|_, v| {
            v.lru(now);
            !v.vals.is_empty()
        });
    }
}

#[cfg(test)]
mod keymap_tests {

    use super::*;

    #[test]
    fn test_get_or_create() {
        struct TestCase {
            name: &'static str,
            key: Key<'static>,
            create: bool,
            val: u64,
        }

        let mut km = KeyMap::new(60);

        // badly need to clean these up
        // test: initial, update, no update, new window update, new window no update, new key no update, new key update
        let testcases = vec![
            TestCase {
                key: Key {
                    k: "foo",
                    ts: 10000,
                },
                create: true,
                val: 1,
                name: "first foo",
            },
            TestCase {
                key: Key {
                    k: "foo",
                    ts: 10005,
                },
                create: true,
                val: 2,
                name: "foo in same window",
            },
            TestCase {
                key: Key {
                    k: "foo",
                    ts: 10006,
                },
                create: false,
                val: 2,
                name: "foo in same window, no update",
            },
            TestCase {
                key: Key {
                    k: "foo",
                    ts: 10151,
                },
                create: true,
                val: 3,
                name: "foo in new window",
            },
            TestCase {
                key: Key {
                    k: "foo",
                    ts: 10200,
                },
                create: false,
                val: 3,
                name: "foo in new window, no update",
            },
            TestCase {
                key: Key {
                    k: "bar",
                    ts: 10100,
                },
                create: false,
                val: 0,
                name: "bar, no update",
            },
            TestCase {
                key: Key {
                    k: "bar",
                    ts: 10100,
                },
                create: true,
                val: 1,
                name: "bar, update",
            },
        ];

        for tc in testcases {
            let val = km.get_or_create(tc.key, tc.create);
            assert_eq!(
                val, tc.val,
                "val {} does not match expected val for case '{}'",
                val, tc.name
            )
        }
    }

    #[test]
    fn test_lru() {
        struct TestCase {
            name: &'static str,
            vals: HashMap<&'static str, Vec<u64>>,
            len: usize,
        }

        let cases = vec![
            TestCase {
                name: "delete 1, keep 2",
                vals: HashMap::from([
                    ("foo", vec![10, 20, 25]),
                    ("bar", vec![10, 20, 50]),
                    ("foobar", vec![40, 50, 60]),
                ]),
                len: 2,
            },
            TestCase {
                name: "delete all",
                vals: HashMap::from([
                    ("foo", vec![10, 20, 25]),
                    ("bar", vec![10, 20, 22]),
                    ("foobar", vec![5, 1, 28]),
                ]),
                len: 0,
            },
            TestCase {
                name: "delete none",
                vals: HashMap::from([
                    ("foo", vec![40, 50, 55]),
                    ("bar", vec![32, 37, 50]),
                    ("foobar", vec![40, 50, 60]),
                ]),
                len: 3,
            },
        ];

        for tc in cases {
            let mut km = KeyMap::new(30);

            for (k, v) in tc.vals {
                for vv in v {
                    km.get_or_create(Key { k, ts: vv }, true);
                }
            }

            km.lru(30);
            assert_eq!(
                km.ttls.len(),
                tc.len,
                "length does not match for '{}'",
                tc.name
            );
        }
    }
}

impl Local {
    pub fn ttl(&self) -> u64 {
        self.ttl
    }

    pub fn new(partition_count: u32, ttl: u64, window: u64, sweep: u64) -> Self {
        Self {
            partition_count,
            partitions: {
                let mut v = Vec::with_capacity(partition_count as usize);
                (0..partition_count as usize).for_each(|_| v.push(Mutex::new(KeyMap::new(window))));
                v
            },
            clock: AtomicU64::new(
                time::SystemTime::now()
                    .duration_since(time::UNIX_EPOCH)
                    .expect("can't get duration since UNIX 0 - this is a bug in the code")
                    .as_secs(),
            ),
            ttl,
            sweep,
        }
    }

    pub fn get_or_create(&self, key: &str, create: bool) -> Result<u64, CacheError> {
        let partition = twox_hash::xxh3::hash64(key.as_bytes()) as u32 % self.partition_count;
        let inner = self.partitions.index(partition as usize);

        let mut lock = match inner.lock() {
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
                ts: self.clock.load(Relaxed),
            },
            create,
        );

        Ok(val)
    }

    pub fn start_lru(cache: std::sync::Arc<Self>) {
        // let lru_guard = std::sync::Arc::new(self);
        let sweep = cache.sweep;

        tokio::spawn(async move {
            let ticker =
                ticker::Ticker::new(std::iter::repeat(true), time::Duration::from_secs(sweep));
            for _ in ticker {
                cache.lru();
            }
        });
    }

    pub fn start_clock(cache: std::sync::Arc<Self>) {
        tokio::spawn(async move {
            let ticker = ticker::Ticker::new(std::iter::repeat(true), time::Duration::from_secs(1));
            for _ in ticker {
                cache.clock.store(
                    time::SystemTime::now()
                        .duration_since(time::UNIX_EPOCH)
                        .expect("can't get duration since UNIX 0 - this is a bug in the code")
                        .as_secs(),
                    Relaxed,
                );
            }
        });
    }

    fn lru(&self) {
        for partition in self.partitions.iter() {
            let now = self.clock.load(Relaxed) - self.ttl as u64;
            if let Ok(mut p) = partition.lock() {
                p.lru(now);
            }
        }
    }
}

impl Default for Local {
    fn default() -> Self {
        Self {
            partition_count: DEFAULT_PARTITIONS,
            partitions: {
                let mut v = Vec::with_capacity(DEFAULT_PARTITIONS as usize);
                (0..DEFAULT_PARTITIONS as usize)
                    .for_each(|_| v.push(Mutex::new(KeyMap::default())));
                v
            },
            clock: AtomicU64::new(
                time::SystemTime::now()
                    .duration_since(time::UNIX_EPOCH)
                    .expect("can't get duration since UNIX 0 - this is a bug in the code")
                    .as_secs(),
            ),
            ttl: DEFAULT_TTL,
            sweep: DEFAULT_SWEEP,
        }
    }
}

#[cfg(test)]
mod local_tests {

    extern crate test;

    use super::*;
    use rand::Rng;
    use std::sync::Arc;

    #[test]
    fn test_new_local() {
        let local = Local::new(5, 30, DEFAULT_WINDOW, DEFAULT_SWEEP);
        assert_eq!(local.partition_count, 5);
        assert_eq!(local.ttl, 30);

        let local = Local::default();
        assert_eq!(local.partition_count, DEFAULT_PARTITIONS);
        assert_eq!(local.ttl, DEFAULT_TTL);
    }

    // TODO finish testing LRU + start
    #[tokio::test(flavor = "multi_thread")]
    async fn test_lru() {
        struct TestCase {
            name: &'static str,
            vals: HashMap<&'static str, Vec<u64>>, // vals to insert before ttl
            expected: HashMap<&'static str, Option<u64>>, // expected length
        }

        const TTL: u64 = 30;
        const HARDCODED_TIME: u64 = 60;

        let testcases = vec![
            TestCase {
                name: "some values LRUed out",
                vals: HashMap::from([("foo", vec![10, 15, 35]), ("bar", vec![20, 22, 35])]),
                expected: HashMap::from([("foo", Some(1)), ("bar", Some(1))]),
            },
            TestCase {
                name: "all values LRUed out",
                vals: HashMap::from([("foo", vec![10, 15, 20]), ("bar", vec![20, 25, 27])]),
                expected: HashMap::from([("foo", None), ("bar", None)]),
            },
            TestCase {
                name: "no values LRUed out",
                vals: HashMap::from([("foo", vec![30, 35, 40]), ("bar", vec![40, 45, 50])]),
                expected: HashMap::from([("bar", Some(3)), ("foo", Some(3))]),
            },
        ];

        for tc in testcases {
            let local = Box::leak(Box::new(Local::new(2, TTL, 5, 1)));

            for (k, v) in tc.vals {
                for e in v {
                    local.clock.store(e, Relaxed);
                    local
                        .get_or_create(k, true)
                        .expect(format!("failed to set values for {}", tc.name).as_str());
                }
            }

            local.clock.store(HARDCODED_TIME, Relaxed);
            println!("{:?}", local);
            local.lru();
            println!("{:?}", local);

            for (k, v) in tc.expected {
                let val = local
                    .get_or_create(k, false)
                    .map_err(|e| println!("failure getting val: {}", e))
                    .ok();
                println!("{:?}", val);
                assert_eq!(
                    v.or(Some(0)).unwrap(),
                    val.or(Some(0)).unwrap(),
                    "expected {:?}, got {:?} for key {} for '{}'",
                    v,
                    val,
                    k,
                    tc.name
                );
            }
        }
    }

    #[test]
    fn test_get_or_create() {
        struct TestCase {
            name: &'static str,
            key: &'static str,
            create: bool,
            val: u64,
        }

        let testcases = vec![
            TestCase {
                name: "create foo",
                key: "foo",
                create: true,
                val: 1,
            },
            TestCase {
                name: "update foo",
                key: "foo",
                create: true,
                val: 2,
            },
            TestCase {
                name: "create bar",
                key: "bar",
                create: true,
                val: 1,
            },
            TestCase {
                name: "get foobar",
                key: "foobar",
                create: false,
                val: 0,
            },
        ];

        let mut local = Local::new(10, 30, DEFAULT_WINDOW, DEFAULT_SWEEP);
        for tc in testcases {
            let val = local.get_or_create(tc.key, tc.create);
            let inner = val.unwrap();
            assert_eq!(inner, tc.val, "incorrect value {} for {}", inner, tc.name);
        }
    }

    #[test]
    fn test_get_or_create_concurrent() {
        let local = Arc::new(Local::new(10, 30, DEFAULT_WINDOW, DEFAULT_SWEEP));

        let mut threads = Vec::new();
        for i in 0..9 {
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
            let threads = 2;
            let requests = 100000;
            let local = Local::new(128, 300, DEFAULT_WINDOW, DEFAULT_SWEEP);
            let local_protected = Arc::new(local);
            let mut data = vec![Vec::new(); threads];
            let mut rng = rand::thread_rng();

            for i in &mut data {
                for _ in 0..requests {
                    let val: i32 = rng.gen();
                    i.push(val.to_string());
                }
            }

            let mut data_iter = data.into_iter();
            let mut handles = Vec::new();

            let now = time::SystemTime::now();
            for _i in 0..threads {
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
                handles.push(t);
            }

            for i in handles {
                let _ = i.join();
            }

            let done = time::SystemTime::now();
            println!(
                "done: {}, {} rps",
                done.duration_since(now).unwrap().as_secs_f64(),
                (threads * requests) as f64 / done.duration_since(now).unwrap().as_secs_f64()
            )
        })
    }
}
