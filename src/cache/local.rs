use super::CacheError;
use std::collections::{BTreeMap, HashMap};
use std::ops::Index;
use std::sync::{atomic::AtomicU64, atomic::Ordering::Relaxed, Arc, Mutex};
use std::time;
use tokio;

pub const DEFAULT_PARTITIONS: u32 = 1024;
pub const DEFAULT_TTL: u64 = 300;
pub const DEFAULT_SWEEP: u64 = 60;

#[derive(Debug)]
pub struct Local {
    partition_count: u32,
    ttl: u64,
    sweep: u64,
    /*  we want to use a Mutex for better overall performance on OS X - this is due to
        platform-specific differences in how pthread_rwlock works, which is used internally
        (see https://stdrs.dev/nightly/x86_64-apple-darwin/std/sys/unix/locks/pthread_rwlock/struct.AllocatedRwLock.html).
        This seems to be due to OS X preferring writers to readers, as seen at
        https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man3/pthread_rwlock_rdlock.3.html.
        Linux and other *nix platforms work better with a pthread_rwlock.
    */
    #[cfg(target_os = "macos")]
    partitions: Vec<Mutex<KeyMap>>,
    #[cfg(not(target_os = "macos"))]
    partitions: Vec<RwLock<KeyMap>>,
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

    // very naive solution currently
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
            window: DEFAULT_SWEEP,
            vals: Default::default(),
        }
    }
}

#[cfg(test)]
mod ttlvalues_tests {

    use super::*;

    #[test]
    fn test_new_ttlvalue() {
        let val = TTLValues::default();
        assert_eq!(val.window, DEFAULT_SWEEP);

        let val = TTLValues::new(100);
        assert_eq!(val.window, 100);
    }

    #[test]
    fn test_get_inner() {
        let mut val = TTLValues::default();
        val.inc(1000);
        val.inc(1010);
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

    // testcase-based rather than macro-based to simplify state across cases
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

    // testcase-based rather than macro-based to simplify maintaining state across cases
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

    macro_rules! ttl_values_lru_tests {
        ($($name:ident: $value:expr,)*) => {
            $(
                #[test]
                fn $name() {
                    let (vals, ttl, len) = $value;

                    let mut ttl_val = TTLValues::new(5);
                    for v in vals {
                        ttl_val.inc(v);
                    }

                    ttl_val.lru(ttl);
                    assert_eq!(ttl_val.vals.len(), len);
                }
            )*
        }
    }

    ttl_values_lru_tests! {
        ttl_values_lru_empty: (vec![], 50, 0),
        ttl_values_delete_2_keep_1: (vec![10, 20, 50], 30, 1),
        ttl_values_delete_none: (vec![40, 50, 60], 30, 3),
        ttl_values_delete_all: (vec![10, 20, 25], 30, 0),
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

    // testcase-based rather than macro-based to simplify state across cases
    #[test]
    fn test_get_or_create() {
        struct TestCase {
            name: &'static str,
            key: Key<'static>,
            create: bool,
            val: u64,
        }

        let mut km = KeyMap::new(60);

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

    macro_rules! keymap_lru_tests {
        ($($name:ident: $value:expr,)*) => {
            $(
                #[test]
                fn $name() {
                    let (vals, len) = $value;
                    let mut km = KeyMap::new(30);

                    for (k, v) in vals {
                        for vv in v {
                            km.get_or_create(Key { k, ts: vv }, true);
                        }
                    }

                    km.lru(30);
                    assert_eq!(
                        km.ttls.len(),
                        len,
                        "length does not match",
                    );
                }
            )*
        }
    }

    keymap_lru_tests! {
        keymap_lru_delete_1_keep_2: (
            HashMap::from([
                ("foo", vec![10, 20, 25]),
                ("bar", vec![10, 20, 50]),
                ("foobar", vec![40, 50, 60]),
            ]),
            2,
        ),
        keymap_lru_delete_all: (
            HashMap::from([
                ("foo", vec![10, 20, 25]),
                ("bar", vec![10, 20, 22]),
                ("foobar", vec![5, 1, 28]),
            ]),
            0,
        ),
        keymap_lru_delete_none: (
            HashMap::from([
                ("foo", vec![40, 50, 55]),
                ("bar", vec![32, 37, 50]),
                ("foobar", vec![40, 50, 60]),
            ]),
            3,
        ),
    }
}

impl Local {
    pub fn ttl(&self) -> u64 {
        self.ttl
    }

    #[cfg(target_os = "macos")]
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

    #[cfg(not(target_os = "macos"))]
    pub fn new(partition_count: u32, ttl: u64, window: u64, sweep: u64) -> Self {
        Self {
            partition_count,
            partitions: {
                let mut v = Vec::with_capacity(partition_count as usize);
                (0..partition_count as usize)
                    .for_each(|_| v.push(RwLock::new(KeyMap::new(window))));
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

    #[cfg(target_os = "macos")]
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

    #[cfg(not(target_os = "macos"))]
    pub fn get_or_create(&self, key: &str, create: bool) -> Result<u64, CacheError> {
        let partition = twox_hash::xxh3::hash64(key.as_bytes()) as u32 % self.partition_count;
        let inner = self.partitions.index(partition as usize);

        let mut lock = match create {
            true => match inner.write() {
                Ok(l) => l,
                Err(e) => {
                    return Err(CacheError {
                        msg: format!("failed to get partition write lock: {}", e),
                    })
                }
            },
            false => match inner.read() {
                Ok(l) => l,
                Err(e) => {
                    return Err(CacheError {
                        msg: format!("failed to get partition read lock: {}", e),
                    })
                }
            },
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

    pub fn start_lru(self: &Arc<Local>) {
        let clone = self.clone();

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(std::time::Duration::from_secs(clone.sweep));
            loop {
                ticker.tick().await;
                clone.lru();
            }
        });
    }

    pub fn start_clock(self: &Arc<Local>) {
        let clone = self.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(std::time::Duration::from_secs(1));
            loop {
                ticker.tick().await;
                clone.clock.store(
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
    #[cfg(target_os = "macos")]
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

    #[cfg(not(target_os = "macos"))]
    fn default() -> Self {
        Self {
            partition_count: DEFAULT_PARTITIONS,
            partitions: {
                let mut v = Vec::with_capacity(DEFAULT_PARTITIONS as usize);
                (0..DEFAULT_PARTITIONS as usize)
                    .for_each(|_| v.push(RwLock::new(KeyMap::default())));
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

    // extern crate test;

    use super::*;
    use rand::Rng;
    use std::sync::Arc;

    #[test]
    fn test_new_local() {
        let local = Local::new(5, 30, DEFAULT_SWEEP, DEFAULT_SWEEP);
        assert_eq!(local.partition_count, 5);
        assert_eq!(local.ttl, 30);

        let local = Local::default();
        assert_eq!(local.partition_count, DEFAULT_PARTITIONS);
        assert_eq!(local.ttl, DEFAULT_TTL);
    }

    #[tokio::test]
    async fn test_start_clock() {
        let local = std::sync::Arc::new(Local::new(2, 30, 5, 1));
        Local::start_clock(&local);
        let mut running_time = local.clock.load(Relaxed);

        for _ in 0..5 {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            let curr = local.clock.load(Relaxed);
            // tokio::time::sleep should only ever sleep longer than 1 second, as the executor will put it back to sleep if the
            // Instant we are waiting for hasn't elapsed. As such, for some starting time edge cases that can lead to us going
            // from, say, time 19 to time 21 for a 1 second sleep, so we allow it to be 1 or 2 ahead of start time
            assert!(
                curr == running_time || curr == running_time + 1,
                "expected {}, got {}",
                running_time,
                curr
            );
            running_time += 1;
        }
    }

    // test_start_lru combines coverage for lru and start_lru
    #[tokio::test]
    async fn test_start_lru() {
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
            let local = std::sync::Arc::new(Local::new(2, TTL, 5, 1));

            for (k, v) in tc.vals {
                for e in v {
                    local.clock.store(e, Relaxed);
                    local
                        .get_or_create(k, true)
                        .expect(format!("failed to set values for {}", tc.name).as_str());
                }
            }

            local.clock.store(HARDCODED_TIME, Relaxed);
            local.start_lru();
            tokio::time::sleep(std::time::Duration::from_millis(1100)).await;

            for (k, v) in tc.expected {
                let val = local
                    .get_or_create(k, false)
                    .ok();
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

        let local = Local::new(10, 30, DEFAULT_SWEEP, DEFAULT_SWEEP);
        for tc in testcases {
            let val = local.get_or_create(tc.key, tc.create);
            let inner = val.unwrap();
            assert_eq!(inner, tc.val, "incorrect value {} for {}", inner, tc.name);
        }
    }

    #[test]
    fn test_get_or_create_concurrent() {
        let local = Arc::new(Local::new(10, 30, DEFAULT_SWEEP, DEFAULT_SWEEP));

        let mut threads = Vec::new();
        for _ in 0..10 {
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
            .get_or_create("foo", false)
            .expect("failed to get Local lock");

        assert_eq!(val, 10);
    }
}
