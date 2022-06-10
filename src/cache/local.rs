extern crate test;

use std::collections::{BTreeMap, HashMap};
use std::ops::Index;
use std::time;
use std::sync::{Mutex, Arc};

const PARTITIONS: u32 = 1024;
const DEFAULT_WINDOW: u64 = 60;

pub struct Local {
    partition_count: u32,
    partitions: Vec<Arc<Mutex<KeyMap>>>,
    clock: fn() -> u64,
}

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
    msg: String
}

impl std::fmt::Display for CacheError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "error internal to local cache: {}", self.msg)
    }
}

impl TTLValues {
    
  pub fn get(&self, val: u64) -> u64 {
        let v = self.vals.get(&val).unwrap_or(&0);
        *v
    }

   pub fn inc(&mut self, val: u64) -> u64 {
        let insert = match self.vals.iter().next_back() {
            Some(n) => {
                let (bucket, new_val) = if val.abs_diff(*n.0) < self.window {
                    n
                } else {
                    (&val, &0)
                };

                (*bucket, *new_val + 1)
            },
            None => {
               (val, 1)
            },
        };

        self.vals.insert(insert.0, insert.1);
        insert.0
    }

    pub fn inc_and_get(&mut self, val: u64) -> u64 {
        let new_key = self.inc(val);
        self.get(new_key)
    }

  pub  fn new(window: u64) -> Self {
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
        assert_eq!(val.get(5005), 1 );
    }
}

impl KeyMap {
    pub fn new() -> KeyMap {
        KeyMap::default()
    }

    pub fn get_or_create(&mut self, key: Key) -> u64 {
        match self.ttls.get_mut(key.k) {
            Some(val) => {
                let state = val.inc_and_get(key.ts);
                state
            },
            None => {
                let mut val = TTLValues::default();
                let state = val.inc_and_get(key.ts);
                self.ttls.insert(key.k.to_string(), val);
                // println!("{} {}", key.k, state);

                state
            },
        }
    }
}

impl Default for KeyMap {
    fn default() -> Self {
        Self { 
            ttls: Default::default() 
        }
    }
}

#[cfg(test)]
mod keymap_tests {

    use super::*;

    #[test]
    fn test_get_or_create() {
        let mut km = KeyMap::new();

        km.get_or_create(Key{
            k: "foo",
            ts: 10000,
        });
        
        let foo_val = km.get_or_create(Key{
            k: "foo",
            ts: 10005,
        });

        let bar_val  = km.get_or_create(Key{
            k: "bar",
            ts: 10100,
        });

        assert_eq!(km.get_or_create(Key{
            k: "bar",
            ts: 10101,
        }), bar_val + 1);

        assert_eq!(km.get_or_create(Key{
            k: "foo",
            ts: 10050,
        }), foo_val + 1);

        assert_eq!(km.get_or_create(Key{
            k: "foo",
            ts: 10150,
        }), 1);

        assert_eq!(km.get_or_create(Key{
            k: "foobar",
            ts: 10150,
        }), 1);

    }
}

impl Local {
    fn default_clock() -> u64 {
        time::SystemTime::now().duration_since(time::UNIX_EPOCH)
                .expect("can't get duration since UNIX 0 - this is a bug in the code").as_secs()
    }

    fn new(partitions: u32) -> Self {
        Self { 
            partition_count: partitions, 
            partitions: vec![Arc::new(Mutex::new(KeyMap::default())); partitions as usize],
            clock: Local::default_clock,
        }
    }

    fn get_or_create(&mut self, key: &str) -> Result<u64, CacheError> {
        let partition = twox_hash::xxh3::hash64(key.as_bytes()) as u32 % self.partition_count;
        let inner = self.partitions.index(partition as usize);
        let inner_clone = inner.clone();

        let mut lock = match inner_clone.lock() {
            Ok(l) => l,
            Err(e) => return Err(CacheError{
               
                msg: format!("failed to get partitions lock: {}", e.to_string())
            }),
        };

        let val = lock.get_or_create(Key{
            k: key,
            ts: (self.clock)(),
        });
        
        Ok(val)
    }
    
}

impl Default for Local {
    fn default() -> Self {
        Self { 
            partition_count: PARTITIONS, 
            partitions: vec![Arc::new(Mutex::new(KeyMap::default())); PARTITIONS as usize],
            clock: Local::default_clock,
        }
    }
}

#[cfg(test)]
mod local_tests {
    
    extern crate test;

    use super::*;
    use rand::Rng;

    #[test]
    fn test_new_local() {
        let local = Local::new(5);
        assert_eq!(local.partition_count, 5);
        
        let local = Local::default();
        assert_eq!(local.partition_count, PARTITIONS);
    }

    #[test]
    fn test_get_or_create() {
        let mut local = Local::new(10);
        let val = local.get_or_create("foo");
        let inner = val.unwrap();
        assert_eq!(inner, 1);

        let val = local.get_or_create("foo");
        let inner = val.unwrap();
        assert_eq!(inner, 2);

        let val= local.get_or_create("bar");
        let inner = val.unwrap();
        assert_eq!(inner, 1);
    }

    #[test]
    fn test_get_or_create_concurrent() {
        let local = Local::new(10);
        let local_protected = Arc::new(Mutex::new(local));
        

        let mut threads = Vec::new();
        for i in 0..9 {
            println!("thread_num {}", i);
            let lp = local_protected.clone();
            let t = std::thread::spawn(move || {
                    let mut l = lp.lock().expect("unable to get Local lock");
                    if let Err(e) = l.get_or_create("foo") {
                        panic!("failed to get get_or_create: {}", e.to_string()); 
                    }
            });
            threads.push(t);
        }

        for i in threads {
            let _ = i.join();
        }

        let val = local_protected.lock().unwrap()
        .get_or_create("foo")
        .expect("failed to get Local lock");

        assert_eq!(val, 10);
    }

    #[bench]
    fn bench_get_or_create_concurrent(b: &mut test::Bencher) {
        b.iter(|| {
            let local = Local::new(128);
            let local_protected = Arc::new(Mutex::new(local));
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
                            let mut l = lp.lock().expect("unable to get Local lock");
                            if let Err(e) = l.get_or_create(&key) {
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