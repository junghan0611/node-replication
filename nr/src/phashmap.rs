use core::default::Default;
use std::borrow::Borrow;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::mem;

pub use crate::pvec::PVec;

/* #[macro_use]
extern crate lazy_static;
lazy_static! {
    pub static ref PMPOOL1: pmdk::ObjPool = {
        let path = String::from("/mnt/pmem0/basic-hash-map-pool-1.pool");
        //let path = String::from("/dev/shm/pmem0/test.pool");
        let mut pool = pmdk::ObjPool::new::<_, String>(path, None, 0x1000, 0x2000_0000 / 0x1000).unwrap();
        pool.set_rm_on_drop(true);
        pool
    };   
}
 */
//const INITIAL_NBUCKETS: usize = 1;
const INITIAL_NBUCKETS: usize = 1024;

#[derive(Debug, Clone)]
pub struct PHashMap<K, V> {
    buckets: PVec<PVec<(K, V)>>,
    items: usize,
}

impl<K, V> Default for PHashMap<K, V> {
    fn default() -> PHashMap<K, V> {        
        PHashMap {
            buckets: PVec::new(),
            items: 0,
        }
    }
}

impl<K, V> PHashMap<K, V> {
    pub fn new() -> Self {
        PHashMap {
            buckets: PVec::new(),
            items: 0,
        }
    }

    pub fn with_capacity(capacity : usize) -> Self {
        //println!{"PHashMap::with_capacity capacity : {}", capacity};
        
        let hashmap = PHashMap {
            buckets: PVec::with_capacity(capacity),
            items: 0,
        };

        //println!{"PHashMap::with_capacity, buckets.capacity {}, buckets.len {}", hashmap.buckets.capacity(), hashmap.buckets.len() };

        hashmap
    }
}

impl<K, V> PHashMap<K, V>
where
    K: Hash + Eq,
{
    fn bucket<Q>(&self, key: &Q) -> usize
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let hashindex = (hasher.finish() % self.buckets.len() as u64) as usize;
        //println!("::bucket, self.buckets.len() {}, hash index {}", self.buckets.len(), hashindex);
        hashindex
    }

    pub fn insert(&mut self, key: K, value: V) -> Option<V> {

        //println!("item {}, buckets.len {}", self.items, self.buckets.len());

        if self.buckets.is_empty() || self.items > 3 * self.buckets.len() / 4 {
            self.resize();
        }

        let bucket = self.bucket(&key);
        let bucket = &mut self.buckets[bucket];

        self.items += 1;
        for &mut (ref ekey, ref mut evalue) in bucket.iter_mut() {
            if ekey == &key {
                return Some(mem::replace(evalue, value));
            }
        }

        bucket.push((key, value));
        None
    }

    pub fn get<Q>(&self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let bucket = self.bucket(key);
        self.buckets[bucket]
            .iter()
            .find(|&(ref ekey, _)| ekey.borrow() == key)
            .map(|&(_, ref v)| v)
    }

    pub fn contains_key<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.get(key).is_some()
    }

    pub fn remove<Q>(&mut self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let bucket = self.bucket(key);
        let bucket = &mut self.buckets[bucket];
        let i = bucket
            .iter()
            .position(|&(ref ekey, _)| ekey.borrow() == key)?;
        self.items -= 1;
        Some(bucket.swap_remove(i).1)
    }

    pub fn len(&self) -> usize {
        self.items
    }

    pub fn is_empty(&self) -> bool {
        self.items == 0
    }

    fn resize(&mut self) {
        let target_size = match self.buckets.len() {
            0 => INITIAL_NBUCKETS,
            n => 2 * n,
        };

        //println!("::resize taget_size {}", target_size);

        let mut new_buckets: PVec<PVec<(K,V)>> = PVec::with_capacity(target_size);
        //new_buckets.extend((0..target_size).map(|_| PVec::new()));
        for i in 0..target_size {
            new_buckets.insert(i,PVec::new()); 
            //new_buckets.push(PVec::new()); 
        }

        //for (key, value) in self.buckets.iter_mut().flat_map(|bucket| bucket.drain(..)) {
        for (key, value) in self.buckets.iter_mut().flat_map(|bucket| bucket.drain()) {
            let mut hasher = DefaultHasher::new();
            key.hash(&mut hasher);
            let bucket = (hasher.finish() % new_buckets.len() as u64) as usize;
            new_buckets[bucket].push((key, value));
        }

        mem::replace(&mut self.buckets, new_buckets);
    }
}

pub struct Iter<'a, K: 'a, V: 'a> {
    map: &'a PHashMap<K, V>,
    bucket: usize,
    at: usize,
}

impl<'a, K, V> Iterator for Iter<'a, K, V> {
    type Item = (&'a K, &'a V);
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.map.buckets.get(self.bucket) {
                Some(bucket) => {
                    match bucket.get(self.at) {
                        Some(&(ref k, ref v)) => {
                            // move along self.at and self.bucket
                            self.at += 1;
                            break Some((k, v));
                        }
                        None => {
                            self.bucket += 1;
                            self.at = 0;
                            continue;
                        }
                    }
                }
                None => break None,
            }
        }
    }
}

impl<'a, K, V> IntoIterator for &'a PHashMap<K, V> {
    type Item = (&'a K, &'a V);
    type IntoIter = Iter<'a, K, V>;
    fn into_iter(self) -> Self::IntoIter {
        Iter {
            map: self,
            bucket: 0,
            at: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert() {
        //let mut map = PHashMap::new();
        let mut map = PHashMap::with_capacity(2);
        assert_eq!(map.len(), 0);
        assert!(map.is_empty());
        map.insert("foo", 42);
        assert_eq!(map.len(), 1);
        assert!(!map.is_empty());
        assert_eq!(map.get(&"foo"), Some(&42));
        assert_eq!(map.remove(&"foo"), Some(42));
        assert_eq!(map.len(), 0);
        assert!(map.is_empty());
        assert_eq!(map.get(&"foo"), None);
    }

    #[test]
    fn iter() {
        //let mut map = PHashMap::new();
        let mut map = PHashMap::with_capacity(2);
        map.insert("foo", 42);
        map.insert("bar", 43);
        map.insert("baz", 142);
        map.insert("quox", 7);
        for (&k, &v) in &map {
            match k {
                "foo" => assert_eq!(v, 42),
                "bar" => assert_eq!(v, 43),
                "baz" => assert_eq!(v, 142),
                "quox" => assert_eq!(v, 7),
                _ => unreachable!(),
            }
        }
        
        println!("----");
        assert_eq!(map.get(&"foo"), Some(&42));

        assert_eq!((&map).into_iter().count(), 4);
    }
}
