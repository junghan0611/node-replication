use core::panic;
use std::borrow::Borrow;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::mem;

const INITIAL_NBUCKETS: usize = 1;

use super::pvec::PVec;

unsafe impl Send for PHashMap<u64, u64> {}
unsafe impl Sync for PHashMap<u64, u64> {}

#[derive(Debug, Clone)]
pub struct PHashMap<K, V> {
    buckets: PVec<PVec<(K, V)>>,    
    items: usize,
}

impl<K, V> PHashMap<K, V> {
    pub fn new() -> Self {
        PHashMap {
            //buckets: Vec::new(),
            buckets: PVec::default(),
            items: 0,
        }
    }

    pub fn with_capacity(capacity : usize) -> Self {
        println!{"PHashMap::with_capacity capacity : {}", capacity};
        
        let hashmap = PHashMap {
            //buckets: PVec::new(),            
            //buckets: PVec::with_capacity(capacity),
            buckets: PVec::with_capacity(capacity),
            items: 0,
        };

        println!{"PHashMap::with_capacity, buckets.capacity {}", hashmap.buckets.capacity() };

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
        //(hasher.finish() % self.buckets.len() as u64) as usize
        (hasher.finish() % self.buckets.capacity() as u64) as usize
    }

    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        /*if self.buckets.is_empty() || self.items > 3 * self.buckets.len() / 4 {
            self.resize();
        }*/

        let bucket = self.bucket(&key);

        //println!("PHashMap::insert bucket.capacity {}, bucket index: {}", self.buckets.capacity(), bucket);

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

/*     pub fn remove<Q>(&mut self, key: &Q) -> Option<V>
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
    } */

    pub fn len(&self) -> usize {
        self.items
    }

    pub fn is_empty(&self) -> bool {
        self.items == 0
    }

    fn resize(&mut self) {
        
        println!("[PHASHMAP] self.buckets.len(): {}", self.buckets.len());

        let target_size = match self.buckets.len() {
            0 => INITIAL_NBUCKETS,
            n => 2 * n,
        };

        panic!();

    /*     let mut new_buckets = PVec::with_capacity(target_size);
        new_buckets.extend((0..target_size).map(|_| PVec::new()));

        for (key, value) in self.buckets.iter_mut().flat_map(|bucket| bucket.drain(..)) {
            let mut hasher = DefaultHasher::new();
            key.hash(&mut hasher);
            let bucket = (hasher.finish() % new_buckets.len() as u64) as usize;
            new_buckets[bucket].push((key, value));
        }

        mem::replace(&mut self.buckets, new_buckets); */
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
        let mut map = PHashMap::new();
        assert_eq!(map.len(), 0);
        assert!(map.is_empty());
        map.insert("foo", 42);
        assert_eq!(map.len(), 1);
        assert!(!map.is_empty());
        assert_eq!(map.get(&"foo"), Some(&42));
        //assert_eq!(map.remove(&"foo"), Some(42));
        assert_eq!(map.len(), 0);
        assert!(map.is_empty());
        assert_eq!(map.get(&"foo"), None);
    }

    #[test]
    fn iter() {
        let mut map = PHashMap::new();
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
        assert_eq!((&map).into_iter().count(), 4);
    }
}
