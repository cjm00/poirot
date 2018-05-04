extern crate owning_ref;
extern crate parking_lot;

use owning_ref::{OwningRef, OwningRefMut};
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use std::borrow::Borrow;
use std::collections::hash_map::{HashMap, RandomState};
use std::default::Default;
use std::hash::{BuildHasher, Hash, Hasher};
use std::iter::FlatMap;
use std::vec;
use std::ops::{Deref, DerefMut};
use std::cmp::{Eq, PartialEq};
use std::fmt;

const DEFAULT_INITIAL_CAPACITY: usize = 64;
const DEFAULT_SEGMENT_COUNT: usize = 16;

pub struct ConcurrentHashMap<K, V, B = RandomState> {
    segments: Vec<RwLock<HashMap<K, V, B>>>,
    hash_builder: B,
}

impl<K: Eq + Hash, V> ConcurrentHashMap<K, V, RandomState> {
    pub fn new() -> Self {
        Self::default()
    }
}

impl<K: Eq + Hash, V, B: BuildHasher + Default> ConcurrentHashMap<K, V, B> {
    #[inline]
    pub fn insert(&self, key: K, value: V) -> Option<V> {
        let hash = self.hash(&key);
        let segment_index = self.get_segment(hash);
        self.segments[segment_index]
            .write()
            .insert(key, value)
    }

    #[inline]
    pub fn contains<Q: ?Sized>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Eq + Hash,
    {
        let hash = self.hash(key);
        let segment_index = self.get_segment(hash);
        self.segments[segment_index]
            .read()
            .contains_key(key)
    }

    #[inline]
    pub fn remove<Q: ?Sized>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Eq + Hash,
    {
        let hash = self.hash(key);
        let segment_index = self.get_segment(hash);
        self.segments[segment_index].write().remove(key)
    }

    #[inline]
    pub fn get<Q: ?Sized>(&self, key: &Q) -> Option<ReadGuard<K, V, B>>
    where
        K: Borrow<Q>,
        Q: Eq + Hash,
    {
        let hash = self.hash(key);
        let segment_index = self.get_segment(hash);
        let read_lock = self.segments[segment_index].read();
        let owning_ref = OwningRef::new(read_lock);
        owning_ref
            .try_map(|segment| segment.get(key).ok_or(()))
            .ok()
            .map(|inner| ReadGuard{inner})
    }

    #[inline]
    pub fn get_mut<Q: ?Sized>(&self, key: &Q) -> Option<WriteGuard<K, V, B>>
    where
        K: Borrow<Q>,
        Q: Eq + Hash,
    {
        let hash = self.hash(key);
        let segment_index = self.get_segment(hash);
        let write_lock = self.segments[segment_index].write();
        let owning_ref = OwningRefMut::new(write_lock);
        owning_ref
            .try_map_mut(|segment| segment.get_mut(key).ok_or(()))
            .ok()
            .map(|inner| WriteGuard{inner})
    }

    pub fn with_options(capacity: usize, hash_builder: B, concurrency_level: usize) -> Self {
        let concurrency_level = concurrency_level.next_power_of_two();
        let per_segment_capacity = (capacity / concurrency_level).next_power_of_two();
        let mut segments = Vec::with_capacity(concurrency_level);
        for _ in 0..concurrency_level {
            segments.push(RwLock::new(HashMap::with_capacity_and_hasher(
                per_segment_capacity,
                <B as Default>::default(),
            )))
        }
        ConcurrentHashMap {
            hash_builder,
            segments,
        }
    }

    #[inline]
    pub fn insert_or_update<F, G>(&self, key: K, insert: F, update: G) where F: FnOnce() -> V, G: FnOnce(&mut V) {
        let hash = self.hash(&key);
        let segment_index = self.get_segment(hash);
        let mut segment_lock = self.segments[segment_index].write();
        segment_lock.entry(key)
            .and_modify(update)
            .or_insert_with(insert);
    }

    #[inline]
    fn hash<Q: ?Sized>(&self, key: &Q) -> u64
    where
        K: Borrow<Q>,
        Q: Eq + Hash,
    {
        let mut hasher = self.hash_builder.build_hasher();
        key.hash(&mut hasher);
        hasher.finish()
    }

    #[inline(always)]
    fn get_segment(&self, hash: u64) -> usize {
        let shift_size =
            (std::mem::size_of::<usize>() * 8) - self.segments.len().trailing_zeros() as usize;
        (hash as usize >> shift_size) & (self.segments.len() - 1)
    }
}

impl<K: Eq + Hash, V, B: BuildHasher + Default> Default for ConcurrentHashMap<K, V, B> {
    fn default() -> Self {
        ConcurrentHashMap::with_options(
            DEFAULT_INITIAL_CAPACITY,
            Default::default(),
            DEFAULT_SEGMENT_COUNT,
        )
    }
}

pub struct ReadGuard<'a, K: 'a, V: 'a, B: 'a> {
    inner: OwningRef<RwLockReadGuard<'a, HashMap<K, V, B>>, V>,
}

impl<'a, K: 'a, V: 'a, B: 'a> Deref for ReadGuard<'a, K, V, B> {
    type Target = V;
    fn deref(&self) -> &V {
        &self.inner
    }
}

impl<'a, K: 'a, V: PartialEq + 'a, B: 'a> PartialEq for ReadGuard<'a, K, V, B> {
    fn eq(&self, other: &Self) -> bool {
        V::eq(self, other)
    }
}

impl<'a, K: 'a, V: Eq + 'a, B: 'a> Eq for ReadGuard<'a, K, V, B> {}


pub struct WriteGuard<'a, K: 'a, V: 'a, B: 'a> {
    inner: OwningRefMut<RwLockWriteGuard<'a, HashMap<K, V, B>>, V>,
}

impl<'a, K: 'a, V: 'a, B: 'a> Deref for WriteGuard<'a, K, V, B> {
    type Target = V;
    fn deref(&self) -> &V {
        &self.inner
    }
}

impl<'a, K: 'a, V: 'a, B: 'a> DerefMut for WriteGuard<'a, K, V, B> {
    fn deref_mut(&mut self) -> &mut V {
        &mut self.inner
    }
}

impl<'a, K: 'a, V: PartialEq + 'a, B: 'a> PartialEq for WriteGuard<'a, K, V, B> {
    fn eq(&self, other: &Self) -> bool {
        V::eq(self, other)
    }
}

impl<'a, K: 'a, V: Eq + 'a, B: 'a> Eq for WriteGuard<'a, K, V, B> {}


impl<K, V, B> IntoIterator for ConcurrentHashMap<K, V, B>
where
    K: Eq + Hash,
    B: BuildHasher,
{
    type Item = (K, V);
    type IntoIter = ConcurrentHashMapIntoIter<K, V, B>;
    fn into_iter(self) -> Self::IntoIter {
        let seg: fn(_) -> _ = |segment: RwLock<HashMap<K, V, B>>| segment.into_inner();
        let inner = self.segments.into_iter().flat_map(seg);
        ConcurrentHashMapIntoIter { inner }
    }
}

pub struct ConcurrentHashMapIntoIter<K, V, B>
where
    K: Eq + Hash,
    B: BuildHasher,
{
    inner: FlatMap<
        vec::IntoIter<RwLock<HashMap<K, V, B>>>,
        HashMap<K, V, B>,
        fn(RwLock<HashMap<K, V, B>>) -> HashMap<K, V, B>,
    >,
}

impl<K, V, B> Iterator for ConcurrentHashMapIntoIter<K, V, B>
where
    K: Eq + Hash,
    B: BuildHasher,
{
    type Item = (K, V);
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

pub struct ConcurrentHashSet<K, B = RandomState> {
    table: ConcurrentHashMap<K, (), B>,
}

impl<K: Eq + Hash> ConcurrentHashSet<K, RandomState> {
    pub fn new() -> Self {
        ConcurrentHashSet {
            table: ConcurrentHashMap::new(),
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        ConcurrentHashSet {
            table: ConcurrentHashMap::with_options(
                capacity,
                Default::default(),
                DEFAULT_SEGMENT_COUNT,
            ),
        }
    }

    pub fn with_capacity_and_concurrency_level(capacity: usize, concurrency_level: usize) -> Self {
        ConcurrentHashSet {
            table: ConcurrentHashMap::with_options(capacity, Default::default(), concurrency_level),
        }
    }
}

impl<K: Eq + Hash, B: BuildHasher + Default> ConcurrentHashSet<K, B> {
    #[inline]
    pub fn insert(&self, key: K) -> bool {
        self.table.insert(key, ()).is_none()
    }

    #[inline]
    pub fn contains<Q: ?Sized>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Eq + Hash,
    {
        self.table.contains(key)
    }

    #[inline]
    pub fn remove<Q: ?Sized>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Eq + Hash,
    {
        self.table.remove(key).is_some()
    }

    pub fn with_capacity_and_hasher(capacity: usize, hash_builder: B) -> Self {
        ConcurrentHashSet {
            table: ConcurrentHashMap::with_options(capacity, hash_builder, DEFAULT_SEGMENT_COUNT),
        }
    }

    pub fn with_options(capacity: usize, hash_builder: B, concurrency_level: usize) -> Self {
        ConcurrentHashSet {
            table: ConcurrentHashMap::with_options(capacity, hash_builder, concurrency_level),
        }
    }
}

impl<K: Eq + Hash, B: BuildHasher + Default> Default for ConcurrentHashSet<K, B> {
    fn default() -> Self {
        ConcurrentHashSet {
            table: ConcurrentHashMap::default(),
        }
    }
}

impl<K: Eq + Hash, B: BuildHasher> IntoIterator for ConcurrentHashSet<K, B> {
    type Item = K;
    type IntoIter = ConcurrentHashSetIntoIter<K, B>;
    fn into_iter(self) -> ConcurrentHashSetIntoIter<K, B> {
        let inner = self.table.into_iter();
        ConcurrentHashSetIntoIter{inner}
    }
}

pub struct ConcurrentHashSetIntoIter<K, B> where K: Eq + Hash, B: BuildHasher {
    inner: ConcurrentHashMapIntoIter<K, (), B>,
}

impl<K: Eq + Hash, B: BuildHasher> Iterator for ConcurrentHashSetIntoIter<K, B> {
    type Item = K;
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|(k, _)| k)
    }
}