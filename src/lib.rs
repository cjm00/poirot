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

const DEFAULT_INITIAL_CAPACITY: usize = 64;
const DEFAULT_SEGMENT_COUNT: usize = 16;

pub type RwLockWriteGuardRefMut<'a, T, U = T> = OwningRefMut<RwLockWriteGuard<'a, T>, U>;
pub type RwLockReadGuardRef<'a, T, U = T> = OwningRef<RwLockReadGuard<'a, T>, U>;

#[derive(Debug)]
pub struct ConcurrentHashMap<K: Eq + Hash, V, B: BuildHasher = RandomState> {
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
    pub fn get<Q: ?Sized>(&self, key: &Q) -> Option<RwLockReadGuardRef<HashMap<K, V, B>, V>>
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
    }

    #[inline]
    pub fn get_mut<Q: ?Sized>(&self, key: &Q) -> Option<RwLockWriteGuardRefMut<HashMap<K, V, B>, V>>
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
    fn hash<Q: ?Sized>(&self, key: &Q) -> u64
    where
        K: Borrow<Q>,
        Q: Eq + Hash,
    {
        let mut hasher = self.hash_builder.build_hasher();
        key.hash(&mut hasher);
        hasher.finish()
    }

    #[inline]
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

impl<K, V, B> IntoIterator for ConcurrentHashMap<K, V, B>
where
    K: Eq + Hash,
    B: BuildHasher,
{
    type Item = (K, V);
    type IntoIter = ConcurrentHashMapIter<K, V, B>;
    fn into_iter(self) -> Self::IntoIter {
        let seg: fn(_) -> _ = |segment: RwLock<HashMap<K, V, B>>| segment.into_inner();
        let inner = self.segments.into_iter().flat_map(seg);
        ConcurrentHashMapIter { inner }
    }
}

pub struct ConcurrentHashMapIter<K, V, B>
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

impl<K, V, B> Iterator for ConcurrentHashMapIter<K, V, B>
where
    K: Eq + Hash,
    B: BuildHasher,
{
    type Item = (K, V);
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

#[derive(Debug)]
pub struct ConcurrentHashSet<K: Eq + Hash, B: BuildHasher = RandomState> {
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
    type IntoIter = ConcurrentHashSetIter<K, B>;
    fn into_iter(self) -> ConcurrentHashSetIter<K, B> {
        let inner = self.table.into_iter();
        ConcurrentHashSetIter{inner}
    }
}

pub struct ConcurrentHashSetIter<K, B> where K: Eq + Hash, B: BuildHasher {
    inner: ConcurrentHashMapIter<K, (), B>,
}

impl<K: Eq + Hash, B: BuildHasher> Iterator for ConcurrentHashSetIter<K, B> {
    type Item = K;
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|(k, _)| k)
    }
}