extern crate poirot;
#[macro_use]
extern crate quickcheck;

use poirot::ConcurrentHashMap;

quickcheck! {
    fn qc_hashmap_insert(xs: Vec<u64>) -> bool {
        let poirot_map = ConcurrentHashMap::new();
        xs.into_iter().enumerate().all(|(k, v)| poirot_map.insert(k, v).is_none())
    }

    fn qc_hashmap_contains(xs: Vec<u64>) -> bool {
        let poirot_map = ConcurrentHashMap::new();
        xs.iter().cloned().for_each(|k| {poirot_map.insert(k, ());});
        xs.into_iter().all(|k| poirot_map.contains(&k))
    }

    fn qc_hashmap_remove(xs: Vec<u64>) -> bool {
        let poirot_map = ConcurrentHashMap::new();
        xs.iter().cloned().for_each(|k| {poirot_map.insert(k, k);});
        xs.iter().cloned().for_each(|k| {poirot_map.remove(&k);});
        xs.into_iter().all(|k| !poirot_map.contains(&k))
    }
}

#[test]
fn hashmap_mutate() {
    let poirot_map = ConcurrentHashMap::new();

    for x in 0..8 {
        poirot_map.insert(x, 0);
    }

    for x in 0..8*1024 {
        let mut entry = poirot_map.get_mut(&(x % 8)).unwrap();
        *entry += 1;
    }

    for x in 0..8 {
        assert_eq!(*poirot_map.get(&x).unwrap(), 1024);
    }
}