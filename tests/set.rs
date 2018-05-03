extern crate poirot;
#[macro_use]
extern crate quickcheck;

use poirot::ConcurrentHashSet;
use std::collections::HashSet;

quickcheck! {
    fn qc_poirot_single_thread_set_insert(xs: Vec<u64>) -> bool {
        let poirot_set = ConcurrentHashSet::new();
        let mut std_set = HashSet::new();
        xs.iter().cloned().for_each(|k| {poirot_set.insert(k); std_set.insert(k);});
        std_set.iter().all(|k| poirot_set.contains(k)) && poirot_set.into_iter().all(|k| std_set.contains(&k))
    }
}