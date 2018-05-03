extern crate chashmap;
#[macro_use]
extern crate criterion;
#[macro_use]
extern crate lazy_static;
extern crate poirot;
extern crate rand;
extern crate rayon;

use chashmap::CHashMap;
use criterion::Criterion;
use poirot::ConcurrentHashMap;
use rand::{thread_rng, Rng};
use rayon::prelude::*;

use std::collections::HashMap;

lazy_static!{
    static ref RANDOM_VEC: Vec<u64> = {
        let mut rng = thread_rng();
        rng.gen_iter::<u64>().take(50_000).collect::<Vec<u64>>()
    };
}

fn poirot_single_thread_map_insert(c: &mut Criterion) {
    c.bench_function("poirot_single_thread_map_insert",
        |b| b.iter(|| {
            let poirot_map = ConcurrentHashMap::new();
            for x in RANDOM_VEC.iter().cloned() {
                poirot_map.insert(x, x);
            }
        })
     );
}

fn poirot_rayon_map_insert(c: &mut Criterion) {
    c.bench_function("poirot_rayon_map_insert",
        |b| b.iter(|| {
            let poirot_map = ConcurrentHashMap::new();
            RANDOM_VEC.par_iter().for_each(|&x| {poirot_map.insert(x,x);});
        })
     );
}

fn stdlib_single_thread_map_insert(c: &mut Criterion) {
    c.bench_function("stdlib_single_thread_map_insert",
        |b| b.iter(|| {
            let mut stdlib_map = HashMap::new();
            for x in RANDOM_VEC.iter().cloned() {
                stdlib_map.insert(x, x);
            }
        })
     );
}

fn chashmap_single_thread_map_insert(c: &mut Criterion) {
    c.bench_function("chashmap_single_thread_map_insert",
        |b| b.iter(|| {
            let chm = CHashMap::new();
            for x in RANDOM_VEC.iter().cloned() {
                chm.insert(x, x);
            }
        })
     );
}

fn chashmap_rayon_map_insert(c: &mut Criterion) {
    c.bench_function("chashmap_rayon_map_insert",
        |b| b.iter(|| {
            let chm = CHashMap::new();
            RANDOM_VEC.par_iter().for_each(|&x| {chm.insert(x,x);});
        })
     );
}

criterion_group!(poirot_map, poirot_single_thread_map_insert, poirot_rayon_map_insert);
criterion_group!(stdlib_map, stdlib_single_thread_map_insert);
criterion_group!(chashmap, chashmap_single_thread_map_insert, chashmap_rayon_map_insert);
criterion_main!(poirot_map, stdlib_map, chashmap);