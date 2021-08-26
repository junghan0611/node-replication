// Copyright © 2019-2020 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! A minimal example that implements a replicated hashmap

extern crate hwloc2;
extern crate libc;
extern crate core_affinity;

use std::collections::HashMap;

use node_replication::Dispatch;
use node_replication::Log;
use node_replication::Replica;
use node_replication::PHashMap;

use hwloc2::{Topology, ObjectType, CpuBindFlags, CpuSet};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::prelude::FileExt;
use std::time::{Duration, Instant};
use std::sync::{Arc, Barrier, Mutex};

/// The node-replicated hashmap uses a std hashmap internally.
#[derive(Default)]
struct NrHashMap {
    storage: PHashMap<u64, u64>,
}

/// We support mutable put operation on the hashmap.
#[derive(Clone, Debug, PartialEq)]
enum Modify {
    Put(u64, u64),
}

/// We support an immutable read operation to lookup a key from the hashmap.
#[derive(Clone, Debug, PartialEq)]
enum Access {
    Get(u64),
}

/// The Dispatch traits executes `ReadOperation` (our Access enum)
/// and `WriteOperation` (our `Modify` enum) against the replicated
/// data-structure.
impl Dispatch for NrHashMap {
    type ReadOperation = Access;
    type WriteOperation = Modify;
    type Response = Option<u64>;

    /// The `dispatch` function applies the immutable operations.
    fn dispatch(&self, op: Self::ReadOperation) -> Self::Response {
        match op {
            Access::Get(key) => self.storage.get(&key).map(|v| *v),
        }
    }

    /// The `dispatch_mut` function applies the mutable operations.
    fn dispatch_mut(&mut self, op: Self::WriteOperation) -> Self::Response {
        match op {
            Modify::Put(key, value) => self.storage.insert(key, value),
        }
    }
}

const K_CHECKPOINT_SECONDS: u64 = 30;
const K_COMPLETE_PENDING_INTERVAL: usize = 1600;
const K_REFRESH_INTERVAL: usize = 64;
//const K_RUN_TIME: u64 = 360;
const K_RUN_TIME: u64 = 60;
//const K_CHUNK_SIZE: usize = 3200;
const K_CHUNK_SIZE: usize = 4000;
const K_FILE_CHUNK_SIZE: usize = 131072;

//const K_INIT_COUNT: usize = 250000000;
//const K_TXN_COUNT: usize = 1000000000;

//const K_INIT_COUNT: usize = 100000; // 0.1M
const K_INIT_COUNT: usize = 1000000; // 1M
const K_TXN_COUNT: usize = 50000000;

const K_NANOS_PER_SECOND: usize = 1000000000;

const K_THREAD_STACK_SIZE: usize = 4 * 1024 * 1024;

// for ycsb
pub fn load_files(load_file: &str, run_file: &str) -> (Vec<u64>, Vec<u64>) {
    let load_file = File::open(load_file).expect("Unable to open load file");
    let run_file = File::open(run_file).expect("Unable to open run file");

    let mut buffer = [0; K_FILE_CHUNK_SIZE];
    let mut count = 0;
    let mut offset = 0;

    let mut init_keys = Vec::with_capacity(K_INIT_COUNT);

    println!("Loading keys into memory");
    loop {
        let bytes_read = load_file.read_at(&mut buffer, offset).unwrap();
        for i in 0..(bytes_read / 8) {
            let mut num = [0; 8];
            num.copy_from_slice(&buffer[i..i + 8]);
            init_keys.insert(count, u64::from_be_bytes(num));            
            count += 1;
        }
        if bytes_read == K_FILE_CHUNK_SIZE {
            offset += K_FILE_CHUNK_SIZE as u64;
        } else {
            break;
        }
    }
    if K_INIT_COUNT != count {
        panic!("Init file load fail!");
    }
    println!("Loaded {} keys", count);

    let mut count = 0;
    let mut offset = 0;

    let mut run_keys = Vec::with_capacity(K_TXN_COUNT);

    println!("Loading txns into memory");
    loop {
        let bytes_read = run_file.read_at(&mut buffer, offset).unwrap();
        for i in 0..(bytes_read / 8) {
            let mut num = [0; 8];
            num.copy_from_slice(&buffer[i..i + 8]);
            //println!("{}:{}", count, u64::from_be_bytes(num));
            run_keys.insert(count, u64::from_be_bytes(num));
            count += 1;
        }
        if bytes_read == K_FILE_CHUNK_SIZE {
            offset += K_FILE_CHUNK_SIZE as u64;
        } else {
            break;
        }
    }
    if K_TXN_COUNT != count {
        panic!("Txn file load fail!");
    }
    println!("Loaded {} txns", count);

    (init_keys, run_keys)
}

fn cpuset_for_core(topology: &Topology, idx: usize) -> CpuSet {

    println!("idx: {}", idx);

    let cores = (*topology).objects_with_type(&ObjectType::Core).unwrap();
    //let index = cores.get(idx);
    //match index {
    match cores.get(idx) {
        Some(val) => val.cpuset().unwrap(),
        None => panic!("No Core found with id {}", idx),
    }
}

/// We initialize a log, and two replicas for a hashmap, register with the replica
/// and then execute operations.
fn main() {
    // The operation log for storing `WriteOperation`, it has a size of 32 MiB:
    let log = Arc::new(Log::<<NrHashMap as Dispatch>::WriteOperation>::new(
        32 * 1024 * 1024,
    ));

    let load_keys_file = "/home/junghan/workspace/workloads/zipf-workload-loada-1M.dat";    
    let run_keys_file = "/home/junghan/workspace/workloads/zipf-workload-runa-50M.dat";

    //let store = Arc::new(FasterKvBuilder::new(table_size, log_size).with_disk(&dir_path).build().unwrap());
    let (load_keys, txn_keys) = load_files(load_keys_file, run_keys_file);
    let load_keys = Arc::new(load_keys);
    let txn_keys = Arc::new(txn_keys);
     
    // Next, we create two replicas of the hashmap
    let replica1 = Replica::<NrHashMap>::new(&log);
    let replica2 = Replica::<NrHashMap>::new(&log);

    let num_threads = 2;

    println!("Populating datastore");

    //populate_store(&store, &load_keys, num_threads);
    {
        let topo = Arc::new(Mutex::new(Topology::new().unwrap()));
        let idx = Arc::new(AtomicUsize::new(0));
        let mut threads = vec![];
        // Need a barrier to synchronize starting of threads
        let barrier = Arc::new(Barrier::new(num_threads));
        
        println!("num_threads: {}", num_threads);
    
        for thread_idx in 0..num_threads {
            let idx = Arc::clone(&idx);
            let keys = Arc::clone(&load_keys);
            let child_topo = topo.clone();
            let b = barrier.clone();

            let core_ids = core_affinity::get_core_ids().unwrap()[0].id;
            println!("core_ids: {}", core_ids);
            
            
            let replica = replica1.clone();

            threads.push(std::thread::spawn(move || {
                {
                    // Bind thread to core
                    let tid = unsafe { libc::pthread_self() };
                    let mut locked_topo = child_topo.lock().unwrap();
    
                    println!("thread_idx: {}", thread_idx);
    
                    let bind_to = cpuset_for_core(&*locked_topo, thread_idx as usize);
                    locked_topo
                        .set_cpubind_for_thread(tid, bind_to, CpuBindFlags::CPUBIND_THREAD)
                        .unwrap();
                }
                
                let ridx = replica.register().expect("Unable to register with log");
                
                let mut chunk_idx = idx.fetch_add(K_CHUNK_SIZE, Ordering::SeqCst);

                b.wait();

                while chunk_idx < K_INIT_COUNT {
                    for i in chunk_idx..(chunk_idx + K_CHUNK_SIZE) {
                       
                        if i % K_REFRESH_INTERVAL == 0 {
                            replica.sync(ridx);
                        }

                        //println!("i: {}, key: {}", i, &*keys.get(i as usize).unwrap());
                        //store.upsert(&*keys.get(i as usize).unwrap(), &42, i as u64);
                        replica.execute_mut(Modify::Put(*keys.get(i as usize).unwrap(), 42), ridx);
                    }

                    chunk_idx = idx.fetch_add(K_CHUNK_SIZE, Ordering::SeqCst);
                    println!("id {}, chunk_idx {}", thread_idx, chunk_idx);
                }                
                
                b.wait();
                //replica.sync(ridx);
                //store.complete_pending(true);
                //store.stop_session();
            }));
        }
        for t in threads {
            t.join().expect("Something went wrong in a thread");
        }
    }

    println!("Beginning benchmark");
    //run_benchmark(&store, &txn_keys, num_threads, op_allocator);

    return ;

    // The replica executes a Modify or Access operations by calling
    // `execute_mut` and `execute`. Eventually they end up in the `Dispatch` trait.
    let thread_loop = |replica: &Arc<Replica<NrHashMap>>, ridx| {
        for i in 0..2048 {
            let _r = match i % 2 {
                0 => replica.execute_mut(Modify::Put(i, i + 1), ridx),
                1 => {
                    let response = replica.execute(Access::Get(i - 1), ridx);
                    assert_eq!(response, Some(i));
                    response
                }
                _ => unreachable!(),
            };
        }
    };

    // Finally, we spawn three threads that issue operations, thread 1 and 2
    // will use replica1 and thread 3 will use replica 2:
    let mut threads = Vec::with_capacity(3);
    let replica11 = replica1.clone();
    threads.push(std::thread::spawn(move || {
        let ridx = replica11.register().expect("Unable to register with log");
        thread_loop(&replica11, ridx);
    }));

    let replica12 = replica1.clone();
    threads.push(std::thread::spawn(move || {
        let ridx = replica12.register().expect("Unable to register with log");
        thread_loop(&replica12, ridx);
    }));

    threads.push(std::thread::spawn(move || {
        let ridx = replica2.register().expect("Unable to register with log");
        thread_loop(&replica2, ridx);
    }));

    // Wait for all the threads to finish
    for thread in threads {
        thread.join().unwrap();
    }


    

}
