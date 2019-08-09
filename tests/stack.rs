extern crate rand;
extern crate std;

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::{Arc, Barrier};
use std::thread;
use std::usize;

use node_replication::log::Log;
use node_replication::replica::Replica;
use node_replication::Dispatch;

use rand::{thread_rng, Rng};

#[derive(Eq, PartialEq, Clone, Copy, Debug)]
enum Op {
    Push(u32),
    Pop,
    Invalid,
}

impl Default for Op {
    fn default() -> Op {
        Op::Invalid
    }
}

#[derive(Eq, PartialEq)]
struct Stack {
    storage: RefCell<Vec<u32>>,
    popped: RefCell<Vec<Option<u32>>>,
}

impl Stack {
    pub fn push(&self, data: u32) {
        self.storage.borrow_mut().push(data);
    }

    pub fn pop(&self) {
        let r = self.storage.borrow_mut().pop();
        self.popped.borrow_mut().push(r);
    }
}

impl Default for Stack {
    fn default() -> Stack {
        let s = Stack {
            storage: Default::default(),
            popped: Default::default(),
        };

        s
    }
}

impl Dispatch for Stack {
    type Operation = Op;

    fn dispatch(&self, op: Self::Operation) {
        match op {
            Op::Push(v) => self.push(v),
            Op::Pop => self.pop(),
            Op::Invalid => panic!("Got invalid OP"),
        }
    }
}

/// Sequential data structure test (one thread).
///
/// Execute operations at random, comparing the result
/// against a known correct implementation.
#[test]
fn sequential_test() {
    let log = Arc::new(Log::<<Stack as Dispatch>::Operation>::new(5 * 1024 * 1024));

    let mut orng = thread_rng();
    let nop = 50;

    let r = Replica::<Stack>::new(&log);
    let idx = r.register().expect("Failed to register with Replica.");
    let mut correct_stack: Vec<u32> = Vec::new();
    let mut correct_popped: Vec<Option<u32>> = Vec::new();

    // Populate with some initial data
    for _i in 0..50 {
        let element = orng.gen();
        r.execute(Op::Push(element), idx);
        correct_stack.push(element);
    }

    for _i in 0..nop {
        let op: usize = orng.gen();
        match op % 2usize {
            0usize => {
                r.execute(Op::Pop, idx);
                correct_popped.push(correct_stack.pop());
            }
            1usize => {
                let element = orng.gen();
                r.execute(Op::Push(element), idx);
                correct_stack.push(element);
            }
            _ => unreachable!(),
        }
    }

    unsafe {
        let s = r.data();
        assert_eq!(
            correct_popped,
            *s.popped.borrow_mut(),
            "Pop operation error detected"
        );
        assert_eq!(
            correct_stack,
            *s.storage.borrow_mut(),
            "Push operation error detected"
        );
    }
}

/// A stack to verify that the log works correctly with multiple threads.
#[derive(Eq, PartialEq)]
struct VerifyStack {
    storage: RefCell<Vec<u32>>,
    per_replica_counter: RefCell<HashMap<u16, u16>>,
}

impl VerifyStack {
    pub fn push(&self, data: u32) {
        self.storage.borrow_mut().push(data);
    }

    pub fn pop(&self) -> u32 {
        self.storage.borrow_mut().pop().unwrap()
    }
}

impl Default for VerifyStack {
    fn default() -> VerifyStack {
        let s = VerifyStack {
            storage: Default::default(),
            per_replica_counter: Default::default(),
        };

        s
    }
}

impl Dispatch for VerifyStack {
    type Operation = Op;

    fn dispatch(&self, op: Self::Operation) {
        match op {
            Op::Push(v) => {
                let _tid = (v & 0xffff) as u16;
                let _val = ((v >> 16) & 0xffff) as u16;
                //println!("Push tid {} val {}", tid, val);
                self.push(v)
            }
            Op::Pop => {
                let ele: u32 = self.pop();
                let tid = (ele & 0xffff) as u16;
                let val = ((ele >> 16) & 0xffff) as u16;
                //println!("POP tid {} val {}", tid, val);
                let mut per_replica_counter = self.per_replica_counter.borrow_mut();

                let cnt = per_replica_counter.get(&tid).unwrap_or(&u16::max_value());
                if *cnt <= val {
                    println!(
                        "assert violation cnt={} val={} tid={} {:?}",
                        *cnt, val, tid, per_replica_counter
                    );
                }
                assert!(
                    *cnt > val,
                    "Elements that came from a given thread are monotonically decreasing"
                );
                per_replica_counter.insert(tid, val);

                if val == 0 {
                    // This is one of our last elements, so we sanity check that we've
                    // seen values from all threads by now (if not we may have been really unlucky
                    // with thread scheduling or something is wrong with fairness in our implementation)
                    // println!("per_replica_counter ={:?}", per_replica_counter);
                    assert_eq!(per_replica_counter.len(), 8, "Popped a final element from a thread before seeing elements from every thread.");
                }
            }
            Op::Invalid => panic!("Got invalid OP"),
        }
    }
}

/// Many threads run in parallel, each pushing a unique increasing element into the stack.
// Then, a single thread pops all elements and checks that they are popped in the right order.
#[test]
fn parallel_push_sequential_pop_test() {
    let t = 4usize;
    let r = 2usize;
    let l = 1usize;
    let nop: u16 = 50000;

    let log = Arc::new(Log::<<Stack as Dispatch>::Operation>::new(
        l * 1024 * 1024 * 1024,
    ));

    let mut replicas = Vec::with_capacity(r);
    for _i in 0..r {
        replicas.push(Arc::new(Replica::<VerifyStack>::new(&log)));
    }

    let mut threads = Vec::new();
    let barrier = Arc::new(Barrier::new(t * r));

    for i in 0..r {
        for j in 0..t {
            let replica = replicas[i].clone();
            let b = barrier.clone();
            let child = thread::spawn(move || {
                let tid: u32 = (i * t + j) as u32;
                //println!("tid = {} i={} j={}", tid, i, j);
                let idx = replica
                    .register()
                    .expect("Failed to register with replica.");

                // 1. Insert phase
                b.wait();
                for i in 0..nop {
                    replica.execute(Op::Push((i as u32) << 16 | tid), idx);
                }
            });
            threads.push(child);
        }
    }

    for _i in 0..threads.len() {
        let _retval = threads
            .pop()
            .unwrap()
            .join()
            .expect("Thread didn't finish successfully.");
    }

    // Verify by popping everything off all replicas:
    for i in 0..r {
        let replica = replicas[i].clone();
        for _j in 0..t {
            for _z in 0..nop {
                replica.execute(Op::Pop, 0);
            }
        }
    }
}

/// Many threads run in parallel, each pushing a unique increasing element into the stack.
/// Then, many threads run in parallel, each popping an element and checking that the
/// elements that came from a given thread are monotonically decreasing.
#[test]
fn parallel_push_and_pop_test() {
    let t = 4usize;
    let r = 2usize;
    let l = 1usize;
    let nop: u16 = 50000;

    let log = Arc::new(Log::<<Stack as Dispatch>::Operation>::new(
        l * 1024 * 1024 * 1024,
    ));

    let mut replicas = Vec::with_capacity(r);
    for _i in 0..r {
        replicas.push(Arc::new(Replica::<VerifyStack>::new(&log)));
    }

    let mut threads = Vec::new();
    let barrier = Arc::new(Barrier::new(t * r));

    for i in 0..r {
        for j in 0..t {
            let replica = replicas[i].clone();
            let b = barrier.clone();
            let child = thread::spawn(move || {
                let tid: u32 = (i * t + j) as u32;
                //println!("tid = {} i={} j={}", tid, i, j);
                let idx = replica
                    .register()
                    .expect("Failed to register with replica.");

                // 1. Insert phase
                b.wait();
                for i in 0..nop {
                    replica.execute(Op::Push((i as u32) << 16 | tid), idx);
                }

                // 2. Dequeue phase, verification
                b.wait();
                for _i in 0..nop {
                    replica.execute(Op::Pop, idx);
                }
            });
            threads.push(child);
        }
    }

    for _i in 0..threads.len() {
        let _retval = threads
            .pop()
            .unwrap()
            .join()
            .expect("Thread didn't finish successfully.");
    }
}

fn bench(r: Arc<Replica<Stack>>, nop: usize, barrier: Arc<Barrier>) -> (u64, u64) {
    let idx = r.register().expect("Failed to register with Replica.");

    let mut orng = thread_rng();
    let mut arng = thread_rng();

    let mut ops = Vec::with_capacity(nop);
    for _i in 0..nop {
        let op: usize = orng.gen();
        match op % 2usize {
            0usize => ops.push(Op::Pop),
            1usize => ops.push(Op::Push(arng.gen())),
            _ => unreachable!(),
        }
    }
    barrier.wait();

    for i in 0..nop {
        r.execute(ops[i], idx);
    }

    barrier.wait();

    (0, 0)
}

/// Verify that 2 replicas are equal after a set of random
/// operations have been executed against the log.
#[test]
fn replicas_are_equal() {
    let t = 4usize;
    let r = 2usize;
    let l = 1usize;
    let n = 50usize;

    let log = Arc::new(Log::<<Stack as Dispatch>::Operation>::new(
        l * 1024 * 1024 * 1024,
    ));

    let mut replicas = Vec::with_capacity(r);
    for _i in 0..r {
        replicas.push(Arc::new(Replica::<Stack>::new(&log)));
    }

    let mut threads = Vec::new();
    let barrier = Arc::new(Barrier::new(t * r));

    for i in 0..r {
        for _j in 0..t {
            let r = replicas[i].clone();
            let o = n.clone();
            let b = barrier.clone();
            let child = thread::spawn(move || bench(r, o, b));
            threads.push(child);
        }
    }

    for _i in 0..threads.len() {
        let _retval = threads
            .pop()
            .unwrap()
            .join()
            .expect("Thread didn't finish successfully.");
    }

    unsafe {
        let s0 = Arc::try_unwrap(replicas.pop().unwrap()).unwrap().data();
        let s1 = Arc::try_unwrap(replicas.pop().unwrap()).unwrap().data();
        assert_eq!(s0.storage, s1.storage, "Data-structures don't match.");
        assert_eq!(
            s0.popped, s1.popped,
            "Removed elements in each replica dont match."
        );
    }
}