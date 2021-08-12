use core::default::Default;
use std::alloc::{self, Layout};
use std::marker::PhantomData;
use std::mem;
use std::ops::{Deref, DerefMut};
use std::ptr::{self, NonNull};

extern crate pmdk;
extern crate pmdk_sys;

extern crate core_affinity;

use crate::PMPOOL1;
use crate::PMPOOL2;

#[derive(Debug, Clone)]
struct RawVec<T> {
    ptr: NonNull<T>,
    cap: usize,
    _marker: PhantomData<T>,
}

unsafe impl<T: Send> Send for RawVec<T> {}
unsafe impl<T: Sync> Sync for RawVec<T> {}

impl<T> RawVec<T> {
    fn new() -> Self {
        // !0 is usize::MAX. This branch should be stripped at compile time.
        let cap = if mem::size_of::<T>() == 0 { !0 } else { 0 };

        println!("PVec::new()");

        // `NonNull::dangling()` doubles as "unallocated" and "zero-sized allocation"
        RawVec {
            ptr: NonNull::dangling(),
            cap: cap,
            _marker: PhantomData,
        }
    }

    fn with_capacity(capacity: usize) -> Self {
        let new_cap = if mem::size_of::<T>() == 0 {
            !0
        } else {
            capacity
        };
        let size = new_cap * mem::size_of::<T>();
        
        println!("PVec::with_capacity {}", capacity);

        // Ensure that the new allocation doesn't exceed `isize::MAX` bytes.
        assert!(new_cap <= isize::MAX as usize, "Allocation too large");

        let core_ids = core_affinity::get_core_ids().unwrap()[0].id;

        let new_ptr = NonNull::new(unsafe { 
            if core_ids < 20 {
                PMPOOL1.allocate(size, 0 as u64, None).unwrap().as_mut_ptr()
            } else {
                PMPOOL2.allocate(size, 0 as u64, None).unwrap().as_mut_ptr()
            }
        } as *mut T).unwrap();

        RawVec {
            ptr: new_ptr,
            cap: new_cap,
            _marker: PhantomData,
        }
    }

    fn grow(&mut self) {
        // since we set the capacity to usize::MAX when T has size 0,
        // getting to here necessarily means the PVec is overfull.
        assert!(mem::size_of::<T>() != 0, "capacity overflow");

        let new_cap = if self.cap == 0 { 1 } else { 2 * self.cap };
        let size = new_cap * mem::size_of::<T>();

        // Ensure that the new allocation doesn't exceed `isize::MAX` bytes.
        assert!(new_cap <= isize::MAX as usize, "Allocation too large");

        let core_ids = core_affinity::get_core_ids().unwrap()[0].id;

        let new_ptr = if self.cap == 0 {
            //println!("grow:: cap==0 size {}", size);
            unsafe { 
                if core_ids < 20 {
                    PMPOOL1.allocate(size, 0 as u64, None).unwrap().as_mut_ptr() 
                } else {
                    PMPOOL2.allocate(size, 0 as u64, None).unwrap().as_mut_ptr() 
                }
            }
        } else {
            let align = mem::align_of::<T>();
            size.checked_add(size % align).expect("Can't allocate");
            let old_ptr = self.ptr.as_ptr() as *mut u8;
            unsafe {
                if core_ids < 20 {
                    PMPOOL1
                    .reallocate(old_ptr as *const core::ffi::c_void, size, 0 as u64)
                    .unwrap()
                    .as_mut_ptr()
                } else {
                    PMPOOL2
                    .reallocate(old_ptr as *const core::ffi::c_void, size, 0 as u64)
                    .unwrap()
                    .as_mut_ptr()
                }
            }
        };

        // If allocation fails, `new_ptr` will be null, in which case we abort.
        self.ptr = match NonNull::new(new_ptr as *mut T) {
            Some(p) => p,
            None => panic!("Failed to allocate memory for the PVec!"),
        };

        self.cap = new_cap;
    }
}

impl<T> Drop for RawVec<T> {
    fn drop(&mut self) {
        let elem_size = mem::size_of::<T>();
        let core_ids = core_affinity::get_core_ids().unwrap()[0].id;

        if self.cap != 0 && elem_size != 0 {
            //println!("Drop::RawVec<T> {}", self.ptr.as_ptr() as u64);
            unsafe {
                if core_ids < 20 {
                    PMPOOL1.dealloc(self.ptr.as_ptr() as *const core::ffi::c_void);
                } else {
                    PMPOOL2.dealloc(self.ptr.as_ptr() as *const core::ffi::c_void);
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct PVec<T> {
    buf: RawVec<T>,
    len: usize,
}

impl<T> Default for PVec<T> {
    fn default() -> PVec<T> {        
        PVec {
            buf: RawVec::new(),
            len: 0,
        }
    }
}

impl<T> PVec<T> {
    fn ptr(&self) -> *mut T {
        self.buf.ptr.as_ptr()
    }

    pub unsafe fn set_len(&mut self, new_len: usize) {
        assert!(new_len <= self.capacity());
        self.len = new_len;
    }

    pub fn capacity(&self) -> usize {
        self.buf.cap
    }

    pub fn new() -> Self {
        PVec {
            buf: RawVec::new(),
            len: 0,
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        PVec {
            buf: RawVec::with_capacity(capacity),
            len: 0,
        }
    }

    pub fn push(&mut self, elem: T) {
        if self.len == self.capacity() {
            self.buf.grow();
        }

        unsafe {
            ptr::write(self.ptr().add(self.len), elem);
        }

        // Can't overflow, we'll OOM first.
        self.len += 1;
    }

    pub fn pop(&mut self) -> Option<T> {
        if self.len == 0 {
            None
        } else {
            self.len -= 1;
            unsafe { Some(ptr::read(self.ptr().add(self.len))) }
        }
    }

    pub fn insert(&mut self, index: usize, elem: T) {
        assert!(index <= self.len, "index out of bounds");
        if self.capacity() == self.len {
            self.buf.grow();
        }

        unsafe {
            ptr::copy(
                self.ptr().add(index),
                self.ptr().add(index + 1),
                self.len - index,
            );
            ptr::write(self.ptr().add(index), elem);
            self.len += 1;
        }
    }

    pub fn remove(&mut self, index: usize) -> T {
        assert!(index < self.len, "index out of bounds");
        unsafe {
            self.len -= 1;
            let result = ptr::read(self.ptr().add(index));
            ptr::copy(
                self.ptr().add(index + 1),
                self.ptr().add(index),
                self.len - index,
            );
            result
        }
    }

    pub fn swap_remove(&mut self, index: usize) -> T {
        assert!(index < self.len, "index out of bounds");

        let len = self.len();
        if index >= len {
            panic!();
        }
        unsafe {
            // We replace self[index] with the last element. Note that if the
            // bounds check above succeeds there must be a last element (which
            // can be self[index] itself).
            let last = ptr::read(self.as_ptr().add(len - 1));
            let hole = self.as_mut_ptr().add(index);
            self.set_len(len - 1);
            ptr::replace(hole, last)
        }
    }

    pub fn into_iter(self) -> IntoIter<T> {
        unsafe {
            let iter = RawValIter::new(&self);
            let buf = ptr::read(&self.buf);
            mem::forget(self);

            IntoIter {
                iter: iter,
                _buf: buf,
            }
        }
    }

    pub fn drain(&mut self) -> Drain<T> {
        unsafe {
            let iter = RawValIter::new(&self);

            // this is a mem::forget safety thing. If Drain is forgotten, we just
            // leak the whole PVec's contents. Also we need to do this *eventually*
            // anyway, so why not do it now?
            self.len = 0;

            Drain {
                iter: iter,
                vec: PhantomData,
            }
        }
    }
}

impl<T> Drop for PVec<T> {
    fn drop(&mut self) {
        while let Some(_) = self.pop() {}
        // deallocation is handled by RawVec
    }
}

impl<T> Deref for PVec<T> {
    type Target = [T];
    fn deref(&self) -> &[T] {
        unsafe { std::slice::from_raw_parts(self.ptr(), self.len) }
    }
}

impl<T> DerefMut for PVec<T> {
    fn deref_mut(&mut self) -> &mut [T] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr(), self.len) }
    }
}

struct RawValIter<T> {
    start: *const T,
    end: *const T,
}

impl<T> RawValIter<T> {
    unsafe fn new(slice: &[T]) -> Self {
        RawValIter {
            start: slice.as_ptr(),
            end: if mem::size_of::<T>() == 0 {
                ((slice.as_ptr() as usize) + slice.len()) as *const _
            } else if slice.len() == 0 {
                slice.as_ptr()
            } else {
                slice.as_ptr().add(slice.len())
            },
        }
    }
}

impl<T> Iterator for RawValIter<T> {
    type Item = T;
    fn next(&mut self) -> Option<T> {
        if self.start == self.end {
            None
        } else {
            unsafe {
                let result = ptr::read(self.start);
                self.start = if mem::size_of::<T>() == 0 {
                    (self.start as usize + 1) as *const _
                } else {
                    self.start.offset(1)
                };
                Some(result)
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let elem_size = mem::size_of::<T>();
        let len =
            (self.end as usize - self.start as usize) / if elem_size == 0 { 1 } else { elem_size };
        (len, Some(len))
    }
}

impl<T> DoubleEndedIterator for RawValIter<T> {
    fn next_back(&mut self) -> Option<T> {
        if self.start == self.end {
            None
        } else {
            unsafe {
                self.end = if mem::size_of::<T>() == 0 {
                    (self.end as usize - 1) as *const _
                } else {
                    self.end.offset(-1)
                };
                Some(ptr::read(self.end))
            }
        }
    }
}

pub struct IntoIter<T> {
    _buf: RawVec<T>, // we don't actually care about this. Just need it to live.
    iter: RawValIter<T>,
}

impl<T> Iterator for IntoIter<T> {
    type Item = T;
    fn next(&mut self) -> Option<T> {
        self.iter.next()
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter.size_hint()
    }
}

impl<T> DoubleEndedIterator for IntoIter<T> {
    fn next_back(&mut self) -> Option<T> {
        self.iter.next_back()
    }
}

impl<T> Drop for IntoIter<T> {
    fn drop(&mut self) {
        for _ in &mut *self {}
    }
}

pub struct Drain<'a, T: 'a> {
    vec: PhantomData<&'a mut PVec<T>>,
    iter: RawValIter<T>,
}

impl<'a, T> Iterator for Drain<'a, T> {
    type Item = T;
    fn next(&mut self) -> Option<T> {
        self.iter.next()
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter.size_hint()
    }
}

impl<'a, T> DoubleEndedIterator for Drain<'a, T> {
    fn next_back(&mut self) -> Option<T> {
        self.iter.next_back()
    }
}

impl<'a, T> Drop for Drain<'a, T> {
    fn drop(&mut self) {
        // pre-drain the iter
        for _ in &mut *self {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    pub fn create_pm_pool() {
        let mut pmpool: pmdk::ObjPool = {
            let path = String::from("/mnt/pmem0/pvec_test.pool");
            //let path = String::from("/dev/shm/pmem0/test.pool");
            let mut pool =
                pmdk::ObjPool::new::<_, String>(path, None, 0x1000, 0x2000_0000 / 0x1000).unwrap();
            pool.set_rm_on_drop(true);
            pool
        };
    }

    #[test]
    pub fn create_push_pop() {
        //let mut v = PVec::new();
        let mut v = PVec::with_capacity(2);
        v.push(1);
        assert_eq!(1, v.len());
        assert_eq!(1, v[0]);
        for i in v.iter_mut() {
            *i += 1;
        }
        v.insert(0, 5);
        let x = v.pop();
        assert_eq!(Some(2), x);
        assert_eq!(1, v.len());
        v.push(10);
        let x = v.remove(0);
        assert_eq!(5, x);
        assert_eq!(1, v.len());
    }

    #[test]
    pub fn iter_test() {
        let mut v = PVec::new();
        //let mut v = PVec::with_capacity(2);
        for i in 0..10 {
            v.push(Box::new(i))
        }
        let mut iter = v.into_iter();
        let first = iter.next().unwrap();
        let last = iter.next_back().unwrap();
        drop(iter);
        assert_eq!(0, *first);
        assert_eq!(9, *last);
    }
    #[test]
    pub fn test_drain() {
        let mut v = PVec::new();
        for i in 0..10 {
            v.push(Box::new(i))
        }
        {
            let mut drain = v.drain();
            let first = drain.next().unwrap();
            let last = drain.next_back().unwrap();
            assert_eq!(0, *first);
            assert_eq!(9, *last);
        }
        assert_eq!(0, v.len());
        v.push(Box::new(1));
        assert_eq!(1, *v.pop().unwrap());
    }
    #[test]
    pub fn test_zst() {
        let mut v = PVec::new();
        for _i in 0..10 {
            v.push(())
        }

        let mut count = 0;

        for _ in v.into_iter() {
            count += 1
        }

        assert_eq!(10, count);
    }
}
