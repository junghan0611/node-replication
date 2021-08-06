use core::default::Default;
use std::fmt;
use std::ops::{Deref, DerefMut};
use std::ptr;
use std::ptr::NonNull;
use std::{alloc, borrow::BorrowMut, u8};

extern crate core_affinity;
use std::thread;

use crate::PMPOOL;
use crate::PMPOOL1;

extern crate pmdk;
#[derive(Debug, Clone)]
pub struct PVec<T> {
    //ptr: NonNull<T>,
    ptr: *mut T,
    len: usize,
    capacity: usize,
}

unsafe impl Send for PVec<u32> {}
unsafe impl Sync for PVec<u32> {}

unsafe impl Send for PVec<(u64, u64)> {}
unsafe impl Sync for PVec<(u64, u64)> {}

impl<T> Default for PVec<T> {
    fn default() -> PVec<T> {
        println!("default PVec");
        let core_ids = core_affinity::get_core_ids().unwrap()[0];
        println!("Vector Create: {}", core_ids.id);
        PVec {
            ptr: 0 as *mut T,
            len: 0,
            capacity: 0,
        }
    }
}

impl<T> PVec<T> {
    //pub fn new(new_pool: &mut pmdk::ObjPool) -> Self {
    pub fn new() -> Self {
        println!("NEW PVEC");
        panic!();
        Self {
            //ptr: NonNull::dangling(),
            ptr: 0 as *mut T,
            len: 0,
            capacity: 0,
        }
    }

    pub fn push(&mut self, item: T) {
        assert_ne!(std::mem::size_of::<T>(), 0, "No zero sized types");

        let core_ids = core_affinity::get_core_ids().unwrap()[0].id;
        //println!("Vector Create: {}", core_ids.id);

        if self.capacity == 0 {
            //let layout = alloc::Layout::array::<T>(4).expect("Could not allocation");
            // SAFETY: the layout is hardcoded to be 4 * size_of<T>
            // size_of<T>j is > 0

            //println!{"capacity {}", self.capacity};
            //println!("size of item {}", std::mem::size_of::<T>()); // 16

            let size = std::mem::size_of::<T>();
            //let size = 24;

            let ptr = unsafe {
                if core_ids < 20 {
                    PMPOOL.allocate(size, 0 as u64, None).unwrap().as_mut_ptr()
                } else {
                    //PMPOOL1
                    PMPOOL1.allocate(size, 0 as u64, None).unwrap().as_mut_ptr()
                }
            } as *mut T;
            //let ptr = NonNull::new(ptr).expect("Could not allocate memory");
            if ptr.is_null() {
                panic!("Failed to allocate memory for the stack!");
            }

            // SAFETY: ptr is non-null and we have just allocated enough space for
            // this item (and 3 more)
            // The memory previously at ptr is not read!
            unsafe {
                ptr.write(item);
                /*if core_ids < 20 {
                    PMPOOL.persist(ptr as *const core::ffi::c_void, std::mem::size_of::<T>());
                } else {
                    PMPOOL1.persist(ptr as *const core::ffi::c_void, std::mem::size_of::<T>());
                }*/
            };

            self.ptr = ptr;
            self.capacity = 1;
            self.len = 1;
        } else if self.len < self.capacity {
            let offset = self
                .len
                .checked_mul(std::mem::size_of::<T>())
                .expect("Cannot reach memory location");
            assert!(offset < isize::MAX as usize, "Wrapped isize");
            // Offset cannot wrap around and pointer is pointing to valid memory
            // And writing to an offset at self.len is valid
            unsafe {
                self.ptr.add(self.len).write(item);
                /*if core_ids < 20 {
                    PMPOOL.persist(self.ptr.add(self.len) as *const core::ffi::c_void, std::mem::size_of::<T>());
                } else {
                    PMPOOL1.persist(self.ptr.add(self.len) as *const core::ffi::c_void, std::mem::size_of::<T>());
                }*/
            }
            self.len += 1;
        } else {
            let new_capacity = self.capacity.checked_mul(2).expect("Capacity wrapped");
            let size = std::mem::size_of::<T>() * self.capacity;
            let align = std::mem::align_of::<T>();
            size.checked_add(size % align).expect("Can't allocate");

            //println! {"Old Size {}", size};
            //println!("Before Resize {:?}", self.ptr);

            let ptr = unsafe {
                debug_assert!(self.len == self.capacity);
                //let layout = alloc::Layout::from_size_align_unchecked(size, align);
                let new_size = std::mem::size_of::<T>() * new_capacity;
                //println! {"New Size {}", new_size};

                if core_ids < 20 {
                    // reallocation
                    PMPOOL
                        .reallocate(self.ptr as *const core::ffi::c_void, new_size, 0 as u64)
                        .unwrap()
                        .as_mut_ptr()
                } else {
                    //PMPOOL1
                    PMPOOL1
                        .reallocate(self.ptr as *const core::ffi::c_void, new_size, 0 as u64)
                        .unwrap()
                        .as_mut_ptr()
                }
            } as *mut T;

            if ptr.is_null() {
                panic!("Failed to allocate memory for the stack!");
            }

            //println!("After Resize {:?}", ptr);

            self.ptr = unsafe {
                ptr.add(self.len).write(item);
                //println!{"Persist {:?}", ptr.add(self.len)}
                /*if core_ids < 20 {
                    PMPOOL.persist(ptr.add(self.len) as *const core::ffi::c_void, std::mem::size_of::<T>());
                } else {
                    PMPOOL1.persist(ptr.add(self.len) as *const core::ffi::c_void, std::mem::size_of::<T>());
                }*/
                ptr
            };

            //println!("After Resize&Add {:?}", self.ptr);

            self.len += 1;
            self.capacity = new_capacity
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        let core_ids = core_affinity::get_core_ids().unwrap()[0].id;
        println!("[PVec::with_capacity, Vector Create: {}", core_ids);

        let mut vec = PVec {
            ptr: 0 as *mut T,
            len: 0,
            capacity: 0,
        };

        //let size = std::mem::size_of::<T>() * capacity;
        let size = std::mem::size_of::<T>() * capacity;
        println!{"PVec::with_capacity, size {}", size};

        //println!("!! size of item {}", std::mem::size_of::<T>()); // 왜 24 바이트지? 

        //let layout = alloc::Layout::array::<T>(4).expect("Could not allocation");
        // SAFETY: the layout is hardcoded to be 4 * size_of<T>
        // size_of<T>j is > 0
        let ptr = unsafe {
            if core_ids < 20 {
                PMPOOL.allocate(size, 0 as u64, None).unwrap().as_mut_ptr()
            } else {
                //PMPOOL1
                PMPOOL1.allocate(size, 0 as u64, None).unwrap().as_mut_ptr()
            }
        } as *mut T;
       
        //let ptr = NonNull::new(ptr).expect("Could not allocate memory");
        if ptr.is_null() {
            panic!("Failed to allocate memory for the stack!");
        }
        vec.ptr = ptr;
        vec.capacity = capacity;
        vec.len = 0; 
        vec
    }

    /*pub fn get(&self, index: usize) -> Option<&T> {
        if index >= self.len {
            return None;
        }
        Some(unsafe { &*self.ptr.as_ptr().add(index) })
    }*/

    pub fn last(&mut self) -> Option<&T> {
        Some(unsafe { &*self.ptr.offset(self.len as isize) })
    }

    pub fn pop(&mut self) -> Option<T> {
        let core_ids = core_affinity::get_core_ids().unwrap()[0].id;
        //println!("Vector Create: {}", core_ids.id);

        if self.len == 0 {
            None
        } else {
            self.len -= 1;
            unsafe {
                let ret = ptr::read(self.ptr.offset(self.len as isize));

                /*if core_ids < 20 {
                    PMPOOL.persist(
                        self.ptr.offset(self.len as isize) as *const core::ffi::c_void,
                        std::mem::size_of::<T>(),
                    );
                } else {
                    PMPOOL1.persist(
                        self.ptr.offset(self.len as isize) as *const core::ffi::c_void,
                        std::mem::size_of::<T>(),
                    );
                }*/
                Some(ret)
            }
        }
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn len(&self) -> usize {
        self.len
    }
}

impl<T> Drop for PVec<T> {
    fn drop(&mut self) {
        println! {"Drop for Stack"};
        unsafe {
            //println! {"Drop for Stack"};
        }
    }
}

impl<T> Deref for PVec<T> {
    type Target = [T];
    fn deref(&self) -> &[T] {
        //unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
        unsafe { std::slice::from_raw_parts(self.ptr, self.capacity) }
    }
}

impl<T> DerefMut for PVec<T> {
    fn deref_mut(&mut self) -> &mut [T] {
        //unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) }
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.capacity) }
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        //let vec: PVec<usize> = PVec::new();
        let mut vec = PVec::<usize>::new();

        /*vec.push(1usize, &mut pool);
        vec.push(2);
        vec.push(3);
        vec.push(4);
        vec.push(5);

        assert_eq!(vec.capacity(), 8);
        assert_eq!(vec.len(), 5);*/
    }
}

