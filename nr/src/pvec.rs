use std::ptr;
use std::ptr::NonNull;
use std::{alloc, borrow::BorrowMut, u8};
use core::default::Default;

use pmdk::ObjPool;

extern crate pmdk;

pub struct PVec<T> {
    //ptr: NonNull<T>,
    ptr: *mut T,
    len: usize,
    capacity: usize,
    pool: *mut pmdk::ObjPool,
}

unsafe impl Send for PVec<u32> {}
unsafe impl Sync for PVec<u32> {}

impl<T> Default for PVec<T> {
    fn default() -> PVec<T> {
        println!("default PVec");
        PVec {            
            ptr: 0 as *mut T,
            len: 0,
            capacity: 0,
            pool: 0 as *mut pmdk::ObjPool,
        }            
    }
}

impl<T> PVec<T> {
    //pub fn new(new_pool: &mut pmdk::ObjPool) -> Self {
    pub fn new() -> Self {
        println!("NEW PVEC");
        Self {
            //ptr: NonNull::dangling(),
            ptr: 0 as *mut T,
            len: 0,
            capacity: 0,
            //pool: new_pool,
            pool: 0 as *mut pmdk::ObjPool,
        }
    }

    pub fn set_pool(&mut self, _pool: &mut pmdk::ObjPool){
        self.pool = _pool;
    }

    pub fn push(&mut self, item: T) {
        assert_ne!(std::mem::size_of::<T>(), 0, "No zero sized types");

        if self.capacity == 0 {
            //let layout = alloc::Layout::array::<T>(4).expect("Could not allocation");
            // SAFETY: the layout is hardcoded to be 4 * size_of<T>
            // size_of<T>j is > 0
            let ptr = unsafe {
                (*self.pool)
                    .allocate(4, 0 as u64, None)
                    .unwrap()
                    .as_mut_ptr()
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
                (*self.pool).persist(ptr as *const core::ffi::c_void, self.len);
            };

            self.ptr = ptr;
            self.capacity = 4;
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
                (*self.pool).persist(self.ptr.add(self.len) as *const core::ffi::c_void, self.len);
            }
            self.len += 1;
        } else {
            let new_capacity = self.capacity.checked_mul(2).expect("Capacity wrapped");
            let size = std::mem::size_of::<T>() * self.capacity;
            let align = std::mem::align_of::<T>();
            size.checked_add(size % align).expect("Can't allocate");

            println! {"Old Size {}", size};
            println!("Before Resize {:?}", self.ptr);

            let ptr = unsafe {
                debug_assert!(self.len == self.capacity);
                //let layout = alloc::Layout::from_size_align_unchecked(size, align);
                let new_size = std::mem::size_of::<T>() * new_capacity;
                println! {"New Size {}", new_size};

                // reallocation
                (*self.pool)
                    .reallocate(self.ptr as *const core::ffi::c_void, new_size, 0 as u64)
                    .unwrap()
                    .as_mut_ptr()
            } as *mut T;

            if ptr.is_null() {
                panic!("Failed to allocate memory for the stack!");
            }

            //println!("After Resize {:?}", ptr);

            self.ptr = unsafe {
                ptr.add(self.len).write(item);
                //println!{"Persist {:?}", ptr.add(self.len)}
                (*self.pool).persist(ptr.add(self.len) as *const core::ffi::c_void, self.len);
                ptr
            };

            //println!("After Resize&Add {:?}", self.ptr);

            self.len += 1;
            self.capacity = new_capacity
        }
    }

    /*pub fn get(&self, index: usize) -> Option<&T> {
        if index >= self.len {
            return None;
        }
        Some(unsafe { &*self.ptr.as_ptr().add(index) })
    }*/

    pub fn pop(&mut self) -> Option<T> {
        if self.len == 0 {
            None
        } else {
            self.len -= 1;
            unsafe {
                let ret = ptr::read(self.ptr.offset(self.len as isize));
                (*self.pool).persist(
                    self.ptr.offset(self.len as isize) as *const core::ffi::c_void,
                    self.len,
                );
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
        unsafe {
            /*std::ptr::drop_in_place(std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len));
            let layout = alloc::Layout::from_size_align_unchecked(
                std::mem::size_of::<T>() * self.capacity,
                std::mem::align_of::<T>(),
            );
            alloc::dealloc(self.ptr as *mut u8, layout)*/
            println! {"Drop for Stack"};
        }
    }
}
/*
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
 */
