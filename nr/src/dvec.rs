//#![feature(ptr_internals)]

use std::alloc;
use std::ptr;
use std::ptr::NonNull;

#[derive(Debug, Clone)]
pub struct DVec<T> {
    ptr: NonNull<T>,
    len: usize,
    capacity: usize,
}

unsafe impl Send for DVec<u32> {}
unsafe impl Sync for DVec<u32> {}

impl<T> Default for DVec<T> {
    fn default() -> DVec<T> {
        println!("default DVec");
        PVec {
            ptr: NonNull::dangling(),
            len: 0,
            capacity: 0,
        }
    }
}

impl<T> DVec<T> {
    pub fn new() -> Self {
        Self {
            ptr: NonNull::dangling(),
            len: 0,
            capacity: 0,
        }
    }

    pub fn push(&mut self, item: T) {
        assert_ne!(std::mem::size_of::<T>(), 0, "No zero sized types");

        if self.capacity == 0 {
            let layout = alloc::Layout::array::<T>(4).expect("Could not allocation");
            // SAFETY: the layout is hardcoded to be 4 * size_of<T>
            // size_of<T>j is > 0
            let ptr = unsafe { alloc::alloc(layout) } as *mut T;
            let ptr = NonNull::new(ptr).expect("Could not allocate memory");

            // SAFETY: ptr is non-null and we have just allocated enough space for
            // this item (and 3 more)
            // The memory previously at ptr is not read!
            unsafe { ptr.as_ptr().write(item) };

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
            unsafe { self.ptr.as_ptr().add(self.len).write(item) }
            self.len += 1;
        } else {
            let new_capacity = self.capacity.checked_mul(2).expect("Capacity wrapped");
            let size = std::mem::size_of::<T>() * self.capacity;
            let align = std::mem::align_of::<T>();
            size.checked_add(size % align).expect("Can't allocate");

            let ptr = unsafe {
                debug_assert!(self.len == self.capacity);
                let layout = alloc::Layout::from_size_align_unchecked(size, align);
                let new_size = std::mem::size_of::<T>() * new_capacity;

                // allocation
                let ptr = alloc::realloc(self.ptr.as_ptr() as *mut u8, layout, new_size);

                let ptr = NonNull::new(ptr as *mut T).expect("Cloud not reallocate");
                ptr.as_ptr().add(self.len).write(item);
                ptr
            };
            self.ptr = ptr;
            self.len += 1;
            self.capacity = new_capacity
        }
    }

    pub fn get(&self, index: usize) -> Option<&T> {
        if index >= self.len {
            return None;
        }
        Some(unsafe { &*self.ptr.as_ptr().add(index) })
    }

    pub fn pop(&mut self) -> Option<T> {
        if self.len == 0 {
            None
        } else {
            self.len -= 1;
            unsafe { Some(ptr::read(self.ptr.as_ptr().offset(self.len as isize))) }
        }
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn len(&self) -> usize {
        self.len
    }
}

impl<T> Drop for DVec<T> {
    fn drop(&mut self) {
        unsafe {
            std::ptr::drop_in_place(std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len));
            let layout = alloc::Layout::from_size_align_unchecked(
                std::mem::size_of::<T>() * self.capacity,
                std::mem::align_of::<T>(),
            );
            alloc::dealloc(self.ptr.as_ptr() as *mut u8, layout)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        //let vec: DVec<usize> = DVec::new();
        let mut vec = DVec::<usize>::new();

        vec.push(1usize);
        vec.push(2);
        vec.push(3);
        vec.push(4);
        vec.push(5);

        assert_eq!(vec.capacity(), 8);
        assert_eq!(vec.len(), 5);
    }
}
