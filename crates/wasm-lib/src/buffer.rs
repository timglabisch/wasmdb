use std::cell::UnsafeCell;

pub struct SharedBuffer<const N: usize>(UnsafeCell<[u8; N]>);
unsafe impl<const N: usize> Sync for SharedBuffer<N> {}

impl<const N: usize> SharedBuffer<N> {
    pub const fn new() -> Self {
        Self(UnsafeCell::new([0; N]))
    }

    pub fn ptr(&self) -> *mut u8 {
        self.0.get() as *mut u8
    }

    pub fn as_slice(&self) -> &[u8] {
        unsafe { &(*self.0.get()) }
    }
}
