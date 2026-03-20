use std::cell::UnsafeCell;

pub const BUFFER_SIZE: usize = 64 * 1024;

pub struct SharedBuffer(UnsafeCell<[u8; BUFFER_SIZE]>);
unsafe impl Sync for SharedBuffer {}

impl SharedBuffer {
    pub const fn new() -> Self {
        Self(UnsafeCell::new([0; BUFFER_SIZE]))
    }

    pub fn ptr(&self) -> *mut u8 {
        self.0.get() as *mut u8
    }

    pub fn write_bytes(&self, data: &[u8]) {
        let len = data.len().min(BUFFER_SIZE - 4);
        let buf = unsafe { &mut *self.0.get() };
        buf[..4].copy_from_slice(&(len as u32).to_le_bytes());
        buf[4..4 + len].copy_from_slice(&data[..len]);
    }
}
