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

    pub fn reset_header(&self) {
        let buf = unsafe { &mut *(self.0.get() as *mut [u8; N]) };
        buf[0..4].copy_from_slice(&8u32.to_le_bytes());
        buf[4..8].copy_from_slice(&8u32.to_le_bytes());
    }
}

pub(crate) fn read_u16_le(buf: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes([buf[offset], buf[offset + 1]])
}

pub(crate) fn read_u32_le(buf: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes([buf[offset], buf[offset + 1], buf[offset + 2], buf[offset + 3]])
}

pub(crate) fn read_str(buf: &[u8], pos: &mut usize, end: usize) -> Option<String> {
    if *pos + 2 > end { return None; }
    let len = read_u16_le(buf, *pos) as usize;
    *pos += 2;
    if *pos + len > end { return None; }
    let s = unsafe { String::from_utf8_unchecked(buf[*pos..*pos + len].to_vec()) };
    *pos += len;
    Some(s)
}
