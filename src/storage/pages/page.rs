use std::fmt::{Debug, Formatter};
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex, RwLock};
use crate::buffer::replacer::PageId;
use crate::storage::disk::disk_manager::PAGE_SIZE;

pub type Data = Arc<RwLock<Align4096>>;

#[derive(Clone)]
pub struct Page {
    data: Arc<RwLock<Align4096>>,
    page_id: Option<PageId>,
    is_dirty: bool,
    pin_count: usize,
}

#[repr(align(8))]
pub struct Align4096(pub(crate) [u8; PAGE_SIZE]);

impl Deref for Align4096 {
    type Target = [u8; PAGE_SIZE];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Align4096 {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Page {
    pub fn new() -> Self {
        Page {
            data: Arc::new(RwLock::new(Align4096([0u8; PAGE_SIZE]))),
            page_id: None,
            is_dirty: false,
            pin_count: 0,
        }
    }

    pub fn get_page_id(&self) -> Option<PageId> {
        self.page_id
    }

    pub fn get_data(&self) -> Arc<RwLock<Align4096>> {
        self.data.clone()
    }

    pub fn is_dirty(&self) -> bool {
        self.is_dirty
    }

    pub fn get_pin_count(&self) -> usize {
        self.pin_count
    }

    pub fn set_page_id(&mut self, page_id: PageId) {
        self.page_id = Some(page_id);
    }

    pub fn set_is_dirty(&mut self, is_dirty: bool) {
        self.is_dirty = is_dirty;
    }

    pub fn set_pin_count(&mut self, pin_count: usize) {
        self.pin_count = pin_count;
    }

    pub fn increase_pin_count(&mut self) {
        self.pin_count += 1;
    }

    pub fn decrease_pin_count(&mut self) {
        self.pin_count -= 1;
    }

    pub fn reset_data(&mut self) {
        self.data = Arc::new(RwLock::new(Align4096([0u8; PAGE_SIZE])));
    }
}
