use crate::buffer::replacer::{FrameId, LRUReplacer, PageId, Replacer};
use crate::storage::disk::disk_manager::{DiskManager, DiskManagerInstance};
use crate::storage::pages::page::{Data, Page};
use libc::free;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::ops::{Deref, DerefMut};
use std::slice::IterMut;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::time::Duration;

pub struct BufferPoolManager<R: Replacer, D: DiskManager> {
    pool_size: usize,
    num_instances: usize,
    instance_index: usize,
    deleted_page_ids: Vec<u32>,
    next_page_id: u32,
    replacer: R,
    frames: Vec<Page>,
    page_table: HashMap<PageId, FrameId>,
    free_list: Vec<FrameId>,
    disk_manager: Arc<D>,
}


impl<R: Replacer, D: DiskManager> BufferPoolManager<R, D> {
    fn new(
        pool_size: usize,
        num_instances: usize,
        instance_index: usize,
        disk_manager: Arc<D>,
    ) -> Self {
        let next_page_id = instance_index as u32;
        let replacer = R::new(pool_size);
        let frames = vec![Page::new(); pool_size];
        let page_table = HashMap::new();
        let deleted_page_ids = Vec::new();
        let free_list = (0..pool_size).map(FrameId).collect();
        BufferPoolManager {
            pool_size,
            num_instances,
            instance_index,
            next_page_id,
            replacer,
            frames,
            page_table,
            free_list,
            disk_manager,
            deleted_page_ids,
        }
    }

    fn alloc_frame(&mut self) -> Option<FrameId> {
        if let Some(frame_id) = self.free_list.pop() {
            Some(frame_id)
        } else {
            self.replacer.victim()
        }
    }

    fn alloc_page_id(&mut self) -> PageId {
        if let Some(page_id) = self.deleted_page_ids.pop() {
            PageId(page_id)
        } else {
            let page_id = self.next_page_id;
            self.next_page_id += self.num_instances as u32;
            PageId(page_id)
        }
    }

    fn fetch_page(&mut self, page_id: PageId) -> Option<Data> {
        if let Some(frame_id) = self.page_table.get(&page_id) {
            let mut page = &mut self.frames[frame_id.0];
            self.replacer.pin(*frame_id);
            page.increase_pin_count();
            Some(page.get_data())
        } else {
            let victim_frame_id = self.replacer.victim()?;
            self.replacer.pin(victim_frame_id);
            let victim_page = &mut self.frames[victim_frame_id.0];
            if victim_page.is_dirty() {
                self.disk_manager
                    .write_page(victim_page.get_page_id().unwrap(), &(*victim_page.get_data().read().unwrap()).0);
            }
            self.page_table.remove(&victim_page.get_page_id().unwrap());
            self.page_table.insert(page_id, victim_frame_id);
            victim_page.set_pin_count(1);
            victim_page.set_is_dirty(false);
            victim_page.set_page_id(page_id);
            self.disk_manager
                .read_page(page_id, &mut (*victim_page.get_data().write().unwrap()).0);
            Some(victim_page.get_data())
        }
    }

    fn unpin_page(&mut self, page_id: PageId, is_dirty: bool) {
        let frame_id = self.page_table.get(&page_id).unwrap();
        let page = &mut self.frames[frame_id.0];
        page.decrease_pin_count();
        if page.get_pin_count() == 0 {
            self.replacer.unpin(*frame_id);
        }
        if !page.is_dirty() {
            page.set_is_dirty(is_dirty);
        }
    }

    fn flush_page(&mut self, page_id: PageId) {
        let frame_id = self.page_table.get(&page_id).unwrap();
        let page = &self.frames[frame_id.0];
        if page.is_dirty() {
            self.disk_manager.write_page(page_id, &(*page.get_data().read().unwrap()).0);
        }
    }

    fn new_page(&mut self, page_id: &mut PageId) -> Option<Data> {
        let victim_frame_id = self.alloc_frame()?;
        let new_page_id = self.alloc_page_id();
        let mut victim_page = &mut self.frames[victim_frame_id.0];
        if victim_page.is_dirty() {
            self.disk_manager
                .write_page(victim_page.get_page_id().unwrap(), &(*victim_page.get_data().read().unwrap()).0);
        }
        if let Some(victim_page_id) = victim_page
            .get_page_id() {
            self.page_table.remove(&victim_page_id);
        }
        self.page_table.insert(new_page_id, victim_frame_id);
        victim_page.set_page_id(new_page_id);
        victim_page.set_is_dirty(true);
        victim_page.set_pin_count(1);
        victim_page.reset_data();
        self.replacer.pin(victim_frame_id);

        *page_id = new_page_id;
        Some(victim_page.get_data())
    }

    fn delete_page(&mut self, page_id: PageId) {
        if let Some(frame_id) = self.page_table.get(&page_id) {
            if self.frames[frame_id.0].get_pin_count() > 0 {
                panic!(
                    "Attempt to delete a page with pin count > 0"
                );
            }
            self.free_list.push(*frame_id);
            self.page_table.remove(&page_id);
            self.deleted_page_ids.push(page_id.0);
        }
    }

    fn flush_all_pages(&mut self) {
        for page in self.frames.iter() {
            if page.is_dirty() {
                self.disk_manager
                    .write_page(page.get_page_id().unwrap(), &(*page.get_data().read().unwrap()).0);
            }
        }
    }
}

pub struct ParallelBufferPoolManager<R: Replacer, D: DiskManager> {
    num_instances: usize,
    pool_size: usize,
    instances: Vec<Arc<Mutex<BufferPoolManager<R, D>>>>,
    start_index: AtomicUsize,
}

impl<'a, R: Replacer, D: DiskManager> ParallelBufferPoolManager<R, D> {
    pub fn new(num_instances: usize, pool_size: usize, disk_manager: Arc<D>) -> Self {
        let mut instances = Vec::new();
        for i in 0..pool_size {
            instances.push(Arc::new(Mutex::new(BufferPoolManager::<R, D>::new(
                pool_size,
                num_instances,
                i,
                disk_manager.clone(),
            ))));
        }
        let start_index = AtomicUsize::new(0);
        Self {
            num_instances,
            pool_size,
            instances,
            start_index,
        }
    }

    fn get_instance(&self, page_id: PageId) -> Arc<Mutex<BufferPoolManager<R, D>>> {
        self.instances[(page_id.0 as usize % self.num_instances)].clone()
    }

    pub fn fetch_page_run<T>(&self, page_id: PageId, f: impl FnOnce(Data) -> T) -> Option<T> {
        self.get_instance(page_id)
            .lock()
            .unwrap()
            .fetch_page(page_id)
            .map(f)
    }

    pub fn unpin_page(&self, page_id: PageId, is_dirty: bool) {
        self.get_instance(page_id).lock().unwrap().unpin_page(page_id, is_dirty)
    }

    pub fn flush_page(&self, page_id: PageId) {
        self.get_instance(page_id).lock().unwrap().flush_page(page_id)
    }

    pub fn new_page_run<T>(&self, page_id: &mut PageId, f: impl FnOnce(Data) -> T) -> Option<T> {
        let (mut left, mut right) = self.instances.split_at(self.start_index.load(Ordering::Relaxed));
        let mut iter = right.iter().chain(left).enumerate();
        self.start_index.fetch_add(1, Ordering::Relaxed);
        for (i, instance) in iter {
            if let Some(page) = instance
                .try_lock()
                .ok()
                .as_mut()
                .and_then(|mut i| i.new_page(page_id))
            {
                self.start_index.store(i, Ordering::Relaxed);
                return Some(f(page));
            }
        }
        None
    }

    pub fn new_page(&self, page_id: &mut PageId) -> Option<Data> {
        self.get_instance(*page_id).lock().unwrap().new_page(page_id)
    }

    pub fn new_page_blocking(&self, page_id: &mut PageId) -> Data {
        loop {
            if let Some(page) = self.new_page(page_id) {
                return page;
            }
            thread::sleep(Duration::from_millis(1));
        }
    }

    pub fn fetch_page(&self, page_id: PageId) -> Option<Data> {
        self.get_instance(page_id).lock().unwrap().fetch_page(page_id)
    }

    pub fn delete_page(&self, page_id: PageId) {
        self.get_instance(page_id).lock().unwrap().delete_page(page_id)
    }

    pub fn flush_all_pages(&self) {
        for instance in self.instances.iter() {
            instance.lock().unwrap().flush_all_pages();
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn parallel_buffer_pool_test() {
        const NUM_THREADS: usize = 10;
        let disk_manager = Arc::new(DiskManagerInstance::new("test"));
        let pbpm = ParallelBufferPoolManager::<LRUReplacer, DiskManagerInstance>::new(
            5,
            10,
            disk_manager,
        );
        let pbpm = Arc::new(pbpm);
        for tid in 0..NUM_THREADS {
            let pbpm = pbpm.clone();
            std::thread::spawn(move || {
                let mut page_id = PageId(tid as u32);
                let page = pbpm.new_page(&mut page_id).unwrap();
                let mut page = page.write().unwrap();
                page.0[0] = page_id.0 as u8;
                pbpm.unpin_page(page_id, true);
            });
        }
        // std::fs::remove_file("test.db").unwrap();
    }
}

