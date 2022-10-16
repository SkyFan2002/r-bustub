use crate::buffer::replacer::PageId;
use lazy_static::lazy_static;
use std::fs::File;
use std::io::Read;
use std::os::unix::fs::FileExt;
use std::os::unix::fs::OpenOptionsExt;
use std::sync::{Arc, RwLock};

extern crate libc;

pub const PAGE_SIZE: usize = 4096;

pub trait DiskManager {
    fn read_page(&self, page_id: PageId, page: &mut [u8; PAGE_SIZE]);
    fn write_page(&self, page_id: PageId, page: &[u8; PAGE_SIZE]);
}
#[derive(Debug)]
pub struct DiskManagerInstance {
    file: File,
}

impl DiskManager for DiskManagerInstance {
    fn read_page(&self, page_id: PageId, page: &mut [u8; PAGE_SIZE]) {
        self.file
            .read_at(page, page_id.0 as u64 * PAGE_SIZE as u64)
            .unwrap();
    }

    fn write_page(&self, page_id: PageId, page: &[u8; PAGE_SIZE]) {
        self.file
            .write_at(page, page_id.0 as u64 * PAGE_SIZE as u64)
            .unwrap();
    }
}

impl DiskManagerInstance {
    pub fn new(dbname: &str) -> Self {
        let file_name = format!("{}.db", dbname);
        let file = File::options()
            // .custom_flags(libc::O_DIRECT)
            .read(true)
            .write(true)
            .create(true)
            .open(file_name)
            .unwrap();
        Self { file }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use libc::{sleep, time};
    use std::sync::Mutex;
    use std::thread;
    use std::time::{Duration, Instant};

    #[test]
    fn disk_manager_instance_test() {
        let disk_manager = DiskManagerInstance::new("test");
        let page10 = [10u8; PAGE_SIZE];
        let page5 = [5u8; PAGE_SIZE];
        let mut buf = [0u8; PAGE_SIZE];
        disk_manager.write_page(PageId(10), &page10);
        disk_manager.write_page(PageId(5), &page5);
        disk_manager.read_page(PageId(10), &mut buf);
        assert_eq!(buf, page10);
        disk_manager.read_page(PageId(5), &mut buf);
        assert_eq!(buf, page5);
        std::fs::remove_file("test.db").unwrap();
    }

    #[test]
    fn disk_manager_instance_multi_thread_test_1() {
        let num_pages = 10;
        let mut write_threads = Vec::new();
        let disk_manager = Arc::new(DiskManagerInstance::new("testm1"));
        for i in 0..num_pages {
            let disk_manager_clone = Arc::clone(&disk_manager);
            write_threads.push(thread::spawn(move || {
                disk_manager_clone.write_page(PageId(i), &[i as u8; PAGE_SIZE]);
            }));
        }
        for thread in write_threads {
            thread.join();
        }
        std::fs::remove_file("testm1.db").unwrap();
    }

    #[test]
    fn disk_manager_instance_multi_thread_test() {
        let num_pages:usize = 10;
        let mut write_threads = Vec::new();
        let disk_manager = Arc::new(DiskManagerInstance::new("testm"));
        let mut pages = Arc::new(Mutex::new(Vec::new()));
        for i in 0..num_pages {
            pages.lock().unwrap().push([i as u8; PAGE_SIZE]);
        }
        for i in 0..num_pages {
            let disk_manager_clone = Arc::clone(&disk_manager);
            let pages_clone = Arc::clone(&pages);
            write_threads.push(thread::spawn(move || {
                disk_manager_clone.write_page(PageId(i as u32), &pages_clone.lock().unwrap()[i]);
            }));
        }
        for thread in write_threads {
            thread.join();
        }
        std::fs::remove_file("testm.db").unwrap();
    }

    #[test]
    fn disk_manager_instance_multi_thread_test_2() {
        let num_pages = 10;
        let mut write_threads = Vec::new();
        let disk_manager = Arc::new(DiskManagerInstance::new("testm2"));
        let mut pages = Vec::new();
        for i in 0..num_pages {
            pages.push(Arc::new(Mutex::new([i as u8; PAGE_SIZE])));
        }
        for i in 0..num_pages {
            let disk_manager_clone = Arc::clone(&disk_manager);
            let page_clone = Arc::clone(&pages[i]);
            write_threads.push(thread::spawn(move || {
                disk_manager_clone.write_page(PageId(i as u32), &page_clone.lock().unwrap());
            }));
        }
        for thread in write_threads {
            thread.join();
        }
        std::fs::remove_file("testm2.db").unwrap();
    }

    lazy_static! {
        static ref DISK_MANAGER: DiskManagerInstance = DiskManagerInstance::new("test_global");
    }

    lazy_static! {
        static ref PAGES: Vec<Mutex<[u8; PAGE_SIZE]>> = new_pages();
    }

    fn new_pages() -> Vec<Mutex<[u8; PAGE_SIZE]>> {
        let mut pages = Vec::new();
        for i in 0..10 {
            pages.push(Mutex::new([i as u8; PAGE_SIZE]));
        }
        pages
    }

    #[test]
    fn disk_manager_instance_multi_thread_test_3() {
        let num_pages:usize = 10;
        let mut write_threads = Vec::new();
        for i in 0..num_pages {
            write_threads.push(thread::spawn(move || {
                DISK_MANAGER.write_page(PageId(i as u32), &PAGES[i].lock().unwrap());
            }));
        }
        for thread in write_threads {
            thread.join();
        }
        std::fs::remove_file("test_global.db").unwrap();
    }

    #[test]
    fn disk_manager_instance_multi_thread_test_4() {
        let num_pages = 10;
        let disk_manager = DiskManagerInstance::new("testm4");
        let mut pages = Vec::new();
        for i in 0..num_pages {
            pages.push([i as u8; PAGE_SIZE]);
        }
        thread::scope(|s| {
            for (i, page) in pages.iter_mut().enumerate() {
                let disk_manager_ref = &disk_manager;
                s.spawn(move || {
                    disk_manager_ref.write_page(PageId(i as u32), page);
                });
            }
        });
        std::fs::remove_file("testm4.db").unwrap();
    }

    #[test]
    fn concurrent_read_write_test() {
        let num_pages = 1000;
        let mut write_threads = Vec::new();
        let disk_manager = Arc::new(DiskManagerInstance::new("test_5"));
        let start = Instant::now();
        for i in 0..4 {
            let disk_manager_clone = Arc::clone(&disk_manager);
            write_threads.push(thread::spawn(move || {
                for j in 0..10 {
                    disk_manager_clone.write_page(PageId(i), &[i as u8; PAGE_SIZE]);
                }
            }));
        }
        for thread in write_threads {
            thread.join();
        }
        let end = Instant::now();
        println!("concurrent write time: {:?}", end - start);
        std::fs::remove_file("test_5.db").unwrap();
    }

    #[test]
    fn read_write_test() {
        let num_pages = 4;
        let disk_manager = Arc::new(DiskManagerInstance::new("test_6"));
        let start = Instant::now();
        for j in 0..10 {
            for i in 0..num_pages {
                disk_manager.write_page(PageId(i), &[i as u8; PAGE_SIZE]);
            }
        }

        let end = Instant::now();
        println!("single thread write time: {:?}", end - start);
        let mut buf = [0u8; PAGE_SIZE];
        disk_manager.read_page(PageId(0), &mut buf);
        for data in buf {
            println!("{}", data);
        }
        std::fs::remove_file("test_6.db").unwrap();
    }
}
