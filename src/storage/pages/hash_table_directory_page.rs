use std::mem::size_of;
use bytemuck::{Pod, Zeroable};
use crate::buffer::replacer::PageId;
use crate::storage::disk::disk_manager::PAGE_SIZE;

const DIRECTORY_ARRAY_SIZE: usize = 512;

const BLANK_SIZE: usize = PAGE_SIZE - size_of::<PageId>() - size_of::<u32>() - size_of::<u8>() * DIRECTORY_ARRAY_SIZE - size_of::<PageId>() * DIRECTORY_ARRAY_SIZE;

#[derive(Debug, Clone, Copy)]
pub struct HashTableDirectoryPage {
    page_id: PageId,
    // 4 byte
    global_depth: u32,
    // 4 byte
    local_depth: [u8; DIRECTORY_ARRAY_SIZE],
    // 512 bytes
    page_ids: [PageId; DIRECTORY_ARRAY_SIZE],
    // 4 * 512 = 2048 bytes
    blank: [u8; BLANK_SIZE],
}

unsafe impl Zeroable for HashTableDirectoryPage {}

unsafe impl Pod for HashTableDirectoryPage {}


impl HashTableDirectoryPage {
    pub fn get_page_id(&self) -> PageId {
        self.page_id
    }


    pub fn get_global_depth(&self) -> u32 {
        self.global_depth
    }

    pub fn get_local_depth(&self, index: usize) -> u8 {
        self.local_depth[index]
    }

    pub fn set_global_depth(&mut self, global_depth: u32) {
        self.global_depth = global_depth;
    }

    pub fn set_local_depth(&mut self, index: usize, local_depth: u8) {
        self.local_depth[index] = local_depth;
    }

    pub fn set_page_id(&mut self, page_id: PageId) {
        self.page_id = page_id;
    }

    pub fn get_bucket_page_id(&self, index: usize) -> PageId {
        self.page_ids[index]
    }

    pub fn set_bucket_page_id(&mut self, index: usize, page_id: PageId) {
        self.page_ids[index] = page_id;
    }

    pub fn increase_global_depth(&mut self) {
        self.global_depth += 1;
    }

    pub fn increase_local_depth(&mut self, bucket_index: usize) {
        self.local_depth[bucket_index] += 1;
    }
}

#[cfg(test)]
mod test {
    use std::alloc::System;
    use std::mem::{transmute, transmute_copy};
    use std::ops::{Deref, DerefMut};
    use bytemuck::{cast_mut, cast_slice, cast_slice_mut, from_bytes_mut};
    use crate::storage::pages::page::Page;
    use super::*;

    #[test]
    fn test_hash_table_directory_page() {
        let mut page = Page::new();
        let data = page.get_data();
        let mut data = data.write().unwrap();
        let hash_table_directory_page: &mut HashTableDirectoryPage = cast_mut(&mut data.0);
        hash_table_directory_page.set_global_depth(1);
        hash_table_directory_page.set_local_depth(0, 1);
        hash_table_directory_page.set_local_depth(1, 1);
        hash_table_directory_page.set_page_id(PageId(1));
        hash_table_directory_page.set_bucket_page_id(0, PageId(2));
        assert_eq!(hash_table_directory_page.get_global_depth(), 1);
        assert_eq!(hash_table_directory_page.get_local_depth(0), 1);
        assert_eq!(hash_table_directory_page.get_local_depth(1), 1);
        assert_eq!(hash_table_directory_page.get_page_id(), PageId(1));
        assert_eq!(hash_table_directory_page.get_bucket_page_id(0), PageId(2));
        let hash_table_directory_page_1: &mut HashTableDirectoryPage = cast_mut(&mut **data);
        assert_eq!(hash_table_directory_page_1.get_global_depth(), 1);
        assert_eq!(hash_table_directory_page_1.get_local_depth(0), 1);
        assert_eq!(hash_table_directory_page_1.get_local_depth(1), 1);
        assert_eq!(hash_table_directory_page_1.get_page_id(), PageId(1));
        assert_eq!(hash_table_directory_page_1.get_bucket_page_id(0), PageId(2));
        let align = core::mem::align_of::<HashTableDirectoryPage>();
        println!("align: {}", align);
        // unsafe{
        //     let hash_table_directory_page: &mut HashTableDirectoryPage = transmute(&mut **data);
        //     hash_table_directory_page.set_global_depth(1);
        //     hash_table_directory_page.set_local_depth(0, 1);
        //     hash_table_directory_page.set_local_depth(1, 1);
        //     hash_table_directory_page.set_page_id(PageId(1));
        //     hash_table_directory_page.set_bucket_page_id(0, PageId(2));
        //     assert_eq!(hash_table_directory_page.get_global_depth(), 1);
        //     assert_eq!(hash_table_directory_page.get_local_depth(0), 1);
        //     assert_eq!(hash_table_directory_page.get_local_depth(1), 1);
        //     assert_eq!(hash_table_directory_page.get_page_id(), PageId(1));
        //     assert_eq!(hash_table_directory_page.get_bucket_page_id(0), PageId(2));
        //     let hash_table_directory_page_1: &mut HashTableDirectoryPage = transmute(&mut **data);
        //     assert_eq!(hash_table_directory_page_1.get_global_depth(), 1);
        //     assert_eq!(hash_table_directory_page_1.get_local_depth(0), 1);
        //     assert_eq!(hash_table_directory_page_1.get_local_depth(1), 1);
        //     assert_eq!(hash_table_directory_page_1.get_page_id(), PageId(1));
        //     assert_eq!(hash_table_directory_page_1.get_bucket_page_id(0), PageId(2));
        // }
    }
}