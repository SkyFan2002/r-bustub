use crate::buffer::buffer_pool_manager::ParallelBufferPoolManager;
use crate::buffer::replacer::{PageId, Replacer};
use crate::storage::disk::disk_manager::DiskManager;
use crate::storage::pages::hash_table_bucket_page::{HashTableBucketPage, InertResult, Tool};
use crate::storage::pages::hash_table_directory_page::HashTableDirectoryPage;
use crate::storage::pages::page::{Data, Page};
use bytemuck::{cast_mut, cast_ref};
use std::collections::hash_map::DefaultHasher;
use std::hash::{BuildHasher, Hash, Hasher};
use std::marker::PhantomData;
use std::ptr::hash;
use std::sync::{Arc, Mutex};

struct EHTContext {
    dir_data: Data,
    bucket_data: Data,
    local_depth: u8,
    bucket_pid: PageId,
    bucket_index: usize,
}

pub struct ExtendibleHashTable<'a, R, D, K, V, H>
where
    R: Replacer,
    D: DiskManager,
    K: Hash,
    H: BuildHasher,
{
    dir_page_id: PageId,
    bpm: &'a ParallelBufferPoolManager<R, D>,
    hash_fn: H,
    phantom_data: PhantomData<(K, V)>,
}

impl<'a, R, D, K: 'static, V: 'static, H> ExtendibleHashTable<'a, R, D, K, V, H>
where
    R: Replacer,
    D: DiskManager,
    K: Hash + Default + Copy + PartialEq,
    H: BuildHasher,
    V: Default + Copy + PartialEq,
    [(); Tool::<K, V>::KV_NUM]:,
    [(); Tool::<K, V>::BYTE_NUM]:,
    [(); Tool::<K, V>::BLANK_SIZE]:,
{
    pub fn new(bpm: &'a ParallelBufferPoolManager<R, D>, hash_fn: H) -> Self {
        let mut dir_page_id = PageId(0);
        let mut dir_data = bpm.new_page_blocking(&mut dir_page_id);
        let mut dir_data = dir_data.write().unwrap();
        let dir: &mut HashTableDirectoryPage = cast_mut(&mut **dir_data);
        dir.set_page_id(dir_page_id);
        let mut bucket_page_id = PageId(0);
        let mut bucket_data = bpm.new_page_blocking(&mut bucket_page_id);
        dir.set_bucket_page_id(0, bucket_page_id);
        dir.set_local_depth(0, 0);
        bpm.unpin_page(dir_page_id, true);
        bpm.unpin_page(bucket_page_id, false);
        Self {
            dir_page_id,
            bpm,
            hash_fn,
            phantom_data: PhantomData,
        }
    }

    fn hash(&self, key: &K) -> u64 {
        let mut hasher = self.hash_fn.build_hasher();
        key.hash(&mut hasher);
        hasher.finish()
    }

    // You should call unpin_page the data is not needed anymore.
    fn pid_to_page_data(&self, page_id: PageId) -> Data {
        let mut data = self.bpm.fetch_page(page_id);
        while data.is_none() {
            data = self.bpm.fetch_page(page_id);
        }
        data.unwrap()
    }
    // You should call unpin_page the data is not needed anymore.
    fn get_dir_data(&self) -> Data {
        self.pid_to_page_data(self.dir_page_id)
    }
    // You should call unpin_page the data is not needed anymore.Twice,for both dir and bucket!!!
    fn get_context(&self, key: &K) -> EHTContext {
        let dir_data = self.get_dir_data();
        let dir_data_rd = dir_data.read().unwrap();
        let dir: &HashTableDirectoryPage = cast_ref(&**dir_data_rd);
        let bucket_index = self.key_to_index(key, dir_data.clone());
        let bucket_pid = dir.get_bucket_page_id(bucket_index as usize);
        EHTContext {
            dir_data: dir_data.clone(),
            bucket_data: self.pid_to_page_data(bucket_pid),
            local_depth: dir.get_local_depth(bucket_index as usize),
            bucket_pid,
            bucket_index: bucket_index as usize,
        }
    }

    fn key_to_index(&self, key: &K, dir_data: Data) -> u64 {
        let dir_data_rd = dir_data.read().unwrap();
        let dir: &HashTableDirectoryPage = cast_ref(&**dir_data_rd);
        let global_depth = dir.get_global_depth();
        let mask = (1 << global_depth) - 1;
        self.hash(key) & mask
    }

    pub fn get_value(&self, key: &K) -> Vec<V> {
        let context = self.get_context(key);
        let mut result = Vec::new();
        let bucket_data = context.bucket_data.read().unwrap();
        let bucket: &HashTableBucketPage<K, V> = cast_ref(&**bucket_data);
        result = bucket.get_value(key);
        self.bpm.unpin_page(self.dir_page_id, false);
        self.bpm.unpin_page(context.bucket_pid, false);
        result
    }

    pub fn insert(&mut self, key: &K, value: &V) -> bool {
        let context = self.get_context(key);
        let result = {
            let mut bucket_data = context.bucket_data.write().unwrap();
            let bucket: &mut HashTableBucketPage<K, V> = cast_mut(&mut **bucket_data);
            bucket.insert(key, value)
        };
        match result {
            InertResult::Success => {
                self.bpm.unpin_page(self.dir_page_id, false);
                self.bpm.unpin_page(context.bucket_pid, true);
                true
            }
            InertResult::Duplicate => {
                self.bpm.unpin_page(self.dir_page_id, false);
                self.bpm.unpin_page(context.bucket_pid, false);
                false
            }
            InertResult::Full => {
                self.bucket_split(key, value, &context);
                self.insert(key, value)
            }
        }
    }

    pub fn remove(&mut self, key: &K, value: &V) -> bool {
        let context = self.get_context(key);
        let mut bucket_data = context.bucket_data.write().unwrap();
        let bucket: &mut HashTableBucketPage<K, V> = cast_mut(&mut **bucket_data);
        if bucket.remove(key, value) {
            self.bpm.unpin_page(self.dir_page_id, false);
            self.bpm.unpin_page(context.bucket_pid, true);
            true
        } else {
            self.bpm.unpin_page(self.dir_page_id, false);
            self.bpm.unpin_page(context.bucket_pid, false);
            false
        }
    }

    fn get_global_depth(&self) -> u32 {
        let dir_data = self.get_dir_data();
        let dir_data = dir_data.read().unwrap();
        let dir: &HashTableDirectoryPage = cast_ref(&**dir_data);
        let global_depth = dir.get_global_depth();
        self.bpm.unpin_page(self.dir_page_id, false);
        global_depth
    }

    fn get_local_depth(&self, bucket_index: u64) -> u8 {
        let dir_data = self.get_dir_data();
        let dir_data = dir_data.read().unwrap();
        let dir: &HashTableDirectoryPage = cast_ref(&**dir_data);
        let local_depth = dir.get_local_depth(bucket_index as usize);
        self.bpm.unpin_page(self.dir_page_id, false);
        local_depth
    }

    fn bucket_split(&mut self, key: &K, value: &V, context: &EHTContext) {
        if context.local_depth == self.get_global_depth() as u8 {
            self.bucket_split_dir_double(key, value, context);
        } else {
            self.bucket_split_dir_same(key, value, context);
        }
    }

    fn bucket_split_dir_double(&mut self, key: &K, value: &V, context: &EHTContext) {
        let mut dir_data = context.dir_data.write().unwrap();
        let dir: &mut HashTableDirectoryPage = cast_mut(&mut **dir_data);
        dir.increase_global_depth();
        dir.increase_local_depth(context.bucket_index);
        let num_buckets_before = (1 << dir.get_global_depth()) / 2;
        for i in 0..num_buckets_before {
            dir.set_bucket_page_id(num_buckets_before + i, dir.get_bucket_page_id(i));
            dir.set_local_depth(num_buckets_before + i, dir.get_local_depth(i));
        }
        let mut new_page_id = PageId(0);
        let new_bucket_data = self.bpm.new_page_blocking(&mut new_page_id);
        let mut new_bucket_data = new_bucket_data.write().unwrap();
        let new_bucket: &mut HashTableBucketPage<K, V> = cast_mut(&mut **new_bucket_data);
        dir.set_bucket_page_id(context.bucket_index + num_buckets_before, new_page_id);
        dir.set_local_depth(
            context.bucket_index + num_buckets_before,
            context.local_depth,
        );
        for i in 0..Tool::<K, V>::KV_NUM {
            if self.key_to_index(key, context.dir_data.clone()) == context.bucket_index as u64 {
                continue;
            }
            new_bucket.insert(key, value);
            let mut bucket_data = context.bucket_data.write().unwrap();
            let bucket: &mut HashTableBucketPage<K, V> = cast_mut(&mut **bucket_data);
            bucket.remove(key, value);
        }
        self.bpm.unpin_page(self.dir_page_id, true);
        self.bpm.unpin_page(context.bucket_pid, true);
        self.bpm.unpin_page(new_page_id, true);
    }

    fn bucket_split_dir_same(&mut self, key: &K, value: &V, context: &EHTContext) {
        let cycle = 1 << context.local_depth;
        let index_in_place = if context.bucket_index < cycle {
            context.bucket_index
        } else {
            context.bucket_index - cycle
        };
        let mut dir_data = context.dir_data.write().unwrap();
        let dir: &mut HashTableDirectoryPage = cast_mut(&mut **dir_data);
        dir.increase_local_depth(context.bucket_index);
        let num_buckets = (1 << dir.get_global_depth()) / 2;
        let start = num_buckets / 2 + context.bucket_index % cycle;
        let mut new_page_id = PageId(0);
        let new_bucket_data = self.bpm.new_page_blocking(&mut new_page_id);
        let mut new_bucket_data = new_bucket_data.write().unwrap();
        let new_bucket: &mut HashTableBucketPage<K, V> = cast_mut(&mut **new_bucket_data);
        for i in (start..num_buckets).step_by(cycle) {
            dir.set_bucket_page_id(i, new_page_id);
            dir.set_local_depth(i, context.local_depth + 1);
        }
        for i in 0..Tool::<K, V>::KV_NUM {
            if self.key_to_index(key, context.dir_data.clone()) < (num_buckets / 2) as u64 {
                continue;
            }
            new_bucket.insert(key, value);
            let mut bucket_data = context.bucket_data.write().unwrap();
            let bucket: &mut HashTableBucketPage<K, V> = cast_mut(&mut **bucket_data);
            bucket.remove(key, value);
        }
        self.bpm.unpin_page(self.dir_page_id, true);
        self.bpm.unpin_page(context.bucket_pid, true);
        self.bpm.unpin_page(new_page_id, true);
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::buffer::replacer::LRUReplacer;
    use crate::storage::disk::disk_manager::DiskManagerInstance;
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, BuildHasherDefault};

    #[test]
    fn test() {
        let disk_manager = Arc::new(DiskManagerInstance::new("test"));
        let bpm = ParallelBufferPoolManager::new(5, 10, disk_manager);
        let hasher = RandomState::new();
        let mut eht =
            ExtendibleHashTable::<LRUReplacer, DiskManagerInstance, i32, i32, RandomState>::new(
                &bpm, hasher,
            );
        for i in 0..100 {
            eht.insert(&i, &(i + 1));
        }
        for i in 0..100 {
            assert_eq!(eht.get_value(&i), vec![i + 1]);
        }

        for i in 0..100 {
            eht.remove(&i, &(i + 1));
        }

        for i in 0..100 {
            assert_eq!(eht.get_value(&i), vec![]);
        }
    }

    #[test]
    fn test_insert() {
        let disk_manager = Arc::new(DiskManagerInstance::new("test"));
        let bpm = ParallelBufferPoolManager::new(5, 10, disk_manager);
        let hasher = RandomState::new();
        let mut eht =
            ExtendibleHashTable::<LRUReplacer, DiskManagerInstance, i32, i32, RandomState>::new(
                &bpm, hasher,
            );
        for i in 0..100 {
            eht.insert(&i, &(i + 1));
        }

        for i in 0..100 {
            eht.insert(&i, &(i));
        }

        for i in 0..100 {
            assert_eq!(eht.get_value(&i).len(), 2);
        }

        for i in 0..100 {
            eht.remove(&i, &(i));
        }

        for i in 0..100 {
            assert_eq!(eht.get_value(&i), vec![i + 1]);
        }
    }
}
