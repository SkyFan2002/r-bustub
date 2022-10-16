use crate::storage::disk::disk_manager::PAGE_SIZE;
use bytemuck::{Pod, Zeroable};
use std::marker::PhantomData;
use std::mem::{size_of, transmute};

pub struct Tool<K, V>(PhantomData<(K, V)>);

pub enum InertResult {
    Success,
    Duplicate,
    Full,
}

impl<K, V> Tool<K, V> {
    pub(crate) const KV_NUM: usize = Self::BYTE_NUM * 8;
    pub(crate) const BYTE_NUM: usize = PAGE_SIZE / (8 * (size_of::<K>() + size_of::<V>()) + 1);
    pub(crate) const BLANK_SIZE: usize =
        PAGE_SIZE - (size_of::<K>() + size_of::<V>()) * Self::KV_NUM - Self::BYTE_NUM;
}
/*
8个键值对占的空间：8 *(key + value) + 2
最多可以储存的键值对的个数：PAGE_SIZE / (8 * (key + value) + 2) * 8
blank的大小：PAGE_SIZE - (key + value + 2) * 最多可以储存的键值对的个数
 */
#[derive(Debug, Clone, Copy)]
pub struct HashTableBucketPage<K, V>
where
    K: Default + Copy + PartialEq,
    V: Default + Copy + PartialEq,
    [(); Tool::<K, V>::KV_NUM]:,
    [(); Tool::<K, V>::BYTE_NUM]:,
    [(); Tool::<K, V>::BLANK_SIZE]:,
{
    readable: [u8; Tool::<K, V>::BYTE_NUM],
    kvs: [(K, V); Tool::<K, V>::KV_NUM],
    blank: [u8; Tool::<K, V>::BLANK_SIZE],
}

unsafe impl<K: 'static, V: 'static> Pod for HashTableBucketPage<K, V>
where
    K: Default + Copy + PartialEq,
    V: Default + Copy + PartialEq,
    [(); Tool::<K, V>::KV_NUM]:,
    [(); Tool::<K, V>::BYTE_NUM]:,
    [(); Tool::<K, V>::BLANK_SIZE]:,
{
}

unsafe impl<K, V> Zeroable for HashTableBucketPage<K, V>
where
    K: Default + Copy + PartialEq,
    V: Default + Copy + PartialEq,
    [(); Tool::<K, V>::KV_NUM]:,
    [(); Tool::<K, V>::BYTE_NUM]:,
    [(); Tool::<K, V>::BLANK_SIZE]:,
{
}

impl<K, V> HashTableBucketPage<K, V>
where
    K: Default + Copy + PartialEq,
    V: Default + Copy + PartialEq,
    [(); Tool::<K, V>::KV_NUM]:,
    [(); Tool::<K, V>::BYTE_NUM]:,
    [(); Tool::<K, V>::BLANK_SIZE]:,
{
    pub fn new() -> Self {
        Self {
            readable: [0u8; Tool::<K, V>::BYTE_NUM],
            kvs: [(K::default(), V::default()); Tool::<K, V>::KV_NUM],
            blank: [0u8; Tool::<K, V>::BLANK_SIZE],
        }
    }

    fn is_readable(&self, index: usize) -> bool {
        self.readable[index / 8] & (1 << (index % 8)) != 0
    }

    pub fn get_value(&self, key: &K) -> Vec<V> {
        let mut result = Vec::new();
        for i in 0..Tool::<K, V>::KV_NUM {
            if self.is_readable(i) && self.kvs[i].0 == *key {
                result.push(self.kvs[i].1);
            }
        }
        result
    }

    pub fn insert(&mut self, key: &K, value: &V) -> InertResult {
        let mut is_full = true;
        let mut first_empty_index = 0;
        for i in 0..Tool::<K, V>::KV_NUM {
            if self.is_readable(i) {
                if self.kvs[i].0 == *key && self.kvs[i].1 == *value {
                    return InertResult::Duplicate;
                }
            } else if is_full {
                is_full = false;
                first_empty_index = i;
            }
        }
        if is_full {
            return InertResult::Full;
        }
        self.kvs[first_empty_index] = (*key, *value);
        self.readable[first_empty_index / 8] |= 1 << (first_empty_index % 8);
        InertResult::Success
    }

    pub fn remove(&mut self, key: &K, value: &V) -> bool {
        for i in 0..Tool::<K, V>::KV_NUM {
            if self.is_readable(i) && self.kvs[i].0 == *key && self.kvs[i].1 == *value {
                self.readable[i / 8] &= !(1 << (i % 8));
                return true;
            }
        }
        false
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::storage::pages::page::Page;
    use bytemuck::cast_mut;
    use std::mem::transmute;

    #[test]
    fn test() {
        let page = HashTableBucketPage::<u64, u64>::new();
        println!("{}", page.kvs.len());
        println!("{}", page.readable.len());
        println!("{}", page.blank.len());
        println!("{}", size_of::<HashTableBucketPage::<u64, u64>>());
    }

    #[test]
    fn type_test() {
        let mut page = Page::new();
        let data = page.get_data();
        let mut data = data.write().unwrap();
        let hash_table_bucket_page: &mut HashTableBucketPage<u64, u64> = cast_mut(&mut **data);
    }
}
