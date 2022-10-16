use std::collections::linked_list::CursorMut;
use std::collections::LinkedList;

#[derive(Clone, Copy, PartialEq, Debug)]
pub struct FrameId(pub(crate) usize);

#[derive(Clone, Copy, PartialEq, Debug, Eq, Hash,Default)]
pub struct PageId(pub(crate) u32);

pub trait Replacer {
    fn new(pool_size: usize) -> Self;
    fn victim(&mut self) -> Option<FrameId>;

    fn pin(&mut self, frame_id: FrameId);

    fn unpin(&mut self, frame_id: FrameId);

    fn size(&self) -> usize;
}

#[derive(Debug)]
pub struct LRUReplacer {
    container: LinkedList<FrameId>,
    index: Vec<Option<CursorMut<'static, FrameId>>>,
}

impl Replacer for LRUReplacer {
    fn new(pool_size: usize) -> Self {
        let mut index = Vec::new();
        index.resize_with(pool_size, || None);
        LRUReplacer {
            container: LinkedList::new(),
            index,
        }
    }
    //pop front
    fn victim(&mut self) -> Option<FrameId> {
        let frame_id = self.container.pop_front()?;
        debug_assert!(self.index[frame_id.0].is_some());
        self.index[frame_id.0] = None;
        Some(frame_id)
    }

    fn pin(&mut self, frame_id: FrameId) {
        // delete frame_id
        if let Some(mut cursor) = self.index[frame_id.0].take() {
            cursor.remove_current().unwrap();
        }
    }

    fn unpin(&mut self, frame_id: FrameId) {
        //push back
        debug_assert!(self.index[frame_id.0].is_none());
        self.container.push_back(frame_id);
        self.index[frame_id.0] =
            Some(unsafe { core::mem::transmute(self.container.cursor_back_mut()) });
    }

    fn size(&self) -> usize {
        self.container.len()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn replacer_test() {
        let mut replacer = LRUReplacer::new(10);
        for i in 0..10 {
            replacer.unpin(FrameId(i));
        }
        replacer.pin(FrameId(5));
        assert_eq!(replacer.victim(), Some(FrameId(0)));
        assert_eq!(replacer.victim(), Some(FrameId(1)));
        assert_eq!(replacer.victim(), Some(FrameId(2)));
        assert_eq!(replacer.victim(), Some(FrameId(3)));
        assert_eq!(replacer.victim(), Some(FrameId(4)));
        assert_eq!(replacer.victim(), Some(FrameId(6)));
        assert_eq!(replacer.victim(), Some(FrameId(7)));
        assert_eq!(replacer.victim(), Some(FrameId(8)));
        assert_eq!(replacer.victim(), Some(FrameId(9)));
    }

    #[test]
    fn sample_test() {
        let mut replacer = LRUReplacer::new(7);
        replacer.unpin(FrameId(1));
        replacer.unpin(FrameId(2));
        replacer.unpin(FrameId(3));
        replacer.unpin(FrameId(4));
        replacer.unpin(FrameId(5));
        replacer.unpin(FrameId(6));
        assert_eq!(replacer.size(), 6);
    }
}
