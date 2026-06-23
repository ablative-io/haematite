// CORE-008/CORE-009: Shard router — stable hash-based key-to-shard mapping.

use crate::shard::actor::ShardHandle;

/// Private database router over shard handles in shard-index order.
#[derive(Clone, Debug)]
pub struct ShardRouter {
    handles: Vec<ShardHandle>,
}

impl ShardRouter {
    pub(crate) fn new(handles: Vec<ShardHandle>) -> Option<Self> {
        if handles.is_empty() {
            None
        } else {
            Some(Self { handles })
        }
    }

    pub(crate) fn shard_for(&self, key: &[u8]) -> usize {
        let digest = blake3::hash(key);
        let mut prefix = [0_u8; 8];
        for (target, source) in prefix.iter_mut().zip(digest.as_bytes().iter()) {
            *target = *source;
        }
        let value = u64::from_be_bytes(prefix);
        (value % self.handles.len() as u64) as usize
    }

    pub(crate) fn handle_for(&self, key: &[u8]) -> Option<&ShardHandle> {
        self.handles.get(self.shard_for(key))
    }

    pub(crate) fn handles_in_order(&self) -> &[ShardHandle] {
        &self.handles
    }
}
