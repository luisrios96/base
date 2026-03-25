//! Sync target with trie data cache for the background sync loop.
//!
//! Buffers trie data from exex notifications so the sync loop can use
//! pre-computed data even when it is many blocks behind the chain tip.

use std::{collections::BTreeMap, sync::Mutex};

use alloy_eips::eip1898::BlockWithParent;
use reth_trie::LazyTrieData;
use tokio::sync::Notify;
use tracing::debug;

/// Maximum number of blocks to cache trie data for.
const CACHE_CAPACITY: usize = 1024;

/// Cached trie data for a single block.
#[derive(Debug)]
pub struct CachedBlockTrieData {
    /// The block identifier with its parent hash.
    pub block_with_parent: BlockWithParent,
    /// The lazy trie data (hashed state + trie updates).
    pub trie_data: LazyTrieData,
}

/// Sync target that buffers trie data from recent exex notifications.
///
/// A `watch::channel` overwrites previous values, discarding trie data from
/// older notifications when new ones arrive. When the sync loop is hundreds of
/// blocks behind the tip, it would always fall back to re-executing blocks
/// because the trie data was gone.
///
/// This struct accumulates trie data in a bounded [`BTreeMap`] so the sync loop
/// can still use pre-computed trie data for blocks from earlier notifications.
pub struct SyncTarget {
    inner: Mutex<SyncTargetInner>,
    notify: Notify,
}

struct SyncTargetInner {
    target: u64,
    cache: BTreeMap<u64, CachedBlockTrieData>,
}

impl SyncTarget {
    /// Create a new `SyncTarget` with no cached data and target 0.
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(SyncTargetInner { target: 0, cache: BTreeMap::new() }),
            notify: Notify::new(),
        }
    }

    /// Set the sync target block number and wake the sync loop.
    ///
    /// Only advances the target forward; ignored if `target` is not greater
    /// than the current value.
    pub fn set_target(&self, target: u64) {
        let mut inner = self.inner.lock().expect("SyncTarget lock poisoned");
        if target > inner.target {
            let prev = inner.target;
            inner.target = target;
            let cached = inner.cache.len();
            drop(inner);
            debug!(
                target: "base::exex::sync_target",
                prev_target = prev,
                new_target = target,
                cached_blocks = cached,
                "Sync target advanced"
            );
            self.notify.notify_waiters();
        }
    }

    /// Insert cached trie data for a block.
    ///
    /// Evicts the oldest entries when the cache exceeds capacity.
    pub fn insert(&self, block_number: u64, data: CachedBlockTrieData) {
        let mut inner = self.inner.lock().expect("SyncTarget lock poisoned");
        inner.cache.insert(block_number, data);
        let mut evicted = 0u64;
        while inner.cache.len() > CACHE_CAPACITY {
            inner.cache.pop_first();
            evicted += 1;
        }
        if evicted > 0 {
            debug!(
                target: "base::exex::sync_target",
                block_number,
                evicted,
                "Cache full, evicted oldest entries"
            );
        }
        debug!(
            target: "base::exex::sync_target",
            block_number,
            cached_blocks = inner.cache.len(),
            "Cached trie data for block"
        );
    }

    /// Get the current sync target block number.
    pub fn target(&self) -> u64 {
        self.inner.lock().expect("SyncTarget lock poisoned").target
    }

    /// Take cached trie data for a specific block, removing it from the cache.
    pub fn take(&self, block_number: u64) -> Option<CachedBlockTrieData> {
        let result = self.inner.lock().expect("SyncTarget lock poisoned").cache.remove(&block_number);
        if result.is_some() {
            debug!(
                target: "base::exex::sync_target",
                block_number,
                "Cache hit: trie data found for block"
            );
        } else {
            debug!(
                target: "base::exex::sync_target",
                block_number,
                "Cache miss: no trie data for block, will re-execute"
            );
        }
        result
    }

    /// Wait until the target changes.
    pub async fn changed(&self) {
        self.notify.notified().await;
    }
}

impl std::fmt::Debug for SyncTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner = self.inner.lock().expect("SyncTarget lock poisoned");
        f.debug_struct("SyncTarget")
            .field("target", &inner.target)
            .field("cached_blocks", &inner.cache.len())
            .finish()
    }
}
