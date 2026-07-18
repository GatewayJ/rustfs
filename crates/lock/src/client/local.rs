// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::RwLock;

use crate::{
    CommitFenceGuard, FastLockGuard, GlobalLockManager, LockClient, LockId, LockInfo, LockManager, LockMetadata, LockPriority,
    LockRequest, LockResponse, LockStats, LockStatus, LockType, Result, types::CommitFenceState,
};

/// Default shard count for guard storage (must be power of 2)
const DEFAULT_GUARD_SHARD_COUNT: usize = 64;

/// Local lock client using FastLock with sharded guard storage for better concurrency
#[derive(Debug)]
pub struct LocalClient {
    /// Sharded guard storage to reduce lock contention
    guard_storage: Vec<Arc<RwLock<HashMap<LockId, LocalGuardEntry>>>>,
    /// Mask for fast shard index calculation (shard_count - 1)
    shard_mask: usize,
    /// Optional lock manager (if None, uses global singleton)
    manager: Option<Arc<GlobalLockManager>>,
}

#[derive(Debug)]
struct LocalGuardEntry {
    guard: FastLockGuard,
    commit_fence: Arc<CommitFenceState>,
    expires_at: SystemTime,
    ttl: Duration,
    /// Owner recorded at acquire time; used only for reclaim diagnostics (#899).
    owner: String,
}

impl LocalGuardEntry {
    fn new(guard: FastLockGuard, ttl: Duration, owner: String) -> Self {
        let now = SystemTime::now();
        Self {
            guard,
            commit_fence: Arc::new(CommitFenceState::default()),
            expires_at: now + ttl,
            ttl,
            owner,
        }
    }

    fn is_expired(&self) -> bool {
        self.expires_at <= SystemTime::now()
    }

    fn refresh(&mut self) {
        self.expires_at = SystemTime::now() + self.ttl;
    }
}

impl LocalClient {
    /// Create new local client with default shard count
    pub fn new() -> Self {
        Self::with_shard_count(DEFAULT_GUARD_SHARD_COUNT)
    }

    /// Create new local client with custom shard count
    /// Shard count must be a power of 2 for efficient masking
    pub fn with_shard_count(shard_count: usize) -> Self {
        assert!(shard_count.is_power_of_two(), "Shard count must be power of 2");

        let guard_storage: Vec<Arc<RwLock<HashMap<LockId, LocalGuardEntry>>>> =
            (0..shard_count).map(|_| Arc::new(RwLock::new(HashMap::new()))).collect();

        Self {
            guard_storage,
            shard_mask: shard_count - 1,
            manager: None,
        }
    }

    /// Create new local client with a specific lock manager
    /// This allows simulating multi-node environments where each node has its own lock backend
    pub fn with_manager(manager: Arc<GlobalLockManager>) -> Self {
        Self {
            guard_storage: (0..DEFAULT_GUARD_SHARD_COUNT)
                .map(|_| Arc::new(RwLock::new(HashMap::new())))
                .collect(),
            shard_mask: DEFAULT_GUARD_SHARD_COUNT - 1,
            manager: Some(manager),
        }
    }

    /// Get the lock manager (injected manager if available, otherwise global singleton)
    pub fn get_lock_manager(&self) -> Arc<GlobalLockManager> {
        self.manager.clone().unwrap_or_else(crate::get_global_lock_manager)
    }

    /// Get the shard index for a given lock ID
    fn get_shard_index(&self, lock_id: &LockId) -> usize {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        lock_id.hash(&mut hasher);
        (hasher.finish() as usize) & self.shard_mask
    }

    /// Get the shard for a given lock ID
    fn get_shard(&self, lock_id: &LockId) -> &Arc<RwLock<HashMap<LockId, LocalGuardEntry>>> {
        let index = self.get_shard_index(lock_id);
        &self.guard_storage[index]
    }

    async fn reclaim_expired_guards_for_resource(&self, resource: &crate::ObjectKey) -> usize {
        let mut expired = Vec::new();
        for shard in &self.guard_storage {
            let guards = shard.read().await;
            expired.extend(
                guards
                    .iter()
                    .filter(|(lock_id, entry)| &lock_id.resource == resource && entry.is_expired())
                    .map(|(lock_id, entry)| (lock_id.clone(), entry.owner.clone(), entry.expires_at, entry.ttl)),
            );
        }

        let mut reclaimed = 0usize;
        for (lock_id, owner, expires_at, ttl) in expired {
            if self.remove_guard_after_fences(&lock_id, Some(expires_at)).await.is_some() {
                // An expired entry whose owner never refreshed it (a dead coordinator, #698) is
                // reclaimed so a live contender can re-form quorum. With guard heartbeats in place
                // (#899) a live owner keeps its entry from expiring, so reaching here means the
                // lease genuinely lapsed. Surface it for observability; the reclaim decision itself
                // is unchanged.
                let since_last_refresh = expires_at
                    .checked_sub(ttl)
                    .and_then(|last_refresh| SystemTime::now().duration_since(last_refresh).ok())
                    .unwrap_or(ttl);
                tracing::warn!(
                    owner = %owner,
                    resource = %resource,
                    ttl_ms = u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX),
                    since_last_refresh_ms = since_last_refresh.as_millis() as u64,
                    "reclaiming expired lock guard whose lease was not refreshed"
                );
                rustfs_io_metrics::record_lock_reclaimed();
                reclaimed = reclaimed.saturating_add(1);
            }
        }

        reclaimed
    }

    async fn remove_guard_after_fences(
        &self,
        lock_id: &LockId,
        expected_expired_at: Option<SystemTime>,
    ) -> Option<LocalGuardEntry> {
        let fence = {
            let guards = self.get_shard(lock_id).write().await;
            let entry = guards.get(lock_id)?;
            if let Some(expected_expired_at) = expected_expired_at
                && (entry.expires_at != expected_expired_at || !entry.is_expired())
            {
                return None;
            }
            entry.commit_fence.close();
            Arc::clone(&entry.commit_fence)
        };
        fence.wait_until_idle().await;
        let mut guards = self.get_shard(lock_id).write().await;
        if guards
            .get(lock_id)
            .is_some_and(|entry| Arc::ptr_eq(&entry.commit_fence, &fence))
        {
            guards.remove(lock_id)
        } else {
            None
        }
    }
}

impl Default for LocalClient {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl LockClient for LocalClient {
    async fn acquire_lock(&self, request: &LockRequest) -> Result<LockResponse> {
        let lock_manager = self.get_lock_manager();
        let reclaimed_before_acquire = self.reclaim_expired_guards_for_resource(&request.resource).await;

        let build_lock_request = || match request.lock_type {
            LockType::Exclusive => crate::ObjectLockRequest::new_write(request.resource.clone(), request.owner.clone())
                .with_acquire_timeout(request.acquire_timeout),
            LockType::Shared => crate::ObjectLockRequest::new_read(request.resource.clone(), request.owner.clone())
                .with_acquire_timeout(request.acquire_timeout),
        };

        let mut retried_after_reclaim = reclaimed_before_acquire > 0;
        loop {
            match lock_manager.acquire_lock(build_lock_request()).await {
                Ok(guard) => {
                    let lock_id = request.lock_id.clone();
                    let acquired_at = SystemTime::now();
                    let expires_at = acquired_at + request.ttl;

                    {
                        let shard = self.get_shard(&lock_id);
                        let mut guards = shard.write().await;
                        guards.insert(lock_id.clone(), LocalGuardEntry::new(guard, request.ttl, request.owner.clone()));
                    }

                    let lock_info = LockInfo {
                        id: lock_id,
                        resource: request.resource.clone(),
                        lock_type: request.lock_type,
                        status: crate::types::LockStatus::Acquired,
                        owner: request.owner.clone(),
                        acquired_at,
                        expires_at,
                        last_refreshed: acquired_at,
                        metadata: request.metadata.clone(),
                        priority: request.priority,
                        wait_start_time: None,
                    };
                    return Ok(LockResponse::success(lock_info, Duration::ZERO));
                }
                Err(crate::fast_lock::LockResult::Timeout) => {
                    if !retried_after_reclaim && self.reclaim_expired_guards_for_resource(&request.resource).await > 0 {
                        retried_after_reclaim = true;
                        continue;
                    }
                    return Ok(LockResponse::failure("Lock acquisition timeout", request.acquire_timeout));
                }
                Err(crate::fast_lock::LockResult::Conflict {
                    current_owner,
                    current_mode,
                }) => {
                    if !retried_after_reclaim && self.reclaim_expired_guards_for_resource(&request.resource).await > 0 {
                        retried_after_reclaim = true;
                        continue;
                    }
                    return Ok(LockResponse::failure(
                        format!("Lock conflict: resource held by {current_owner} in {current_mode:?} mode"),
                        Duration::ZERO,
                    ));
                }
                Err(crate::fast_lock::LockResult::Acquired) => {
                    unreachable!("Acquired should not be an error")
                }
            }
        }
    }

    async fn release(&self, lock_id: &LockId) -> Result<bool> {
        if let Some(guard) = self.remove_guard_after_fences(lock_id, None).await {
            // Guard automatically releases the lock when dropped
            drop(guard.guard);
            Ok(true)
        } else {
            // Lock not found or already released
            Ok(false)
        }
    }

    async fn refresh(&self, lock_id: &LockId) -> Result<bool> {
        let shard = self.get_shard(lock_id);
        let mut guards = shard.write().await;
        if let Some(entry) = guards.get_mut(lock_id) {
            if entry.commit_fence.is_closing() || entry.is_expired() {
                return Ok(false);
            }
            entry.refresh();
            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn refresh_locks_batch(&self, lock_ids: &[LockId]) -> Result<Vec<bool>> {
        let mut results = Vec::with_capacity(lock_ids.len());
        for lock_id in lock_ids {
            results.push(self.refresh(lock_id).await?);
        }
        Ok(results)
    }

    async fn force_release(&self, lock_id: &LockId) -> Result<bool> {
        self.release(lock_id).await
    }

    async fn check_status(&self, lock_id: &LockId) -> Result<Option<LockInfo>> {
        let shard = self.get_shard(lock_id);
        let guards = shard.read().await;
        if let Some(entry) = guards.get(lock_id) {
            // We have an active guard for this lock
            let lock_type = match entry.guard.mode() {
                crate::LockMode::Shared => LockType::Shared,
                crate::LockMode::Exclusive => LockType::Exclusive,
            };
            let status = if entry.is_expired() {
                LockStatus::Expired
            } else {
                LockStatus::Acquired
            };
            Ok(Some(LockInfo {
                id: lock_id.clone(),
                resource: lock_id.resource.clone(),
                lock_type,
                status,
                owner: entry.guard.owner().to_string(),
                acquired_at: SystemTime::now(),
                expires_at: entry.expires_at,
                last_refreshed: SystemTime::now(),
                metadata: LockMetadata::default(),
                priority: LockPriority::Normal,
                wait_start_time: None,
            }))
        } else {
            Ok(None)
        }
    }

    async fn acquire_commit_fence(&self, lock_id: &LockId) -> Result<Option<CommitFenceGuard>> {
        let guards = self.get_shard(lock_id).read().await;
        let Some(entry) = guards.get(lock_id) else {
            return Ok(None);
        };
        if entry.is_expired() || entry.guard.mode() != crate::LockMode::Exclusive {
            return Ok(None);
        }
        Ok(entry.commit_fence.try_enter())
    }

    async fn get_stats(&self) -> Result<LockStats> {
        Ok(LockStats::default())
    }

    async fn close(&self) -> Result<()> {
        Ok(())
    }

    async fn is_online(&self) -> bool {
        true
    }

    async fn is_local(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::LocalClient;
    use crate::{LockClient, LockRequest, LockType, ObjectKey};
    use std::{sync::Arc, time::Duration};

    #[tokio::test]
    async fn release_waits_for_active_commit_fence() {
        let client = Arc::new(LocalClient::new());
        let request =
            LockRequest::new(ObjectKey::new("bucket", "object"), LockType::Exclusive, "owner").with_ttl(Duration::from_secs(30));
        let lock_id = client
            .acquire_lock(&request)
            .await
            .expect("write lock should be acquired")
            .lock_info
            .expect("successful lock should include lock info")
            .id;
        let commit_fence = client
            .acquire_commit_fence(&lock_id)
            .await
            .expect("commit fence lookup should succeed")
            .expect("active exclusive lock should admit a commit fence");

        let release_client = Arc::clone(&client);
        let release_lock_id = lock_id.clone();
        let release = tokio::spawn(async move { release_client.release(&release_lock_id).await });

        loop {
            let probe = client
                .acquire_commit_fence(&lock_id)
                .await
                .expect("commit fence probe should succeed");
            if probe.is_none() {
                break;
            }
            drop(probe);
            tokio::task::yield_now().await;
        }
        assert!(!release.is_finished(), "lock release must wait for the active storage commit");

        drop(commit_fence);
        assert!(
            release
                .await
                .expect("release task should not panic")
                .expect("release should succeed"),
            "the held lock should be released after the commit fence exits"
        );
        assert!(
            client
                .acquire_commit_fence(&lock_id)
                .await
                .expect("post-release commit fence lookup should succeed")
                .is_none(),
            "a released lock must not admit another commit"
        );
    }

    #[tokio::test]
    async fn expired_guard_cannot_be_revived_by_a_late_refresh() {
        let client = LocalClient::new();
        let request = LockRequest::new(ObjectKey::new("bucket", "object"), LockType::Exclusive, "owner")
            .with_ttl(Duration::from_millis(10));
        let lock_id = client
            .acquire_lock(&request)
            .await
            .expect("write lock should be acquired")
            .lock_info
            .expect("successful lock should include lock info")
            .id;
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(
            !client.refresh(&lock_id).await.expect("refresh should complete"),
            "an expired lease must not be revived by a delayed heartbeat"
        );
        assert!(
            client
                .acquire_commit_fence(&lock_id)
                .await
                .expect("commit fence lookup should succeed")
                .is_none(),
            "an expired lease must not admit a storage commit"
        );
        assert!(client.release(&lock_id).await.expect("release should succeed"));
    }
}
