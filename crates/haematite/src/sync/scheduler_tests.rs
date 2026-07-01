use std::error::Error;
use std::io;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use beamr::module::ModuleRegistry;
use beamr::scheduler::{Scheduler, SchedulerConfig};

use super::*;

const TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Default)]
struct RecordingTrigger {
    calls: Arc<Mutex<Vec<(SyncNodeId, ShardId)>>>,
}

impl RecordingTrigger {
    fn calls(&self) -> Vec<(SyncNodeId, ShardId)> {
        self.calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

impl SyncPullTrigger for RecordingTrigger {
    fn trigger_pull(&self, partner: &SyncNodeId, shard_id: ShardId) -> Result<(), String> {
        self.calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push((partner.clone(), shard_id));
        Ok(())
    }
}

fn beamr_scheduler() -> Result<Arc<Scheduler>, Box<dyn Error>> {
    Ok(Arc::new(Scheduler::new(
        SchedulerConfig::default(),
        Arc::new(ModuleRegistry::new()),
    )?))
}

fn nodes(count: usize) -> Vec<SyncNodeId> {
    (0..count).map(SyncNodeId::from).collect()
}

#[test]
fn run_once_uses_topology_targets_for_every_shard() -> Result<(), Box<dyn Error>> {
    let scheduler = beamr_scheduler()?;
    let trigger = RecordingTrigger::default();
    let config =
        SyncSchedulerConfig::new(1, nodes(4), SyncTopology::Ring, 2, Duration::from_secs(60));
    let handle = SyncSchedulerHandle::spawn(
        Arc::clone(&scheduler),
        config,
        Arc::new(trigger.clone()),
        TIMEOUT,
    )?;

    let stats = handle.run_once(TIMEOUT)?;

    assert_eq!(stats.partners, 2);
    assert_eq!(stats.shards, 2);
    assert_eq!(stats.operations_triggered, 4);
    assert_eq!(
        trigger.calls(),
        vec![
            (SyncNodeId::from(0), 0),
            (SyncNodeId::from(0), 1),
            (SyncNodeId::from(2), 0),
            (SyncNodeId::from(2), 1),
        ]
    );

    handle.shutdown(TIMEOUT)?;
    scheduler.shutdown();
    Ok(())
}

/// A fixed shard source returning a chosen subset — models the router's
/// materialised-shard membership under lazy materialisation.
#[derive(Clone)]
struct FixedShardSource {
    shards: Vec<ShardId>,
}

impl SyncShardSource for FixedShardSource {
    fn shards_to_sync(&self) -> Vec<ShardId> {
        self.shards.clone()
    }
}

/// LAZY: with a shard source that reports only the MATERIALISED shards, the
/// scheduler triggers pulls ONLY for those shards — an un-materialised shard
/// (which holds no data) is never touched, even though `shard_count` is large.
#[test]
fn run_once_syncs_only_shards_reported_by_the_source() -> Result<(), Box<dyn Error>> {
    let scheduler = beamr_scheduler()?;
    let trigger = RecordingTrigger::default();
    // shard_count is 4096, but only shards {5, 9} are materialised.
    let config = SyncSchedulerConfig::new(
        1,
        nodes(4),
        SyncTopology::Ring,
        4096,
        Duration::from_secs(60),
    );
    let source = Arc::new(FixedShardSource { shards: vec![5, 9] });
    let handle = SyncSchedulerHandle::spawn_with_shard_source(
        Arc::clone(&scheduler),
        config,
        Arc::new(trigger.clone()),
        source,
        TIMEOUT,
    )?;

    let stats = handle.run_once(TIMEOUT)?;

    // Two partners (Ring over 4 nodes) times two materialised shards — NOT 4096.
    assert_eq!(stats.partners, 2);
    assert_eq!(stats.shards, 2);
    assert_eq!(stats.operations_triggered, 4);
    assert_eq!(
        trigger.calls(),
        vec![
            (SyncNodeId::from(0), 5),
            (SyncNodeId::from(0), 9),
            (SyncNodeId::from(2), 5),
            (SyncNodeId::from(2), 9),
        ],
        "the scheduler must never trigger a pull for an un-materialised shard"
    );

    handle.shutdown(TIMEOUT)?;
    scheduler.shutdown();
    Ok(())
}

#[test]
fn periodic_tick_fires_sync_operations_at_configured_interval() -> Result<(), Box<dyn Error>> {
    let scheduler = beamr_scheduler()?;
    let trigger = RecordingTrigger::default();
    let config = SyncSchedulerConfig::new(
        "local",
        vec!["local".into(), "remote".into()],
        SyncTopology::FullMesh,
        1,
        Duration::from_millis(20),
    );
    let handle = SyncSchedulerHandle::spawn(
        Arc::clone(&scheduler),
        config,
        Arc::new(trigger.clone()),
        TIMEOUT,
    )?;

    let deadline = Instant::now() + TIMEOUT;
    while trigger.calls().is_empty() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }

    assert_eq!(trigger.calls(), vec![(SyncNodeId::from("remote"), 0)]);
    handle.shutdown(TIMEOUT)?;
    scheduler.shutdown();
    Ok(())
}

#[test]
fn supervisor_restarts_child_after_non_normal_exit() -> Result<(), Box<dyn Error>> {
    let scheduler = beamr_scheduler()?;
    let config = SyncSchedulerConfig::new(
        "local",
        vec!["local".into(), "remote".into()],
        SyncTopology::FullMesh,
        1,
        Duration::from_secs(60),
    );
    let handle = SyncSchedulerHandle::spawn(
        Arc::clone(&scheduler),
        config,
        Arc::new(NoopSyncPullTrigger),
        TIMEOUT,
    )?;

    let old_pid = handle.crash_child_for_test()?;
    let deadline = Instant::now() + TIMEOUT;
    let mut restarted_pid = None;
    while Instant::now() < deadline {
        match handle.pid() {
            Some(pid) if pid != old_pid => {
                restarted_pid = Some(pid);
                break;
            }
            Some(_) | None => std::thread::yield_now(),
        }
    }

    let new_pid = restarted_pid.ok_or_else(|| io::Error::other("child was not restarted"))?;
    assert_ne!(new_pid, old_pid);
    handle.shutdown(TIMEOUT)?;
    scheduler.shutdown();
    Ok(())
}

#[test]
fn invalid_scheduler_config_is_rejected() -> Result<(), Box<dyn Error>> {
    let scheduler = beamr_scheduler()?;
    let config = SyncSchedulerConfig::new(
        "missing",
        vec!["local".into(), "remote".into()],
        SyncTopology::FullMesh,
        1,
        Duration::from_secs(1),
    );

    assert!(matches!(
        SyncSchedulerHandle::spawn(
            Arc::clone(&scheduler),
            config,
            Arc::new(NoopSyncPullTrigger),
            TIMEOUT,
        ),
        Err(SyncSchedulerError::Topology(
            TopologyError::LocalNodeNotInTopology { .. }
        ))
    ));
    scheduler.shutdown();
    Ok(())
}
