use std::collections::BTreeMap;
use std::io;
use std::path::Path;

use codex_infra_protocol::TaskError;
use codex_infra_protocol::TaskId;
use codex_infra_protocol::TaskRecord;

use crate::Journal;

#[derive(Debug, thiserror::Error)]
pub enum TaskStoreError {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Transition(#[from] TaskError),
    #[error("unknown task {0}")]
    Missing(TaskId),
    #[error("task already exists: {0}")]
    Exists(TaskId),
    #[error("task transition must preserve identity and increment revision once")]
    Revision,
}

/// Machine runtime's serial task writer; snapshots in the journal retain every
/// confirmed revision while this index holds the current records.
pub struct TaskStore {
    journal: Journal,
    records: BTreeMap<TaskId, TaskRecord>,
}

impl TaskStore {
    pub fn open(path: &Path) -> Result<Self, TaskStoreError> {
        let mut records: BTreeMap<TaskId, TaskRecord> = BTreeMap::new();
        let journal = Journal::open(path, |entry| {
            let record: TaskRecord =
                serde_json::from_slice(&entry.payload).map_err(io::Error::other)?;
            let expected = match records.get(&record.task_id()) {
                Some(previous) => previous.revision().checked_add(1),
                None => Some(0),
            };
            if Some(record.revision()) != expected {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "task journal revision",
                ));
            }
            records.insert(record.task_id(), record);
            Ok(())
        })?;
        Ok(Self { journal, records })
    }

    pub fn get(&self, task_id: TaskId) -> Option<&TaskRecord> {
        self.records.get(&task_id)
    }

    pub fn records(&self) -> impl Iterator<Item = &TaskRecord> {
        self.records.values()
    }

    pub fn create(&mut self, record: TaskRecord) -> Result<u64, TaskStoreError> {
        if self.records.contains_key(&record.task_id()) {
            return Err(TaskStoreError::Exists(record.task_id()));
        }
        if record.revision() != 0 {
            return Err(TaskStoreError::Revision);
        }
        let sequence = self.journal.append(&serde_json::to_vec(&record)?)?;
        self.records.insert(record.task_id(), record);
        Ok(sequence)
    }

    pub fn update(
        &mut self,
        task_id: TaskId,
        transition: impl FnOnce(&mut TaskRecord) -> Result<(), TaskError>,
    ) -> Result<u64, TaskStoreError> {
        let previous = self
            .records
            .get(&task_id)
            .ok_or(TaskStoreError::Missing(task_id))?;
        let mut next = previous.clone();
        transition(&mut next)?;
        if next.task_id() != task_id || Some(next.revision()) != previous.revision().checked_add(1)
        {
            return Err(TaskStoreError::Revision);
        }
        let sequence = self.journal.append(&serde_json::to_vec(&next)?)?;
        self.records.insert(task_id, next);
        Ok(sequence)
    }
}
