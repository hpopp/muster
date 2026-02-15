//! Storage trait and default redb implementation.

use redb::{ReadableTable, TableDefinition};
use std::sync::Arc;

/// Replication log: sequence_number -> operation bytes (msgpack)
const REPLICATION_LOG: TableDefinition<u64, &[u8]> =
    TableDefinition::new("_muster_replication_log");

/// Node state: "state" -> NodeState bytes (msgpack)
const NODE_META: TableDefinition<&str, &[u8]> = TableDefinition::new("_muster_node_meta");

// ============================================================================
// Storage trait
// ============================================================================

/// Byte-oriented storage backend for muster's internal state.
///
/// Muster handles all serialization — implementations just store and retrieve
/// raw bytes. This decouples muster from any specific storage engine.
pub trait Storage: Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;

    /// Append a log entry at a specific sequence number.
    fn append_log_entry(&self, sequence: u64, data: &[u8]) -> Result<(), Self::Error>;

    /// Atomically get the next sequence number and append a log entry.
    /// Returns the new sequence number.
    fn append_next_log_entry(&self, data: &[u8]) -> Result<u64, Self::Error>;

    /// Read log entries from `from_sequence` (inclusive), ordered by sequence.
    fn read_log_from(&self, from_sequence: u64) -> Result<Vec<(u64, Vec<u8>)>, Self::Error>;

    /// Get the latest sequence number (0 if no entries).
    fn latest_sequence(&self) -> Result<u64, Self::Error>;

    /// Get the persisted node state (None if not yet written).
    fn get_node_state(&self) -> Result<Option<Vec<u8>>, Self::Error>;

    /// Persist the node state.
    fn put_node_state(&self, data: &[u8]) -> Result<(), Self::Error>;
}

// ============================================================================
// RedbStorage — default implementation
// ============================================================================

/// Default `Storage` implementation backed by redb.
///
/// Uses `_muster_`-prefixed table names to avoid collisions when sharing
/// a redb instance with the consumer's application tables.
#[derive(Debug, Clone)]
pub struct RedbStorage {
    db: Arc<redb::Database>,
}

/// Errors from the redb storage backend.
#[derive(Debug, thiserror::Error)]
pub enum RedbStorageError {
    #[error("Commit error: {0}")]
    Commit(Box<redb::CommitError>),
    #[error("Database error: {0}")]
    Database(Box<redb::DatabaseError>),
    #[error("Redb error: {0}")]
    Redb(Box<redb::Error>),
    #[error("Storage error: {0}")]
    Storage(Box<redb::StorageError>),
    #[error("Table error: {0}")]
    Table(Box<redb::TableError>),
    #[error("Transaction error: {0}")]
    Transaction(Box<redb::TransactionError>),
}

impl From<redb::CommitError> for RedbStorageError {
    fn from(e: redb::CommitError) -> Self {
        Self::Commit(Box::new(e))
    }
}

impl From<redb::DatabaseError> for RedbStorageError {
    fn from(e: redb::DatabaseError) -> Self {
        Self::Database(Box::new(e))
    }
}

impl From<redb::Error> for RedbStorageError {
    fn from(e: redb::Error) -> Self {
        Self::Redb(Box::new(e))
    }
}

impl From<redb::StorageError> for RedbStorageError {
    fn from(e: redb::StorageError) -> Self {
        Self::Storage(Box::new(e))
    }
}

impl From<redb::TableError> for RedbStorageError {
    fn from(e: redb::TableError) -> Self {
        Self::Table(Box::new(e))
    }
}

impl From<redb::TransactionError> for RedbStorageError {
    fn from(e: redb::TransactionError) -> Self {
        Self::Transaction(Box::new(e))
    }
}

impl RedbStorage {
    /// Create a new `RedbStorage` from a shared redb instance.
    ///
    /// Initializes the `_muster_replication_log` and `_muster_node_meta` tables
    /// if they don't already exist.
    pub fn new(db: Arc<redb::Database>) -> Result<Self, RedbStorageError> {
        let write_txn = db.begin_write()?;
        {
            let _ = write_txn.open_table(REPLICATION_LOG)?;
            let _ = write_txn.open_table(NODE_META)?;
        }
        write_txn.commit()?;
        Ok(Self { db })
    }
}

impl Storage for RedbStorage {
    type Error = RedbStorageError;

    fn append_log_entry(&self, sequence: u64, data: &[u8]) -> Result<(), Self::Error> {
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(REPLICATION_LOG)?;
            table.insert(sequence, data)?;
        }
        write_txn.commit()?;
        Ok(())
    }

    fn append_next_log_entry(&self, data: &[u8]) -> Result<u64, Self::Error> {
        let write_txn = self.db.begin_write()?;
        let sequence = {
            let mut table = write_txn.open_table(REPLICATION_LOG)?;
            let previous = match table.last()? {
                Some((key, _)) => key.value(),
                None => 0,
            };
            let seq = previous + 1;
            table.insert(seq, data)?;
            seq
        };
        write_txn.commit()?;
        Ok(sequence)
    }

    fn read_log_from(&self, from_sequence: u64) -> Result<Vec<(u64, Vec<u8>)>, Self::Error> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(REPLICATION_LOG)?;
        let mut entries = Vec::new();
        for result in table.range(from_sequence..)? {
            let (key, value) = result?;
            entries.push((key.value(), value.value().to_vec()));
        }
        Ok(entries)
    }

    fn latest_sequence(&self) -> Result<u64, Self::Error> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(REPLICATION_LOG)?;
        let result = table.last()?;
        match result {
            Some((key, _)) => {
                let seq = key.value();
                Ok(seq)
            }
            None => Ok(0),
        }
    }

    fn get_node_state(&self) -> Result<Option<Vec<u8>>, Self::Error> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(NODE_META)?;
        match table.get("state")? {
            Some(data) => Ok(Some(data.value().to_vec())),
            None => Ok(None),
        }
    }

    fn put_node_state(&self, data: &[u8]) -> Result<(), Self::Error> {
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(NODE_META)?;
            table.insert("state", data)?;
        }
        write_txn.commit()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn setup() -> (RedbStorage, TempDir) {
        let dir = TempDir::new().unwrap();
        let db = Arc::new(redb::Database::create(dir.path().join("test.redb")).unwrap());
        let storage = RedbStorage::new(db).unwrap();
        (storage, dir)
    }

    #[test]
    fn test_append_and_read_log() {
        let (storage, _dir) = setup();
        storage.append_log_entry(1, b"hello").unwrap();
        storage.append_log_entry(2, b"world").unwrap();

        let entries = storage.read_log_from(1).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0], (1, b"hello".to_vec()));
        assert_eq!(entries[1], (2, b"world".to_vec()));
    }

    #[test]
    fn test_append_next_log_entry() {
        let (storage, _dir) = setup();
        let seq1 = storage.append_next_log_entry(b"first").unwrap();
        let seq2 = storage.append_next_log_entry(b"second").unwrap();
        assert_eq!(seq1, 1);
        assert_eq!(seq2, 2);
        assert_eq!(storage.latest_sequence().unwrap(), 2);
    }

    #[test]
    fn test_latest_sequence_empty() {
        let (storage, _dir) = setup();
        assert_eq!(storage.latest_sequence().unwrap(), 0);
    }

    #[test]
    fn test_node_state_roundtrip() {
        let (storage, _dir) = setup();
        assert!(storage.get_node_state().unwrap().is_none());

        storage.put_node_state(b"state-data").unwrap();
        let state = storage.get_node_state().unwrap().unwrap();
        assert_eq!(state, b"state-data");
    }
}
