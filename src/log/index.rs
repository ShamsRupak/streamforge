use std::{
    fs::{File, OpenOptions},
    io::{BufWriter, Read, Write},
    path::Path,
};

use super::Result;

/// Sparse offset index.
///
/// Every `interval`-th record written to the segment causes an entry
/// `(logical_offset, file_position)` to be appended here.
///
/// Lookup: binary-search to the largest entry ≤ target_offset, then
/// tell the segment to start a linear scan from that file position.
pub struct Index {
    entries: Vec<(u64, u64)>, // (logical offset, file byte position)
    interval: u64,
    path: std::path::PathBuf,
}

impl Index {
    pub fn open(path: &Path, interval: u64) -> Result<Self> {
        let mut entries = Vec::new();

        if path.exists() {
            let mut f = File::open(path)?;
            let mut buf = Vec::new();
            f.read_to_end(&mut buf)?;

            // Each entry is 16 bytes: 8-byte offset + 8-byte file_pos (LE)
            for chunk in buf.chunks_exact(16) {
                let offset = u64::from_le_bytes(chunk[0..8].try_into().unwrap());
                let file_pos = u64::from_le_bytes(chunk[8..16].try_into().unwrap());
                entries.push((offset, file_pos));
            }
        }

        Ok(Self {
            entries,
            interval,
            path: path.to_path_buf(),
        })
    }

    /// Called after every append so the index can decide whether to record
    /// this entry. `record_index` is the 0-based position of the record
    /// within the segment (i.e. `next_offset - base_offset - 1` after append).
    pub fn maybe_record(&mut self, record_index: u64, logical_offset: u64, file_pos: u64) {
        if record_index.is_multiple_of(self.interval) {
            self.entries.push((logical_offset, file_pos));
        }
    }

    /// Return the file position of the entry whose logical offset is the
    /// largest value ≤ `target`. Returns `None` when the index is empty.
    pub fn lookup(&self, target: u64) -> Option<(u64, u64)> {
        if self.entries.is_empty() {
            return None;
        }
        let idx = self
            .entries
            .partition_point(|(off, _)| *off <= target)
            .saturating_sub(1);
        Some(self.entries[idx])
    }

    /// Persist the index to disk.
    pub fn flush(&self) -> Result<()> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.path)?;
        let mut w = BufWriter::new(file);
        for (offset, file_pos) in &self.entries {
            w.write_all(&offset.to_le_bytes())?;
            w.write_all(&file_pos.to_le_bytes())?;
        }
        w.flush()?;
        Ok(())
    }

    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}
