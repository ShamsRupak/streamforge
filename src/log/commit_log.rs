use std::{
    fs,
    path::{Path, PathBuf},
};

use tracing::{debug, info};

use super::{index::Index, segment::Segment, LogError, Result};

#[derive(Clone, Debug)]
pub struct LogConfig {
    /// Maximum size of a single segment file in bytes before rotation.
    pub max_segment_bytes: u64,
    /// Record every Nth append in the sparse index.
    pub index_interval: u64,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            max_segment_bytes: 64 * 1024 * 1024, // 64 MiB
            index_interval: 64,
        }
    }
}

/// A multi-segment, append-only commit log.
///
/// Segments are stored as pairs of files in `dir`:
///   `{base_offset:020}.log`  – record data
///   `{base_offset:020}.idx`  – sparse offset index
pub struct Log {
    dir: PathBuf,
    config: LogConfig,
    segments: Vec<(Segment, Index)>,
}

impl Log {
    /// Open (or create) a commit log in `dir`.
    pub fn open(dir: &Path, config: LogConfig) -> Result<Self> {
        fs::create_dir_all(dir)?;

        let mut base_offsets: Vec<u64> = fs::read_dir(dir)?
            .filter_map(|e| e.ok())
            .filter_map(|e| {
                let name = e.file_name();
                let s = name.to_string_lossy();
                if s.ends_with(".log") {
                    s.trim_end_matches(".log").parse::<u64>().ok()
                } else {
                    None
                }
            })
            .collect();

        base_offsets.sort_unstable();

        let mut segments = Vec::new();

        for base in &base_offsets {
            let seg_path = seg_path(dir, *base);
            let idx_path = idx_path(dir, *base);
            let mut seg = Segment::open(&seg_path, *base, config.max_segment_bytes)?;
            seg.recover()?;
            let idx = Index::open(&idx_path, config.index_interval)?;
            segments.push((seg, idx));
        }

        // If there are no segments, create the initial one at offset 0.
        if segments.is_empty() {
            let base = 0u64;
            let seg = Segment::open(&seg_path(dir, base), base, config.max_segment_bytes)?;
            let idx = Index::open(&idx_path(dir, base), config.index_interval)?;
            segments.push((seg, idx));
            info!("created initial segment at offset 0");
        }

        Ok(Self {
            dir: dir.to_path_buf(),
            config,
            segments,
        })
    }

    /// Append `payload` to the active segment.
    /// Returns the logical offset assigned to this record.
    pub fn append(&mut self, payload: &[u8]) -> Result<u64> {
        // Rotate if the active segment is full.
        if self.active_segment().is_full() {
            self.rotate()?;
        }

        let (seg, idx) = self.active_segment_and_index();
        let file_pos_before = seg.file_pos;
        let offset = seg.append(payload)?;
        let record_index = seg.record_count() - 1; // 0-based index after append
        idx.maybe_record(record_index, offset, file_pos_before);

        debug!(offset, "appended record ({} bytes)", payload.len());
        Ok(offset)
    }

    /// Read the record at logical `offset`.
    pub fn read(&mut self, offset: u64) -> Result<Vec<u8>> {
        if self.segments.is_empty() {
            return Err(LogError::Empty);
        }

        let seg_idx = self.find_segment(offset)?;
        let (seg, idx) = &mut self.segments[seg_idx];

        // Use the sparse index to jump close to the target record, then scan.
        let start_file_pos = match idx.lookup(offset) {
            Some((indexed_offset, file_pos)) => {
                // The index entry at `indexed_offset` is `offset - indexed_offset` records
                // before our target. We need to scan forward that many records from `file_pos`.
                let skip = offset - indexed_offset;
                scan_to(seg, file_pos, skip)?
            }
            None => {
                // No index entry at all — scan from the very beginning of the segment.
                let skip = offset - seg.base_offset;
                scan_to(seg, 0, skip)?
            }
        };

        seg.read_at(start_file_pos)
    }

    /// The logical offset that will be assigned to the next record.
    pub fn next_offset(&self) -> u64 {
        self.active_segment().next_offset
    }

    /// Flush the index of the active segment to disk.
    pub fn flush_index(&mut self) -> Result<()> {
        let (_, idx) = self.active_segment_and_index();
        idx.flush()
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    fn active_segment(&self) -> &Segment {
        &self.segments.last().unwrap().0
    }

    fn active_segment_and_index(&mut self) -> &mut (Segment, Index) {
        self.segments.last_mut().unwrap()
    }

    fn rotate(&mut self) -> Result<()> {
        // Flush the current index before rotating.
        {
            let (_, idx) = self.segments.last_mut().unwrap();
            idx.flush()?;
        }

        let new_base = self.segments.last().unwrap().0.next_offset;
        let seg = Segment::open(
            &seg_path(&self.dir, new_base),
            new_base,
            self.config.max_segment_bytes,
        )?;
        let idx = Index::open(&idx_path(&self.dir, new_base), self.config.index_interval)?;
        self.segments.push((seg, idx));
        info!(new_base, "rotated to new segment");
        Ok(())
    }

    /// Binary-search for the segment that contains `offset`.
    fn find_segment(&self, offset: u64) -> Result<usize> {
        if offset >= self.next_offset() {
            return Err(LogError::OffsetOutOfRange(offset));
        }

        // Find the last segment whose base_offset <= offset.
        let idx = self
            .segments
            .partition_point(|(seg, _)| seg.base_offset <= offset)
            .saturating_sub(1);

        Ok(idx)
    }
}

/// Scan forward `skip` records starting from `start_file_pos` and return
/// the file position of the record after skipping.
fn scan_to(seg: &mut Segment, start_file_pos: u64, skip: u64) -> Result<u64> {
    let mut pos = start_file_pos;
    for _ in 0..skip {
        // Read the 4-byte length prefix to know how far to skip.
        use std::io::{Read, Seek, SeekFrom};
        seg.reader().seek(SeekFrom::Start(pos))?;
        let mut header = [0u8; 4];
        seg.reader()
            .read_exact(&mut header)
            .map_err(|_| LogError::CorruptRecord("truncated during scan".into()))?;
        let length = u32::from_le_bytes(header);
        // Skip: 4 (already read) + 4 (CRC) + length (payload)
        pos += 8 + length as u64;
    }
    Ok(pos)
}

fn seg_path(dir: &Path, base: u64) -> PathBuf {
    dir.join(format!("{:020}.log", base))
}

fn idx_path(dir: &Path, base: u64) -> PathBuf {
    dir.join(format!("{:020}.idx", base))
}
