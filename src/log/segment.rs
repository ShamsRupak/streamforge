use std::{
    fs::{File, OpenOptions},
    io::{BufWriter, Read, Seek, SeekFrom, Write},
    path::Path,
};

use crc32fast::Hasher;

use super::{LogError, Result};

/// On-disk record layout:
///   [4 bytes: payload length (u32 LE)]
///   [4 bytes: CRC32 of payload (u32 LE)]
///   [N bytes: payload]
const HEADER_SIZE: u64 = 8;

pub struct Segment {
    /// The first logical offset stored in this segment.
    pub base_offset: u64,
    /// The next offset to be assigned (base_offset + record count).
    pub next_offset: u64,
    /// Current write position in the data file (bytes).
    pub file_pos: u64,
    writer: BufWriter<File>,
    /// Read-only handle opened separately so we can seek freely.
    reader: File,
    pub max_bytes: u64,
}

impl Segment {
    /// Open or create a segment whose data file is at `path`.
    /// `base_offset` is the logical offset of the first record.
    pub fn open(path: &Path, base_offset: u64, max_bytes: u64) -> Result<Self> {
        let write_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;

        let file_pos = write_file.metadata()?.len();

        let reader = OpenOptions::new().read(true).open(path)?;

        Ok(Self {
            base_offset,
            next_offset: base_offset,
            file_pos,
            writer: BufWriter::new(write_file),
            reader,
            max_bytes,
        })
    }

    /// Replay existing records on disk to restore `next_offset` and `file_pos`.
    pub fn recover(&mut self) -> Result<()> {
        self.reader.seek(SeekFrom::Start(0))?;
        let mut pos = 0u64;
        let mut count = 0u64;

        loop {
            let mut header = [0u8; 8];
            match self.reader.read_exact(&mut header) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(LogError::Io(e)),
            }
            let length = u32::from_le_bytes(header[0..4].try_into().unwrap()) as u64;
            // skip crc (4 bytes) + payload
            self.reader.seek(SeekFrom::Current(length as i64))?;
            pos += HEADER_SIZE + length;
            count += 1;
        }

        self.file_pos = pos;
        self.next_offset = self.base_offset + count;
        Ok(())
    }

    /// Append a payload. Returns the logical offset assigned to this record.
    pub fn append(&mut self, payload: &[u8]) -> Result<u64> {
        let length = payload.len() as u32;
        let crc = crc32(payload);

        self.writer.write_all(&length.to_le_bytes())?;
        self.writer.write_all(&crc.to_le_bytes())?;
        self.writer.write_all(payload)?;
        self.writer.flush()?;

        let offset = self.next_offset;
        self.next_offset += 1;
        self.file_pos += HEADER_SIZE + length as u64;
        Ok(offset)
    }

    /// Read the record at `file_position` (byte offset in this segment's file).
    pub fn read_at(&mut self, file_position: u64) -> Result<Vec<u8>> {
        self.reader.seek(SeekFrom::Start(file_position))?;

        let mut header = [0u8; 8];
        self.reader
            .read_exact(&mut header)
            .map_err(|e| match e.kind() {
                std::io::ErrorKind::UnexpectedEof => {
                    LogError::CorruptRecord("truncated header".into())
                }
                _ => LogError::Io(e),
            })?;

        let length = u32::from_le_bytes(header[0..4].try_into().unwrap());
        let stored_crc = u32::from_le_bytes(header[4..8].try_into().unwrap());

        let mut payload = vec![0u8; length as usize];
        self.reader
            .read_exact(&mut payload)
            .map_err(|e| match e.kind() {
                std::io::ErrorKind::UnexpectedEof => {
                    LogError::CorruptRecord("truncated payload".into())
                }
                _ => LogError::Io(e),
            })?;

        let actual_crc = crc32(&payload);
        if actual_crc != stored_crc {
            return Err(LogError::CrcMismatch {
                expected: stored_crc,
                actual: actual_crc,
            });
        }

        Ok(payload)
    }

    /// True when this segment has no more room for new records.
    pub fn is_full(&self) -> bool {
        self.file_pos >= self.max_bytes
    }

    /// Number of records written to this segment.
    pub fn record_count(&self) -> u64 {
        self.next_offset - self.base_offset
    }

    /// Expose the read handle for use by the log's scan-forward helper.
    pub fn reader(&mut self) -> &mut File {
        &mut self.reader
    }
}

fn crc32(data: &[u8]) -> u32 {
    let mut h = Hasher::new();
    h.update(data);
    h.finalize()
}
