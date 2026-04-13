use lz4_flex::{compress_prepend_size, decompress_size_prepended};

use crate::log::LogError;

// ── Single record ─────────────────────────────────────────────────────────────

/// Compress `input` using LZ4. The returned bytes include a prepended
/// 4-byte decompressed-length header (lz4_flex convention).
pub fn compress(input: &[u8]) -> Vec<u8> {
    compress_prepend_size(input)
}

/// Decompress a buffer produced by `compress`.
pub fn decompress(input: &[u8]) -> Result<Vec<u8>, LogError> {
    decompress_size_prepended(input)
        .map_err(|e| LogError::CorruptRecord(format!("lz4 decompress: {e}")))
}

// ── Batch compression ─────────────────────────────────────────────────────────
//
//  Batch wire format (before LZ4 compression):
//    [4B: count (u32 LE)]
//    for each message:
//      [4B: message_length (u32 LE)]
//      [N B: message bytes]
//
//  The entire concatenated buffer is then compressed with LZ4.

/// Compress a batch of messages into a single LZ4-compressed blob.
pub fn compress_batch(messages: &[&[u8]]) -> Vec<u8> {
    let total_payload: usize = messages.iter().map(|m| 4 + m.len()).sum();
    let mut buf = Vec::with_capacity(4 + total_payload);

    // count
    buf.extend_from_slice(&(messages.len() as u32).to_le_bytes());
    for msg in messages {
        buf.extend_from_slice(&(msg.len() as u32).to_le_bytes());
        buf.extend_from_slice(msg);
    }
    compress(&buf)
}

/// Decompress a batch blob produced by `compress_batch`.
pub fn decompress_batch(input: &[u8]) -> Result<Vec<Vec<u8>>, LogError> {
    let raw = decompress(input)?;
    if raw.len() < 4 {
        return Err(LogError::CorruptRecord(
            "batch header truncated".to_string(),
        ));
    }
    let count = u32::from_le_bytes(raw[0..4].try_into().unwrap()) as usize;
    let mut messages = Vec::with_capacity(count);
    let mut pos = 4usize;

    for _ in 0..count {
        if pos + 4 > raw.len() {
            return Err(LogError::CorruptRecord(
                "batch entry length truncated".to_string(),
            ));
        }
        let len = u32::from_le_bytes(raw[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        if pos + len > raw.len() {
            return Err(LogError::CorruptRecord(
                "batch entry payload truncated".to_string(),
            ));
        }
        messages.push(raw[pos..pos + len].to_vec());
        pos += len;
    }

    Ok(messages)
}
