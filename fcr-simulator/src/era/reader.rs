use std::collections::HashMap;
use std::io::{self, Cursor, Read};

use anyhow::{Context, Result, bail};
use snap::read::FrameDecoder;
use types::{ChainSpec, MainnetEthSpec, SignedBeaconBlock, Slot};

// e2store record type codes (LE u16 values)
// "e2" = [0x65, 0x32] → LE u16 = 0x3265
const TYPE_VERSION: u16 = 0x3265;
const TYPE_COMPRESSED_SIGNED_BEACON_BLOCK: u16 = 0x0001;
const TYPE_COMPRESSED_BEACON_STATE: u16 = 0x0002;
const TYPE_SLOT_INDEX: u16 = 0x3269;

/// Slots per ERA file
pub const SLOTS_PER_ERA: u64 = 8192;

struct Record {
    record_type: u16,
    data: Vec<u8>,
}

fn read_record<R: Read>(reader: &mut R) -> Result<Option<Record>> {
    let mut header = [0u8; 8];
    match reader.read_exact(&mut header) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e.into()),
    }

    let record_type = u16::from_le_bytes([header[0], header[1]]);
    let length = u32::from_le_bytes([header[2], header[3], header[4], header[5]]) as usize;
    // bytes 6-7 are reserved

    let mut data = vec![0u8; length];
    reader
        .read_exact(&mut data)
        .context("failed to read record data")?;

    Ok(Some(Record { record_type, data }))
}

fn decompress_snappy(data: &[u8]) -> Result<Vec<u8>> {
    let mut decoder = FrameDecoder::new(data);
    let mut decompressed = Vec::new();
    decoder
        .read_to_end(&mut decompressed)
        .context("snappy decompression failed")?;
    Ok(decompressed)
}

/// Parse an ERA file and return all blocks keyed by slot
pub fn parse_era_blocks(
    data: &[u8],
    spec: &ChainSpec,
) -> Result<HashMap<Slot, SignedBeaconBlock<MainnetEthSpec>>> {
    let mut reader = Cursor::new(data);
    let mut blocks = HashMap::new();

    // Read version record
    let version = read_record(&mut reader)?.context("missing version record")?;
    if version.record_type != TYPE_VERSION {
        bail!(
            "expected version record (0x{:04x}), got 0x{:04x}",
            TYPE_VERSION,
            version.record_type
        );
    }

    // Read remaining records
    while let Some(record) = read_record(&mut reader)? {
        match record.record_type {
            TYPE_COMPRESSED_SIGNED_BEACON_BLOCK => {
                let ssz_bytes = decompress_snappy(&record.data)?;
                let block = SignedBeaconBlock::<MainnetEthSpec>::from_ssz_bytes(&ssz_bytes, spec)
                    .map_err(|e| anyhow::anyhow!("SSZ decode error: {:?}", e))?;

                let slot = block.message().slot();
                blocks.insert(slot, block);
            }
            TYPE_COMPRESSED_BEACON_STATE | TYPE_SLOT_INDEX => {
                // Skip state and index records - we don't need them after bootstrap
            }
            0x0000 => {
                // Empty record, skip
            }
            _ => {
                // Unknown record type, skip
            }
        }
    }

    Ok(blocks)
}

/// Compute the ERA number for a given slot
pub fn era_number_for_slot(slot: u64) -> u64 {
    // ERA 0 = genesis state only (no blocks)
    // ERA N (N>=1) = blocks for slots [(N-1)*8192, N*8192-1]
    (slot / SLOTS_PER_ERA) + 1
}
