// dvrepair - DV tape repair tool
// Conjoins multiple captures of the same tape into one healthy video.
//
// ARCHITECTURE
// ============
// 1. Parse AVI (RIFF) container, extract raw DV frames.
//    - Type-2: video in "00dc" chunks (separate video stream)
//    - Type-1: video+audio interleaved in "00__" / "iavs" stream
// 2. For each frame, inspect Video DIF block STA bits to detect corruption.
// 3. For corrupt frames: search spare files for a healthy copy via timecode/index.
// 4. Replace corrupt frame with healthy one, or apply fallback policy.
// 5. Patch raw AVI bytes and write a new output file (non-destructive).
//
// DV FORMAT PRIMER
// ================
// A raw DV frame is either 120,000 bytes (NTSC 525/60) or 144,000 bytes (PAL 625/50).
// It is organized into DIF sequences (10 NTSC / 12 PAL), each containing
// 150 DIF blocks of 80 bytes. DIF block header byte 0 bits [7:5] = SCT:
//   0=Header, 1=Subcode, 2=VAUX, 3=Audio, 4=Video
// Video DIF block byte 3 high nibble = STA (status after Reed-Solomon decoding).
// STA != 0 means the VCR flagged the block as unrecoverable.
// All-0xFF payload bytes = complete tape dropout.
//
// VAUX blocks contain 5-byte packs. Pack type 0x13/0x63 = tape timecode in BCD.
//
// TYPE-1 vs TYPE-2
// ================
// Type-1: The full interleaved DV bitstream (video+audio together) sits in
//   a single "iavs" stream. Chunks are named "00__" or similar. The raw DV
//   frame bytes are identical to Type-2 video frames.
// Type-2: Video in "00dc" or "00db" chunks, duplicate audio in "01wb" chunks.
//   Most Windows capture software (WinDV, DVGrab on Windows) produces Type-2.
//   Type-1 is common from Linux dvgrab and some Sony/Canon cameras directly.

use std::{
    collections::HashMap,
    fs,
    io::Write,
    path::{Path, PathBuf},
};

use anyhow::{bail, Context, Result};
use clap::{Parser, ValueEnum};
use log::{debug, info, warn};

// ─────────────────────────────────────────────────────────────────────────────
// CLI
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Parser, Debug)]
#[command(
    name = "dvrepair",
    version,
    about = "Conjoin multiple DV tape captures into one healthy video",
    long_about = "\
dvrepair reads multiple Type-1 or Type-2 DV-in-AVI files captured from the
same tape, detects corrupt frames via DIF block STA error flags, and replaces
them with healthy frames found in spare captures.

DETECTION
  Each Video DIF block has a STA nibble (byte 3 high bits).
  STA != 0 = Reed-Solomon failed. All-0xFF payload = full dropout.
  A frame is healthy only when every Video DIF block passes both checks.

MATCHING
  Frames are matched by DV VAUX timecode (hh:mm:ss;ff from tape), then by
  frame index as fallback. Use --match-mode to control this.

EXAMPLE
  dvrepair tape_a.avi tape_b.avi tape_c.avi -o repaired.avi
  dvrepair tape_a.avi tape_b.avi --main-stream tape_a.avi -o out.avi
  dvrepair --dump-riff tape_a.avi   (show RIFF structure, no repair)
  dvrepair --to-type2 tape_t1.avi -o tape_t2.avi  (convert Type-1 to Type-2)"
)]
struct Cli {
    /// Input AVI files (minimum 2 for repair: one main + one or more spares).
    #[arg(required = true, num_args = 1..)]
    inputs: Vec<PathBuf>,

    /// Which input is the main stream (others are spares).
    /// Required when input files have different frame counts.
    #[arg(long)]
    main_stream: Option<PathBuf>,

    /// Output file path.
    #[arg(short, long, default_value = "repaired.avi")]
    output: PathBuf,

    /// Fallback when no spare has a healthy copy of a corrupt frame:
    ///   keep   = leave corrupt frame as-is [default]
    ///   freeze = duplicate previous healthy frame
    ///   blank  = substitute synthetic black DV frame
    #[arg(long, value_enum, default_value = "keep")]
    fallback: Fallback,

    /// Frame matching strategy:
    ///   timecode-then-index = DV VAUX timecode, fallback to index [default]
    ///   index-only          = frame index only (assumes aligned captures)
    ///   timecode-only       = VAUX timecode only
    #[arg(long, value_enum, default_value = "timecode-then-index")]
    match_mode: MatchMode,

    /// Instead of repairing, dump the RIFF chunk structure of the first input
    /// file and exit. Useful for diagnosing parse failures.
    #[arg(long)]
    dump_riff: bool,

    /// Convert a single Type-1 DV AVI to Type-2 and write to --output.
    /// Use this when your captures are Type-1 and you need to process them.
    /// Only one input file should be given with this flag.
    #[arg(long)]
    to_type2: bool,

    /// Verbose output: -v = debug, -vv = trace.
    #[arg(short, long, action = clap::ArgAction::Count)]
    verbose: u8,
}

#[derive(ValueEnum, Clone, Debug, PartialEq)]
enum Fallback { Keep, Freeze, Blank }

#[derive(ValueEnum, Clone, Debug, PartialEq)]
enum MatchMode { TimecodeThenIndex, IndexOnly, TimecodeOnly }

// ─────────────────────────────────────────────────────────────────────────────
// AVI / RIFF PARSER
// ─────────────────────────────────────────────────────────────────────────────

fn u32le(data: &[u8], off: usize) -> u32 {
    u32::from_le_bytes(data[off..off+4].try_into().unwrap())
}

fn cc_str(cc: &[u8]) -> String {
    cc.iter().map(|&b| if b.is_ascii_graphic() || b == b' ' { b as char } else { '.' }).collect()
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct DvTimecode { hour: u8, minute: u8, second: u8, frame: u8 }

impl std::fmt::Display for DvTimecode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:02}:{:02}:{:02};{:02}", self.hour, self.minute, self.second, self.frame)
    }
}

#[derive(Debug, Clone)]
struct DvFrame {
    data: Vec<u8>,
    index: usize,
    timecode: Option<DvTimecode>,
    healthy: bool,
    error_blocks: u32,
}

/// What kind of AVI was parsed — affects how we write the output.
#[derive(Debug, Clone, PartialEq)]
enum AviKind {
    /// Standard Type-2: video frames in ??dc / ??db chunks.
    Type2,
    /// Type-1: interleaved DV in a single ??__ stream (iavs handler).
    /// We extract each 120000/144000-byte frame from the stream chunks.
    Type1,
}

#[derive(Debug)]
struct ParsedAvi {
    micro_sec_per_frame: u32,
    raw: Vec<u8>,
    frames: Vec<DvFrame>,
    /// Byte offset + size of each frame's data region in `raw`.
    frame_regions: Vec<(usize, usize)>,
    kind: AviKind,
    /// Frame size (120000 NTSC or 144000 PAL), detected from first frame.
    frame_size: usize,
}

fn parse_avi(path: &Path) -> Result<ParsedAvi> {
    let raw = fs::read(path).with_context(|| format!("Cannot read {:?}", path))?;
    if raw.len() < 12 { bail!("{:?}: file too small", path); }
    if &raw[0..4] != b"RIFF" { bail!("{:?}: not a RIFF file", path); }
    if &raw[8..12] != b"AVI " { bail!("{:?}: RIFF type not AVI", path); }

    let mut us_per_frame = 33367u32;
    let mut frames = Vec::new();
    let mut frame_regions = Vec::new();
    // We detect AVI kind from the stream handler in strh.
    let mut kind = AviKind::Type2;
    let mut frame_size = 120_000usize;

    walk_chunks(&raw, 12, raw.len(), &mut us_per_frame, &mut frames,
                &mut frame_regions, &mut kind, &mut frame_size, path);

    if frames.is_empty() {
        bail!(
            "{:?}: no DV video frames found.\n\
             Hint: run with --dump-riff to inspect the file structure.\n\
             If the file is Type-1 DV, use --to-type2 to convert it first.",
            path
        );
    }

    Ok(ParsedAvi { micro_sec_per_frame: us_per_frame, raw, frames, frame_regions, kind, frame_size })
}

#[allow(clippy::too_many_arguments)]
fn walk_chunks(
    data: &[u8], mut pos: usize, end: usize,
    us_per_frame: &mut u32,
    frames: &mut Vec<DvFrame>,
    regions: &mut Vec<(usize, usize)>,
    kind: &mut AviKind,
    frame_size: &mut usize,
    path: &Path,
) {
    while pos + 8 <= end.min(data.len()) {
        let cc = &data[pos..pos+4];
        let sz = u32le(data, pos+4) as usize;
        let ds = pos + 8;
        let nx = pos + 8 + sz + (sz & 1);
        if ds + sz > data.len() {
            warn!("{:?}: truncated chunk '{}' at {:#x}", path, cc_str(cc), pos);
            break;
        }
        match cc {
            b"RIFF" | b"LIST" => {
                if ds + 4 > data.len() { break; }
                let lt = &data[ds..ds+4];
                let cs = ds + 4;
                let ce = ds + sz;
                match lt {
                    b"AVI " | b"hdrl" | b"strl" | b"AVIX" =>
                        walk_chunks(data, cs, ce, us_per_frame, frames, regions, kind, frame_size, path),
                    b"movi" =>
                        walk_movi(data, cs, ce, frames, regions, kind, frame_size, path),
                    _ => {}
                }
            }
            b"avih" if sz >= 40 => { *us_per_frame = u32le(data, ds); }
            // strh: detect Type-1 via handler field.
            // strh layout: fccType(4) fccHandler(4) ...
            // Type-1 has fccType="iavs" or fccHandler="iavs" or "dvsd" with type "iavs".
            b"strh" if sz >= 8 => {
                let fcc_type    = &data[ds..ds+4];
                let fcc_handler = &data[ds+4..ds+8];
                if fcc_type == b"iavs" || fcc_handler == b"iavs" {
                    *kind = AviKind::Type1;
                    debug!("{:?}: detected Type-1 DV (iavs stream)", path);
                }
            }
            _ => {}
        }
        if nx <= pos { break; }
        pos = nx;
    }
}

/// Returns true if this chunk tag looks like a video or interleaved-DV data chunk.
/// Type-2 video: "00dc", "00db" (some encoders use db=uncompressed marker)
/// Type-1 interleaved: "00__" where __ can be anything (the stream carries full DV)
fn is_dv_data_chunk(cc: &[u8], kind: &AviKind) -> bool {
    if cc.len() != 4 { return false; }
    match kind {
        AviKind::Type2 => {
            // ??dc (compressed video) or ??db (uncompressed video marker, same bytes)
            (cc[2] == b'd' && (cc[3] == b'c' || cc[3] == b'b'))
        }
        AviKind::Type1 => {
            // The interleaved stream chunks: typically "00__" where stream 0 carries
            // the full DV frame. We match any chunk from stream 0 that isn't audio.
            // Type-1 chunks use names like "00__" — two digit stream + two chars.
            // We accept any chunk from stream 00 that is large enough to be a DV frame.
            cc[0] == b'0' && cc[1] == b'0'
        }
    }
}

fn walk_movi(
    data: &[u8], mut pos: usize, end: usize,
    frames: &mut Vec<DvFrame>,
    regions: &mut Vec<(usize, usize)>,
    kind: &mut AviKind,
    frame_size: &mut usize,
    path: &Path,
) {
    while pos + 8 <= end.min(data.len()) {
        let cc = &data[pos..pos+4];
        let sz = u32le(data, pos+4) as usize;
        let ds = pos + 8;
        let nx = pos + 8 + sz + (sz & 1);
        if ds + sz > data.len() {
            warn!("{:?}: truncated movi chunk '{}' at {:#x}", path, cc_str(cc), pos);
            break;
        }

        if is_dv_data_chunk(cc, kind) && sz > 0 {
            let fb = &data[ds..ds+sz];
            // For Type-1 chunks, the chunk may be the exact DV frame size.
            // Accept if it matches a known DV frame size.
            let valid_size = sz == 120_000 || sz == 144_000;
            if valid_size {
                if *frame_size == 120_000 && sz == 144_000 { *frame_size = 144_000; }
                let idx = frames.len();
                let tc = extract_timecode(fb);
                let (healthy, eblocks) = assess_frame(fb);
                regions.push((ds, sz));
                frames.push(DvFrame {
                    data: fb.to_vec(), index: idx, timecode: tc, healthy, error_blocks: eblocks
                });
            } else if sz > 0 {
                debug!("{:?}: skipping chunk '{}' size {} (not a DV frame size)", path, cc_str(cc), sz);
            }
        } else if cc == b"LIST" && ds + 4 <= data.len() && &data[ds..ds+4] == b"rec " {
            walk_movi(data, ds+4, ds+sz, frames, regions, kind, frame_size, path);
        }
        // Skip audio chunks (??wb, ??__) silently.

        if nx <= pos { break; }
        pos = nx;
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// RIFF DUMP (diagnostic)
// ─────────────────────────────────────────────────────────────────────────────

fn dump_riff(data: &[u8], mut pos: usize, end: usize, depth: usize) {
    let indent = "  ".repeat(depth);
    while pos + 8 <= end.min(data.len()) {
        let cc = &data[pos..pos+4];
        let sz = u32le(data, pos+4) as usize;
        let ds = pos + 8;
        let nx = pos + 8 + sz + (sz & 1);

        if ds + sz > data.len() {
            println!("{}[TRUNCATED chunk '{}' at {:#x}]", indent, cc_str(cc), pos);
            break;
        }

        let is_list = cc == b"LIST" || cc == b"RIFF";
        if is_list && ds + 4 <= data.len() {
            let lt = &data[ds..ds+4];
            println!("{}{} '{}' ({} bytes at {:#x})", indent, cc_str(cc), cc_str(lt), sz, pos);
            // Recurse into known containers; for others just note them.
            let recurse = matches!(lt, b"AVI " | b"hdrl" | b"strl" | b"movi" | b"rec " | b"AVIX");
            if recurse {
                // For movi, only show first 8 and last 2 children to avoid walls of text.
                if lt == b"movi" {
                    dump_movi_summary(data, ds+4, ds+sz, depth+1);
                } else {
                    dump_riff(data, ds+4, ds+sz, depth+1);
                }
            }
        } else {
            // Leaf chunk: show fourcc, size, and a hex preview of first 16 bytes.
            let preview_len = sz.min(16);
            let preview: String = data[ds..ds+preview_len]
                .iter()
                .map(|b| format!("{:02x}", b))
                .collect::<Vec<_>>()
                .join(" ");
            println!("{}  '{}' {} bytes  [{}{}]",
                indent, cc_str(cc), sz, preview,
                if sz > 16 { " ..." } else { "" });
        }

        if nx <= pos { break; }
        pos = nx;
    }
}

fn dump_movi_summary(data: &[u8], mut pos: usize, end: usize, depth: usize) {
    let indent = "  ".repeat(depth);
    let mut count = 0usize;
    let mut chunk_types: HashMap<String, (usize, usize)> = HashMap::new(); // cc -> (count, total_bytes)

    while pos + 8 <= end.min(data.len()) {
        let cc = &data[pos..pos+4];
        let sz = u32le(data, pos+4) as usize;
        let ds = pos + 8;
        let nx = pos + 8 + sz + (sz & 1);

        if ds + sz > data.len() { break; }

        let key = cc_str(cc);
        let e = chunk_types.entry(key).or_insert((0, 0));
        e.0 += 1;
        e.1 += sz;

        count += 1;
        if nx <= pos { break; }
        pos = nx;
    }

    println!("{}(movi contains {} total chunks)", indent, count);
    let mut types: Vec<_> = chunk_types.into_iter().collect();
    types.sort_by_key(|(k,_)| k.clone());
    for (cc, (cnt, total)) in types {
        println!("{}  '{}': {} chunks, {} bytes each avg",
            indent, cc, cnt, if cnt > 0 { total/cnt } else { 0 });
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// TYPE-1 → TYPE-2 CONVERTER
// ─────────────────────────────────────────────────────────────────────────────
//
// Type-1 AVI has a single "iavs" stream where each chunk is a full DV frame
// (video + audio interleaved per the DV standard). To produce Type-2 we need:
//   - Change strh fccType from "iavs" to "vids", handler stays "dvsd"
//   - Rename stream chunks from "00__" to "00dc"
//   - Optionally add a second audio stream (we skip this; audio stays embedded
//     in the video frames, which is fine for dvrepair's purposes)
//
// The simplest approach that preserves everything: we build a new AVI from
// scratch using the extracted frames, with proper Type-2 headers.
// For repair purposes, we don't need full Type-2 compliance — we just need
// dvrepair to be able to parse and patch the file. So we do a minimal conversion:
// patch strh fccType and rename the movi chunks.

fn convert_type1_to_type2(avi: &ParsedAvi, output: &Path) -> Result<()> {
    if avi.kind != AviKind::Type1 {
        bail!("Input is already Type-2 (or unknown), not Type-1");
    }

    // We rebuild the AVI from scratch: write proper headers then all frames as 00dc chunks.
    // This is simpler and more robust than patching the original binary.
    let frame_count = avi.frames.len();
    let frame_size = avi.frame_size;
    let us_per_frame = avi.micro_sec_per_frame;

    // Determine if NTSC or PAL from frame size.
    let (width, height, fps_num, fps_den) = match frame_size {
        120_000 => (720u32, 480u32, 30000u32, 1001u32), // NTSC ~29.97
        144_000 => (720u32, 576u32, 25u32,    1u32),    // PAL 25
        _ => bail!("Unknown frame size {}", frame_size),
    };

    let mut out: Vec<u8> = Vec::new();

    // Helper closures
    let write_u32 = |v: u32, buf: &mut Vec<u8>| buf.extend_from_slice(&v.to_le_bytes());
    let write_cc  = |cc: &[u8;4], buf: &mut Vec<u8>| buf.extend_from_slice(cc);

    // We'll build the movi LIST and idx1 separately, then assemble.
    let mut movi_data: Vec<u8> = Vec::new();
    let mut idx1_data: Vec<u8> = Vec::new();
    let mut movi_offset = 0u32; // will be patched after we know header size

    // Build movi chunks + index
    for frame in &avi.frames {
        let chunk_offset = movi_data.len() as u32; // offset within movi payload
        // 00dc chunk
        movi_data.extend_from_slice(b"00dc");
        movi_data.extend_from_slice(&(frame.data.len() as u32).to_le_bytes());
        movi_data.extend_from_slice(&frame.data);
        if frame.data.len() & 1 != 0 { movi_data.push(0); } // word-align

        // idx1 entry: cc(4) flags(4) offset(4) size(4)
        idx1_data.extend_from_slice(b"00dc");
        idx1_data.extend_from_slice(&0x10u32.to_le_bytes()); // AVIIF_KEYFRAME
        idx1_data.extend_from_slice(&chunk_offset.to_le_bytes());
        idx1_data.extend_from_slice(&(frame.data.len() as u32).to_le_bytes());
    }

    // --- Build headers ---
    // avih (56 bytes)
    let mut avih = Vec::<u8>::new();
    write_u32(us_per_frame, &mut avih);              // dwMicroSecPerFrame
    write_u32((frame_size as u32 * fps_num / fps_den) + 1, &mut avih); // dwMaxBytesPerSec (approx)
    write_u32(0, &mut avih);                          // dwPaddingGranularity
    write_u32(0x0110, &mut avih);                     // dwFlags (AVIF_HASINDEX | AVIF_ISINTERLEAVED)
    write_u32(frame_count as u32, &mut avih);         // dwTotalFrames
    write_u32(0, &mut avih);                          // dwInitialFrames
    write_u32(1, &mut avih);                          // dwStreams (video only)
    write_u32(frame_size as u32, &mut avih);          // dwSuggestedBufferSize
    write_u32(width, &mut avih);                      // dwWidth
    write_u32(height, &mut avih);                     // dwHeight
    write_u32(0, &mut avih); write_u32(0, &mut avih); write_u32(0, &mut avih); write_u32(0, &mut avih); // reserved

    // strh (video, 56 bytes)
    let mut strh_v = Vec::<u8>::new();
    write_cc(b"vids", &mut strh_v);                   // fccType
    write_cc(b"dvsd", &mut strh_v);                   // fccHandler
    write_u32(0, &mut strh_v);                        // dwFlags
    write_u32(0, &mut strh_v);                        // wPriority + wLanguage
    write_u32(0, &mut strh_v);                        // dwInitialFrames
    write_u32(fps_den, &mut strh_v);                  // dwScale
    write_u32(fps_num, &mut strh_v);                  // dwRate
    write_u32(0, &mut strh_v);                        // dwStart
    write_u32(frame_count as u32, &mut strh_v);       // dwLength
    write_u32(frame_size as u32, &mut strh_v);        // dwSuggestedBufferSize
    write_u32(0xFFFF_FFFFu32, &mut strh_v);           // dwQuality
    write_u32(0, &mut strh_v);                        // dwSampleSize
    // rcFrame (left, top, right, bottom) as 4×u16 packed into 2×u32
    write_u32(0, &mut strh_v);
    write_u32((height << 16) | width, &mut strh_v);

    // strf for video = BITMAPINFOHEADER (40 bytes)
    let mut strf_v = Vec::<u8>::new();
    write_u32(40, &mut strf_v);                       // biSize
    write_u32(width, &mut strf_v);                    // biWidth
    write_u32(height, &mut strf_v);                   // biHeight (positive = bottom-up)
    strf_v.extend_from_slice(&1u16.to_le_bytes());    // biPlanes
    strf_v.extend_from_slice(&24u16.to_le_bytes());   // biBitCount
    write_cc(b"dvsd", &mut strf_v);                   // biCompression
    write_u32(frame_size as u32, &mut strf_v);        // biSizeImage
    write_u32(0, &mut strf_v);                        // biXPelsPerMeter
    write_u32(0, &mut strf_v);                        // biYPelsPerMeter
    write_u32(0, &mut strf_v);                        // biClrUsed
    write_u32(0, &mut strf_v);                        // biClrImportant

    // Assemble LIST strl (strh + strf)
    let mut strl = Vec::<u8>::new();
    strl.extend_from_slice(b"strh");
    write_u32(strh_v.len() as u32, &mut strl);
    strl.extend_from_slice(&strh_v);
    strl.extend_from_slice(b"strf");
    write_u32(strf_v.len() as u32, &mut strl);
    strl.extend_from_slice(&strf_v);

    // Assemble LIST hdrl
    let mut hdrl = Vec::<u8>::new();
    hdrl.extend_from_slice(b"avih");
    write_u32(avih.len() as u32, &mut hdrl);
    hdrl.extend_from_slice(&avih);
    // strl LIST
    hdrl.extend_from_slice(b"LIST");
    write_u32((strl.len() + 4) as u32, &mut hdrl);
    hdrl.extend_from_slice(b"strl");
    hdrl.extend_from_slice(&strl);

    // Now we can compute the movi offset for idx1.
    // RIFF header: 12 bytes
    // LIST hdrl: 8 + 4 + hdrl.len() = 12 + hdrl.len()
    // LIST movi: 8 + 4 + movi_data.len()
    // movi chunks start at: 12 + (12 + hdrl.len()) + 12 = 36 + hdrl.len()
    movi_offset = (12 + hdrl.len() + 12) as u32; // start of movi LIST payload
    // Patch idx1: offsets are relative to start of movi payload (after LIST movi header+type)
    // idx1 offsets should be relative to the start of the movi LIST chunk data region.
    // (Some players want absolute, some relative — we use relative to movi start, which is the spec.)
    // Actually per AVI spec, idx1 offsets are relative to the start of the movi LIST chunk
    // (i.e., the 'movi' fourcc offset). Our idx1 was built with per-chunk movi-relative offsets
    // already, so this is correct.

    // Assemble full file
    let hdrl_list_size = (hdrl.len() + 4) as u32; // +4 for "hdrl" fourcc
    let movi_list_size = (movi_data.len() + 4) as u32; // +4 for "movi" fourcc
    let idx1_size = idx1_data.len() as u32;

    let riff_payload_size = 4 // "AVI "
        + 8 + hdrl_list_size as usize // LIST hdrl
        + 8 + movi_list_size as usize // LIST movi
        + 8 + idx1_data.len(); // idx1

    // RIFF header
    out.extend_from_slice(b"RIFF");
    write_u32(riff_payload_size as u32, &mut out);
    out.extend_from_slice(b"AVI ");

    // LIST hdrl
    out.extend_from_slice(b"LIST");
    write_u32(hdrl_list_size, &mut out);
    out.extend_from_slice(b"hdrl");
    out.extend_from_slice(&hdrl);

    // LIST movi
    out.extend_from_slice(b"LIST");
    write_u32(movi_list_size, &mut out);
    out.extend_from_slice(b"movi");
    out.extend_from_slice(&movi_data);

    // idx1
    out.extend_from_slice(b"idx1");
    write_u32(idx1_size, &mut out);
    out.extend_from_slice(&idx1_data);

    let mut f = fs::File::create(output)
        .with_context(|| format!("Cannot create {:?}", output))?;
    f.write_all(&out)
        .with_context(|| format!("Write error {:?}", output))?;

    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// DV DIF BLOCK HEALTH CHECK
// ─────────────────────────────────────────────────────────────────────────────

const DIF_BLOCK: usize = 80;
const SCT_VAUX: u8 = 2;
const SCT_VIDEO: u8 = 4;

fn n_seq(frame_len: usize) -> Option<usize> {
    match frame_len { 120_000 => Some(10), 144_000 => Some(12), _ => None }
}

fn assess_frame(frame: &[u8]) -> (bool, u32) {
    let ns = match n_seq(frame.len()) { Some(n) => n, None => return (false, 1) };
    let mut errs = 0u32;
    for seq in 0..ns {
        for blk in 0..150usize {
            let off = (seq * 150 + blk) * DIF_BLOCK;
            if off + DIF_BLOCK > frame.len() { break; }
            let b = &frame[off..off+DIF_BLOCK];
            if (b[0] >> 5) & 7 != SCT_VIDEO { continue; }
            if (b[3] >> 4) & 0x0F != 0 { errs += 1; continue; }
            if b[4..].iter().all(|&x| x == 0xFF) { errs += 1; }
        }
    }
    (errs == 0, errs)
}

// ─────────────────────────────────────────────────────────────────────────────
// DV VAUX TIMECODE
// ─────────────────────────────────────────────────────────────────────────────

fn extract_timecode(frame: &[u8]) -> Option<DvTimecode> {
    let ns = n_seq(frame.len())?;
    for seq in 0..ns {
        for vi in 3usize..=5 {
            let off = (seq * 150 + vi) * DIF_BLOCK;
            if off + DIF_BLOCK > frame.len() { continue; }
            let b = &frame[off..off+DIF_BLOCK];
            if (b[0] >> 5) & 7 != SCT_VAUX { continue; }
            for pi in 0..15usize {
                let p = 3 + pi * 5;
                if p + 5 > b.len() { break; }
                let pk = &b[p..p+5];
                if pk[0] == 0x13 || pk[0] == 0x63 {
                    if let Some(tc) = decode_tc_pack(pk) { return Some(tc); }
                }
            }
        }
    }
    None
}

fn decode_tc_pack(p: &[u8]) -> Option<DvTimecode> {
    if p[1]==0xFF || p[2]==0xFF || p[3]==0xFF || p[4]==0xFF { return None; }
    let fr = bcd(p[1] & 0x3F)?;
    let se = bcd(p[2] & 0x7F)?;
    let mi = bcd(p[3] & 0x7F)?;
    let ho = bcd(p[4] & 0x3F)?;
    if se > 59 || mi > 59 || ho > 23 { return None; }
    Some(DvTimecode { hour: ho, minute: mi, second: se, frame: fr })
}

fn bcd(v: u8) -> Option<u8> {
    let hi = v >> 4; let lo = v & 0xF;
    if hi > 9 || lo > 9 { None } else { Some(hi*10+lo) }
}

// ─────────────────────────────────────────────────────────────────────────────
// BLANK FRAME SYNTHESIS
// ─────────────────────────────────────────────────────────────────────────────

fn make_blank_frame(size: usize) -> Vec<u8> {
    let ns = match n_seq(size) { Some(n) => n, None => return vec![0u8; size] };
    let mut f = vec![0u8; size];
    for seq in 0..ns {
        for blk in 0..150usize {
            let off = (seq * 150 + blk) * DIF_BLOCK;
            let sct: u8 = match blk {
                0 => 0, 1|2 => 1, 3..=5 => 2,
                _ => if (blk-6) % 16 == 0 && blk < 90 { 3 } else { 4 },
            };
            f[off] = (sct << 5) | (seq as u8 & 0x0F);
            f[off+1] = blk as u8 & 0x3F;
        }
    }
    f
}

// ─────────────────────────────────────────────────────────────────────────────
// SPARE INDEX
// ─────────────────────────────────────────────────────────────────────────────

struct SpareIndex {
    by_tc: HashMap<DvTimecode, Vec<(usize, usize)>>,
    files: Vec<Vec<DvFrame>>,
}

impl SpareIndex {
    fn build(spares: &[ParsedAvi]) -> Self {
        let mut by_tc: HashMap<DvTimecode, Vec<(usize, usize)>> = HashMap::new();
        let mut files = Vec::new();
        for (fi, avi) in spares.iter().enumerate() {
            files.push(avi.frames.clone());
            for (fri, frame) in avi.frames.iter().enumerate() {
                if frame.healthy {
                    if let Some(tc) = &frame.timecode {
                        by_tc.entry(tc.clone()).or_default().push((fi, fri));
                    }
                }
            }
        }
        SpareIndex { by_tc, files }
    }

    fn by_timecode(&self, tc: &DvTimecode) -> Option<&DvFrame> {
        self.by_tc.get(tc)?.iter().map(|&(fi,fri)| &self.files[fi][fri]).find(|f| f.healthy)
    }

    fn by_index(&self, idx: usize) -> Option<&DvFrame> {
        self.files.iter().flat_map(|fs| fs.get(idx)).find(|f| f.healthy)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// REPAIR
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Default)]
struct Stats {
    total: usize, corrupt: usize,
    rep_tc: usize, rep_idx: usize,
    fb_keep: usize, fb_freeze: usize, fb_blank: usize,
}

fn repair(main: &ParsedAvi, spares: &[ParsedAvi], mode: &MatchMode, fallback: &Fallback)
    -> (Vec<Vec<u8>>, Stats)
{
    let idx = SpareIndex::build(spares);
    let mut out = Vec::with_capacity(main.frames.len());
    let mut st = Stats { total: main.frames.len(), ..Default::default() };

    for frame in &main.frames {
        if frame.healthy { out.push(frame.data.clone()); continue; }
        st.corrupt += 1;
        debug!("Frame {:5}: CORRUPT err_blks={} tc={:?}", frame.index, frame.error_blocks,
               frame.timecode.as_ref().map(|t| t.to_string()));

        let replacement: Option<Vec<u8>> = match mode {
            MatchMode::TimecodeThenIndex => {
                if let Some(f) = frame.timecode.as_ref().and_then(|tc| idx.by_timecode(tc)) {
                    info!("Frame {:5}: repaired via timecode {}", frame.index, frame.timecode.as_ref().unwrap());
                    st.rep_tc += 1; Some(f.data.clone())
                } else if let Some(f) = idx.by_index(frame.index) {
                    info!("Frame {:5}: repaired via index", frame.index);
                    st.rep_idx += 1; Some(f.data.clone())
                } else { None }
            }
            MatchMode::IndexOnly => {
                if let Some(f) = idx.by_index(frame.index) {
                    info!("Frame {:5}: repaired via index", frame.index);
                    st.rep_idx += 1; Some(f.data.clone())
                } else { None }
            }
            MatchMode::TimecodeOnly => {
                if let Some(f) = frame.timecode.as_ref().and_then(|tc| idx.by_timecode(tc)) {
                    info!("Frame {:5}: repaired via timecode {}", frame.index, frame.timecode.as_ref().unwrap());
                    st.rep_tc += 1; Some(f.data.clone())
                } else { None }
            }
        };

        if let Some(data) = replacement {
            out.push(data);
        } else {
            warn!("Frame {:5}: no healthy replacement — fallback={:?}", frame.index, fallback);
            match fallback {
                Fallback::Keep   => { st.fb_keep   += 1; out.push(frame.data.clone()); }
                Fallback::Freeze => {
                    st.fb_freeze += 1;
                    let last = out.last().cloned().unwrap_or_else(|| frame.data.clone());
                    out.push(last);
                }
                Fallback::Blank  => { st.fb_blank  += 1; out.push(make_blank_frame(frame.data.len())); }
            }
        }
    }
    (out, st)
}

// ─────────────────────────────────────────────────────────────────────────────
// AVI WRITER (in-place patch)
// ─────────────────────────────────────────────────────────────────────────────

fn write_repaired(main: &ParsedAvi, repaired: &[Vec<u8>], output: &Path) -> Result<()> {
    if repaired.len() != main.frame_regions.len() {
        bail!("Frame count mismatch: {} vs {}", repaired.len(), main.frame_regions.len());
    }
    let mut raw = main.raw.clone();
    for (i, &(off, sz)) in main.frame_regions.iter().enumerate() {
        let nf = &repaired[i];
        if nf.len() != sz {
            bail!("Frame {} size mismatch: {} vs {} bytes. Cannot mix NTSC and PAL.", i, sz, nf.len());
        }
        raw[off..off+sz].copy_from_slice(nf);
    }
    let mut f = fs::File::create(output)
        .with_context(|| format!("Cannot create {:?}", output))?;
    f.write_all(&raw)
        .with_context(|| format!("Write error {:?}", output))?;
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// MAIN
// ─────────────────────────────────────────────────────────────────────────────

fn main() -> Result<()> {
    let cli = Cli::parse();
    env_logger::Builder::from_env(
        env_logger::Env::default().default_filter_or(match cli.verbose {
            0 => "info", 1 => "debug", _ => "trace"
        })
    ).format_timestamp(None).init();

    // ── RIFF dump mode ───────────────────────────────────────────────────────
    if cli.dump_riff {
        let path = &cli.inputs[0];
        let raw = fs::read(path).with_context(|| format!("Cannot read {:?}", path))?;
        if raw.len() < 12 || &raw[0..4] != b"RIFF" {
            bail!("{:?}: not a RIFF file", path);
        }
        println!("RIFF structure of {:?} ({} bytes total):", path, raw.len());
        println!("RIFF 'AVI ' ({} bytes)", u32le(&raw, 4));
        dump_riff(&raw, 12, raw.len(), 1);
        return Ok(());
    }

    // ── Type-1 → Type-2 conversion mode ─────────────────────────────────────
    if cli.to_type2 {
        if cli.inputs.len() != 1 {
            bail!("--to-type2 requires exactly one input file");
        }
        let path = &cli.inputs[0];
        info!("Loading {:?} for Type-1→Type-2 conversion...", path);
        let avi = parse_avi(path)?;
        info!("  {} frames ({:?})", avi.frames.len(), avi.kind);
        if avi.kind != AviKind::Type1 {
            // It parsed as Type-2 already — might just need reprocessing.
            warn!("File appears to be Type-2 already. Converting anyway (rewriting clean AVI).");
        }
        info!("Writing Type-2 AVI to {:?}...", cli.output);
        convert_type1_to_type2(&avi, &cli.output)?;
        println!("Converted {} frames → {:?}", avi.frames.len(), cli.output);
        return Ok(());
    }

    // ── Normal repair mode ───────────────────────────────────────────────────
    if cli.inputs.len() < 2 {
        bail!("Repair mode requires at least 2 input files. Use --dump-riff for single-file inspection.");
    }

    info!("Loading {} files...", cli.inputs.len());
    let mut loaded: Vec<(PathBuf, ParsedAvi)> = Vec::new();
    for path in &cli.inputs {
        info!("  {:?}", path);
        let avi = parse_avi(path)?;
        let corrupt = avi.frames.iter().filter(|f| !f.healthy).count();
        info!("    {} frames ({:?}), {} corrupt ({:.1}%)",
              avi.frames.len(), avi.kind, corrupt,
              if avi.frames.is_empty() { 0.0 } else { corrupt as f64 / avi.frames.len() as f64 * 100.0 });
        loaded.push((path.clone(), avi));
    }

    // Select main stream
    let main_path: PathBuf = if let Some(ref mp) = cli.main_stream {
        if !loaded.iter().any(|(p,_)| p == mp) {
            bail!("--main-stream {:?} not found in inputs", mp);
        }
        mp.clone()
    } else {
        let lens: Vec<usize> = loaded.iter().map(|(_,a)| a.frames.len()).collect();
        if !lens.windows(2).all(|w| w[0]==w[1]) {
            eprintln!("\nERROR: Input streams are different in lengths:");
            for (p,a) in &loaded {
                eprintln!("  {:?} — {} frames", p.file_name().unwrap_or_default(), a.frames.len());
            }
            eprintln!("\nPlease specify the main stream with --main-stream <PATH>, and check\nthat the selected videos are roughly the same tape capture.");
            std::process::exit(1);
        }
        let worst = loaded.iter()
            .max_by_key(|(_,a)| a.frames.iter().filter(|f| !f.healthy).count())
            .map(|(p,_)| p.clone()).unwrap();
        info!("Auto-selected main (most corrupt): {:?}", worst);
        worst
    };

    let main_pos = loaded.iter().position(|(p,_)| p == &main_path).unwrap();
    let (_, main_avi) = loaded.remove(main_pos);
    let spare_avis: Vec<ParsedAvi> = loaded.into_iter().map(|(_,a)| a).collect();

    let corrupt = main_avi.frames.iter().filter(|f| !f.healthy).count();
    info!("Main: {} frames, {} corrupt", main_avi.frames.len(), corrupt);

    if corrupt == 0 {
        info!("No corrupt frames — writing copy of main.");
        let frames: Vec<Vec<u8>> = main_avi.frames.iter().map(|f| f.data.clone()).collect();
        write_repaired(&main_avi, &frames, &cli.output)?;
        println!("No corrupt frames found. Output: {:?}", cli.output);
        return Ok(());
    }

    info!("Repairing {} corrupt frames using {} spare file(s)...", corrupt, spare_avis.len());
    let (repaired, st) = repair(&main_avi, &spare_avis, &cli.match_mode, &cli.fallback);

    info!("Writing {:?}...", cli.output);
    write_repaired(&main_avi, &repaired, &cli.output)?;

    let rep = st.rep_tc + st.rep_idx;
    let fbt = st.fb_keep + st.fb_freeze + st.fb_blank;
    println!();
    println!("=== dvrepair summary ===");
    println!("Total frames:       {:>7}", st.total);
    println!("Corrupt frames:     {:>7}", st.corrupt);
    println!("Repaired:           {:>7}", rep);
    println!("  via timecode:     {:>7}", st.rep_tc);
    println!("  via index:        {:>7}", st.rep_idx);
    println!("Fallback applied:   {:>7}", fbt);
    if fbt > 0 {
        println!("  kept corrupt:     {:>7}", st.fb_keep);
        println!("  freeze-framed:    {:>7}", st.fb_freeze);
        println!("  blanked:          {:>7}", st.fb_blank);
    }
    if st.corrupt > 0 {
        println!("Repair rate:     {:>8.1}%", rep as f64 / st.corrupt as f64 * 100.0);
    }
    println!("Output: {:?}", cli.output);

    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// TESTS
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test] fn bcd_valid() {
        assert_eq!(bcd(0x00), Some(0));
        assert_eq!(bcd(0x29), Some(29));
        assert_eq!(bcd(0x59), Some(59));
    }
    #[test] fn bcd_invalid() {
        assert_eq!(bcd(0xFF), None);
        assert_eq!(bcd(0x9A), None);
        assert_eq!(bcd(0xA0), None);
    }
    #[test] fn tc_decode() {
        let p = [0x13u8, 0x12, 0x45, 0x23, 0x01];
        let tc = decode_tc_pack(&p).unwrap();
        assert_eq!(tc.to_string(), "01:23:45;12");
    }
    #[test] fn tc_ff_none() {
        assert!(decode_tc_pack(&[0x13u8, 0xFF, 0xFF, 0xFF, 0xFF]).is_none());
    }
    #[test] fn assess_wrong_size() {
        let (h, e) = assess_frame(&[0u8; 1234]);
        assert!(!h); assert!(e > 0);
    }
    #[test] fn assess_ntsc_zeros_healthy() {
        let f = vec![0u8; 120_000];
        let (h, e) = assess_frame(&f);
        assert!(h); assert_eq!(e, 0);
    }
    #[test] fn assess_ff_payload_corrupt() {
        let mut f = vec![0u8; 120_000];
        let off = 6 * DIF_BLOCK;
        f[off] = (SCT_VIDEO << 5) | 0;
        for b in &mut f[off+4..off+DIF_BLOCK] { *b = 0xFF; }
        let (h, e) = assess_frame(&f);
        assert!(!h); assert!(e > 0);
    }
    #[test] fn blank_sizes() {
        assert_eq!(make_blank_frame(120_000).len(), 120_000);
        assert_eq!(make_blank_frame(144_000).len(), 144_000);
    }
    #[test] fn blank_is_healthy() {
        let f = make_blank_frame(120_000);
        let (h, e) = assess_frame(&f);
        assert!(h); assert_eq!(e, 0);
    }
    #[test] fn n_seq_values() {
        assert_eq!(n_seq(120_000), Some(10));
        assert_eq!(n_seq(144_000), Some(12));
        assert_eq!(n_seq(99_999), None);
    }
    #[test] fn is_dv_chunk_type2() {
        assert!(is_dv_data_chunk(b"00dc", &AviKind::Type2));
        assert!(is_dv_data_chunk(b"00db", &AviKind::Type2));
        assert!(is_dv_data_chunk(b"01dc", &AviKind::Type2));
        assert!(!is_dv_data_chunk(b"01wb", &AviKind::Type2));
        assert!(!is_dv_data_chunk(b"LIST", &AviKind::Type2));
    }
    #[test] fn is_dv_chunk_type1() {
        assert!(is_dv_data_chunk(b"00__", &AviKind::Type1));
        assert!(is_dv_data_chunk(b"00dc", &AviKind::Type1));
        assert!(!is_dv_data_chunk(b"01wb", &AviKind::Type1));
    }
}
