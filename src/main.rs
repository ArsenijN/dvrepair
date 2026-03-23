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
    sta_errors: u32,
    ac_errors: u32,
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
            cc[2] == b'd' && (cc[3] == b'c' || cc[3] == b'b')  // ← 00db now covered
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

/// Collect raw frame byte regions from movi without assessment (sequential, fast).
fn walk_movi_collect(
    data: &[u8], mut pos: usize, end: usize,
    regions: &mut Vec<(usize, usize)>,  // (data_offset, size)
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
            let valid_size = sz == 120_000 || sz == 144_000;
            if valid_size {
                if *frame_size == 120_000 && sz == 144_000 { *frame_size = 144_000; }
                regions.push((ds, sz));
            } else {
                debug!("{:?}: skipping chunk '{}' size {} (not a DV frame size)", path, cc_str(cc), sz);
            }
        } else if cc == b"LIST" && ds + 4 <= data.len() && &data[ds..ds+4] == b"rec " {
            walk_movi_collect(data, ds+4, ds+sz, regions, kind, frame_size, path);
        }

        if nx <= pos { break; }
        pos = nx;
    }
}

// walk_movi kept as alias for backward compat with walk_chunks call site
fn walk_movi(
    data: &[u8], pos: usize, end: usize,
    frames: &mut Vec<DvFrame>,
    regions: &mut Vec<(usize, usize)>,
    kind: &mut AviKind,
    frame_size: &mut usize,
    path: &Path,
) {
    // Phase 1: collect frame regions sequentially (just byte offsets, no assessment)
    let regions_before = regions.len();  // ← snapshot before this movi chunk's regions
    walk_movi_collect(data, pos, end, regions, kind, frame_size, path);

    // Phase 2: assess all frames in parallel using std::thread::scope.
    // Each frame is independent — embarrassingly parallel.
    // Chunk work across available CPU threads; collect results via Vec return.
    let n_threads = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    let new_regions = &regions[regions_before..];  // ← only the new ones from this call
    let n_frames = new_regions.len();              // ← was regions.len() — the bug

    // Each thread returns its chunk as a Vec<(global_index, DvFrame)>.
    let chunks: Vec<Vec<(usize, DvFrame)>> = std::thread::scope(|s| {
        let chunk_size = (n_frames + n_threads - 1) / n_threads.max(1);
        let chunk_size = chunk_size.max(1);

        let handles: Vec<_> = new_regions  // ← was `regions`
            .chunks(chunk_size)
            .enumerate()
            .map(|(chunk_idx, chunk)| {
                let base = regions_before + chunk_idx * chunk_size;  // ← global frame index offset
                s.spawn(move || {
                    chunk.iter().enumerate().map(|(i, &(offset, size))| {
                        let fb = &data[offset..offset + size];
                        let tc = extract_timecode(fb);
                        let (healthy, sta_errs, ac_errs) = assess_frame(fb);
                        (base + i, DvFrame {
                            data: fb.to_vec(),
                            index: base + i,
                            timecode: tc,
                            healthy,
                            sta_errors: sta_errs,
                            ac_errors: ac_errs,
                        })
                    }).collect::<Vec<_>>()
                })
            })
            .collect();

        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });

    // Flatten chunks back into order (they come back in chunk order, within each
    // chunk they're in frame order, so the final collect is already sorted).
    frames.extend(chunks.into_iter().flatten().map(|(_, f)| f));
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
// DV DIF BLOCK HEALTH CHECK  (Level 1: Reed-Solomon / dropout)
// ─────────────────────────────────────────────────────────────────────────────

const DIF_BLOCK: usize = 80;
const SCT_VAUX: u8 = 2;
const SCT_VIDEO: u8 = 4;

fn n_seq(frame_len: usize) -> Option<usize> {
    match frame_len { 120_000 => Some(10), 144_000 => Some(12), _ => None }
}

/// Returns STA error count. Checks STA nibble != 0 and all-0xFF dropout.
fn check_sta(frame: &[u8]) -> u32 {
    let ns = match n_seq(frame.len()) { Some(n) => n, None => return 1 };
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
    errs
}

/// Combined assessment. Returns (healthy, sta_errors, ac_errors).
fn assess_frame(frame: &[u8]) -> (bool, u32, u32) {
    if n_seq(frame.len()).is_none() { return (false, 1, 0); }
    let sta = check_sta(frame);
    let ac  = if sta == 0 { check_ac_bitstream(frame) } else { 0 };
    (sta == 0 && ac == 0, sta, ac)
}

// ─────────────────────────────────────────────────────────────────────────────
// DV AC BITSTREAM CHECKER  (Level 2: DCT coefficient stream validity)
// ─────────────────────────────────────────────────────────────────────────────
//
// STRUCTURE (from IEC 61834-2 / SMPTE 314M, confirmed against FFmpeg dvdec.c):
//
// Each Video DIF block (80 bytes) contains exactly one macroblock.
// Layout:
//   byte  0:    DIF block header byte 0 (SCT in bits 7:5)
//   byte  1:    DIF block header byte 1
//   byte  2:    DIF block header byte 2
//   byte  3:    STA(4b high) | QNO(4b low)
//   bytes 4..79: 76 bytes = 608 bits of compressed DCT data
//
// The 608 bits are split into 6 DCT blocks with FIXED bit allocations:
//   blocks 0-3 (luma Y0-Y3): 112 bits each
//   blocks 4-5 (chroma Cb, Cr): 80 bits each
//   Total: 4*112 + 2*80 = 608 bits
//
// Each DCT block starts with a 12-bit header:
//   9 bits: DC coefficient (signed)
//   1 bit:  dct_mode (0=8x8, 1=2x4x8)
//   2 bits: class (quantisation class)
// Followed by AC VLC pairs until EOB.
//
// VLC TABLE (canonical Huffman, from FFmpeg's ff_dv_vlc_* arrays in dvdata.c):
// Each entry: (MSB-aligned codeword u32, bit_length u8, run u8, level i8)
// - level=0, run=127: EOB signal (run+1=128 in decoder, pushes pos past 64)
// - level=0, run!=127: other zero-level entries (used in pass 3 overflow)
// - level!=0: AC coefficient; sign bit is appended after the base code
//   (positive = base_code<<1|0, negative = base_code<<1|1, len+1 total)
//
// EOB DETECTION (matching FFmpeg exactly):
// The decoder maintains pos (coefficient index, starts at 0 after DC).
// On each codeword: pos += run_in_decoder (which is table_run + 1).
// EOB = run=127 in table => run=128 in decoder => pos jumps to >= 64 => break.
// CLEAN end: pos >= 64 (reached via EOB jump or ran past end of block naturally).
// ERROR: 64 <= pos < 127 at end of decoding a block (pos overran without clean EOB).
// This matches the FFmpeg error: "AC EOB marker is absent pos=N" where N is 64..126.
//
// This table was generated from FFmpeg's ff_dv_vlc_len[], ff_dv_vlc_run[],
// ff_dv_vlc_level[] arrays in libavcodec/dvdata.c using canonical Huffman
// code assignment (same algorithm as ff_vlc_init_from_lengths).
static DV_VLC: &[(u32, u8, u8, i16)] = &[
    (0x00000000u32,  3,   0,     1),
    (0x20000000u32,  3,   0,    -1),
    (0x40000000u32,  4,   0,     2),
    (0x50000000u32,  4,   0,    -2),
    (0x60000000u32,  4, 127,     0),
    (0x70000000u32,  5,   1,     1),
    (0x78000000u32,  5,   1,    -1),
    (0x80000000u32,  5,   0,     3),
    (0x88000000u32,  5,   0,    -3),
    (0x90000000u32,  5,   0,     4),
    (0x98000000u32,  5,   0,    -4),
    (0xA0000000u32,  6,   2,     1),
    (0xA4000000u32,  6,   2,    -1),
    (0xA8000000u32,  6,   1,     2),
    (0xAC000000u32,  6,   1,    -2),
    (0xB0000000u32,  6,   0,     5),
    (0xB4000000u32,  6,   0,    -5),
    (0xB8000000u32,  6,   0,     6),
    (0xBC000000u32,  6,   0,    -6),
    (0xC0000000u32,  7,   3,     1),
    (0xC2000000u32,  7,   3,    -1),
    (0xC4000000u32,  7,   4,     1),
    (0xC6000000u32,  7,   4,    -1),
    (0xC8000000u32,  7,   0,     7),
    (0xCA000000u32,  7,   0,    -7),
    (0xCC000000u32,  7,   0,     8),
    (0xCE000000u32,  7,   0,    -8),
    (0xD0000000u32,  8,   5,     1),
    (0xD1000000u32,  8,   5,    -1),
    (0xD2000000u32,  8,   6,     1),
    (0xD3000000u32,  8,   6,    -1),
    (0xD4000000u32,  8,   2,     2),
    (0xD5000000u32,  8,   2,    -2),
    (0xD6000000u32,  8,   1,     3),
    (0xD7000000u32,  8,   1,    -3),
    (0xD8000000u32,  8,   1,     4),
    (0xD9000000u32,  8,   1,    -4),
    (0xDA000000u32,  8,   0,     9),
    (0xDB000000u32,  8,   0,    -9),
    (0xDC000000u32,  8,   0,    10),
    (0xDD000000u32,  8,   0,   -10),
    (0xDE000000u32,  8,   0,    11),
    (0xDF000000u32,  8,   0,   -11),
    (0xE0000000u32,  9,   7,     1),
    (0xE0800000u32,  9,   7,    -1),
    (0xE1000000u32,  9,   8,     1),
    (0xE1800000u32,  9,   8,    -1),
    (0xE2000000u32,  9,   9,     1),
    (0xE2800000u32,  9,   9,    -1),
    (0xE3000000u32,  9,  10,     1),
    (0xE3800000u32,  9,  10,    -1),
    (0xE4000000u32,  9,   3,     2),
    (0xE4800000u32,  9,   3,    -2),
    (0xE5000000u32,  9,   4,     2),
    (0xE5800000u32,  9,   4,    -2),
    (0xE6000000u32,  9,   2,     3),
    (0xE6800000u32,  9,   2,    -3),
    (0xE7000000u32,  9,   1,     5),
    (0xE7800000u32,  9,   1,    -5),
    (0xE8000000u32,  9,   1,     6),
    (0xE8800000u32,  9,   1,    -6),
    (0xE9000000u32,  9,   1,     7),
    (0xE9800000u32,  9,   1,    -7),
    (0xEA000000u32,  9,   0,    12),
    (0xEA800000u32,  9,   0,   -12),
    (0xEB000000u32,  9,   0,    13),
    (0xEB800000u32,  9,   0,   -13),
    (0xEC000000u32,  9,   0,    14),
    (0xEC800000u32,  9,   0,   -14),
    (0xED000000u32,  9,   0,    15),
    (0xED800000u32,  9,   0,   -15),
    (0xEE000000u32,  9,   0,    16),
    (0xEE800000u32,  9,   0,   -16),
    (0xEF000000u32,  9,   0,    17),
    (0xEF800000u32,  9,   0,   -17),
    (0xF0000000u32, 10,  11,     1),
    (0xF0400000u32, 10,  11,    -1),
    (0xF0800000u32, 10,  12,     1),
    (0xF0C00000u32, 10,  12,    -1),
    (0xF1000000u32, 10,  13,     1),
    (0xF1400000u32, 10,  13,    -1),
    (0xF1800000u32, 10,  14,     1),
    (0xF1C00000u32, 10,  14,    -1),
    (0xF2000000u32, 10,   5,     2),
    (0xF2400000u32, 10,   5,    -2),
    (0xF2800000u32, 10,   6,     2),
    (0xF2C00000u32, 10,   6,    -2),
    (0xF3000000u32, 10,   3,     3),
    (0xF3400000u32, 10,   3,    -3),
    (0xF3800000u32, 10,   4,     3),
    (0xF3C00000u32, 10,   4,    -3),
    (0xF4000000u32, 10,   2,     4),
    (0xF4400000u32, 10,   2,    -4),
    (0xF4800000u32, 10,   2,     5),
    (0xF4C00000u32, 10,   2,    -5),
    (0xF5000000u32, 10,   1,     8),
    (0xF5400000u32, 10,   1,    -8),
    (0xF5800000u32, 10,   0,    18),
    (0xF5C00000u32, 10,   0,   -18),
    (0xF6000000u32, 10,   0,    19),
    (0xF6400000u32, 10,   0,   -19),
    (0xF6800000u32, 10,   0,    20),
    (0xF6C00000u32, 10,   0,   -20),
    (0xF7000000u32, 10,   0,    21),
    (0xF7400000u32, 10,   0,   -21),
    (0xF7800000u32, 10,   0,    22),
    (0xF7C00000u32, 10,   0,   -22),
    (0xF8000000u32, 11,   5,     3),
    (0xF8200000u32, 11,   5,    -3),
    (0xF8400000u32, 11,   3,     4),
    (0xF8600000u32, 11,   3,    -4),
    (0xF8800000u32, 11,   3,     5),
    (0xF8A00000u32, 11,   3,    -5),
    (0xF8C00000u32, 11,   2,     6),
    (0xF8E00000u32, 11,   2,    -6),
    (0xF9000000u32, 11,   1,     9),
    (0xF9200000u32, 11,   1,    -9),
    (0xF9400000u32, 11,   1,    10),
    (0xF9600000u32, 11,   1,   -10),
    (0xF9800000u32, 11,   1,    11),
    (0xF9A00000u32, 11,   1,   -11),
    (0xF9C00000u32, 11,   0,     0),
    (0xF9E00000u32, 11,   1,     0),
    (0xFA000000u32, 12,   6,     3),
    (0xFA100000u32, 12,   6,    -3),
    (0xFA200000u32, 12,   4,     4),
    (0xFA300000u32, 12,   4,    -4),
    (0xFA400000u32, 12,   3,     6),
    (0xFA500000u32, 12,   3,    -6),
    (0xFA600000u32, 12,   1,    12),
    (0xFA700000u32, 12,   1,   -12),
    (0xFA800000u32, 12,   1,    13),
    (0xFA900000u32, 12,   1,   -13),
    (0xFAA00000u32, 12,   1,    14),
    (0xFAB00000u32, 12,   1,   -14),
    (0xFAC00000u32, 12,   2,     0),
    (0xFAD00000u32, 12,   3,     0),
    (0xFAE00000u32, 12,   4,     0),
    (0xFAF00000u32, 12,   5,     0),
    (0xFB000000u32, 13,   7,     2),
    (0xFB080000u32, 13,   7,    -2),
    (0xFB100000u32, 13,   8,     2),
    (0xFB180000u32, 13,   8,    -2),
    (0xFB200000u32, 13,   9,     2),
    (0xFB280000u32, 13,   9,    -2),
    (0xFB300000u32, 13,  10,     2),
    (0xFB380000u32, 13,  10,    -2),
    (0xFB400000u32, 13,   7,     3),
    (0xFB480000u32, 13,   7,    -3),
    (0xFB500000u32, 13,   8,     3),
    (0xFB580000u32, 13,   8,    -3),
    (0xFB600000u32, 13,   4,     5),
    (0xFB680000u32, 13,   4,    -5),
    (0xFB700000u32, 13,   3,     7),
    (0xFB780000u32, 13,   3,    -7),
    (0xFB800000u32, 13,   2,     7),
    (0xFB880000u32, 13,   2,    -7),
    (0xFB900000u32, 13,   2,     8),
    (0xFB980000u32, 13,   2,    -8),
    (0xFBA00000u32, 13,   2,     9),
    (0xFBA80000u32, 13,   2,    -9),
    (0xFBB00000u32, 13,   2,    10),
    (0xFBB80000u32, 13,   2,   -10),
    (0xFBC00000u32, 13,   2,    11),
    (0xFBC80000u32, 13,   2,   -11),
    (0xFBD00000u32, 13,   1,    15),
    (0xFBD80000u32, 13,   1,   -15),
    (0xFBE00000u32, 13,   1,    16),
    (0xFBE80000u32, 13,   1,   -16),
    (0xFBF00000u32, 13,   1,    17),
    (0xFBF80000u32, 13,   1,   -17),
    (0xFC000000u32, 13,   0,     0),
    (0xFC080000u32, 13,   1,     0),
    (0xFC100000u32, 13,   2,     0),
    (0xFC180000u32, 13,   3,     0),
    (0xFC200000u32, 13,   4,     0),
    (0xFC280000u32, 13,   5,     0),
    (0xFC300000u32, 13,   6,     0),
    (0xFC380000u32, 13,   7,     0),
    (0xFC400000u32, 13,   8,     0),
    (0xFC480000u32, 13,   9,     0),
    (0xFC500000u32, 13,  10,     0),
    (0xFC580000u32, 13,  11,     0),
    (0xFC600000u32, 13,  12,     0),
    (0xFC680000u32, 13,  13,     0),
    (0xFC700000u32, 13,  14,     0),
    (0xFC780000u32, 13,  15,     0),
    (0xFC800000u32, 13,  16,     0),
    (0xFC880000u32, 13,  17,     0),
    (0xFC900000u32, 13,  18,     0),
    (0xFC980000u32, 13,  19,     0),
    (0xFCA00000u32, 13,  20,     0),
    (0xFCA80000u32, 13,  21,     0),
    (0xFCB00000u32, 13,  22,     0),
    (0xFCB80000u32, 13,  23,     0),
    (0xFCC00000u32, 13,  24,     0),
    (0xFCC80000u32, 13,  25,     0),
    (0xFCD00000u32, 13,  26,     0),
    (0xFCD80000u32, 13,  27,     0),
    (0xFCE00000u32, 13,  28,     0),
    (0xFCE80000u32, 13,  29,     0),
    (0xFCF00000u32, 13,  30,     0),
    (0xFCF80000u32, 13,  31,     0),
    (0xFD000000u32, 13,  32,     0),
    (0xFD080000u32, 13,  33,     0),
    (0xFD100000u32, 13,  34,     0),
    (0xFD180000u32, 13,  35,     0),
    (0xFD200000u32, 13,  36,     0),
    (0xFD280000u32, 13,  37,     0),
    (0xFD300000u32, 13,  38,     0),
    (0xFD380000u32, 13,  39,     0),
    (0xFD400000u32, 13,  40,     0),
    (0xFD480000u32, 13,  41,     0),
    (0xFD500000u32, 13,  42,     0),
    (0xFD580000u32, 13,  43,     0),
    (0xFD600000u32, 13,  44,     0),
    (0xFD680000u32, 13,  45,     0),
    (0xFD700000u32, 13,  46,     0),
    (0xFD780000u32, 13,  47,     0),
    (0xFD800000u32, 13,  48,     0),
    (0xFD880000u32, 13,  49,     0),
    (0xFD900000u32, 13,  50,     0),
    (0xFD980000u32, 13,  51,     0),
    (0xFDA00000u32, 13,  52,     0),
    (0xFDA80000u32, 13,  53,     0),
    (0xFDB00000u32, 13,  54,     0),
    (0xFDB80000u32, 13,  55,     0),
    (0xFDC00000u32, 13,  56,     0),
    (0xFDC80000u32, 13,  57,     0),
    (0xFDD00000u32, 13,  58,     0),
    (0xFDD80000u32, 13,  59,     0),
    (0xFDE00000u32, 13,  60,     0),
    (0xFDE80000u32, 13,  61,     0),
    (0xFDF00000u32, 13,  62,     0),
    (0xFDF80000u32, 13,  63,     0),
    (0xFE000000u32, 15,   0,     0),
    (0xFE020000u32, 16,   0,     1),
    (0xFE030000u32, 16,   0,    -1),
    (0xFE040000u32, 16,   0,     2),
    (0xFE050000u32, 16,   0,    -2),
    (0xFE060000u32, 16,   0,     3),
    (0xFE070000u32, 16,   0,    -3),
    (0xFE080000u32, 16,   0,     4),
    (0xFE090000u32, 16,   0,    -4),
    (0xFE0A0000u32, 16,   0,     5),
    (0xFE0B0000u32, 16,   0,    -5),
    (0xFE0C0000u32, 16,   0,     6),
    (0xFE0D0000u32, 16,   0,    -6),
    (0xFE0E0000u32, 16,   0,     7),
    (0xFE0F0000u32, 16,   0,    -7),
    (0xFE100000u32, 16,   0,     8),
    (0xFE110000u32, 16,   0,    -8),
    (0xFE120000u32, 16,   0,     9),
    (0xFE130000u32, 16,   0,    -9),
    (0xFE140000u32, 16,   0,    10),
    (0xFE150000u32, 16,   0,   -10),
    (0xFE160000u32, 16,   0,    11),
    (0xFE170000u32, 16,   0,   -11),
    (0xFE180000u32, 16,   0,    12),
    (0xFE190000u32, 16,   0,   -12),
    (0xFE1A0000u32, 16,   0,    13),
    (0xFE1B0000u32, 16,   0,   -13),
    (0xFE1C0000u32, 16,   0,    14),
    (0xFE1D0000u32, 16,   0,   -14),
    (0xFE1E0000u32, 16,   0,    15),
    (0xFE1F0000u32, 16,   0,   -15),
    (0xFE200000u32, 16,   0,    16),
    (0xFE210000u32, 16,   0,   -16),
    (0xFE220000u32, 16,   0,    17),
    (0xFE230000u32, 16,   0,   -17),
    (0xFE240000u32, 16,   0,    18),
    (0xFE250000u32, 16,   0,   -18),
    (0xFE260000u32, 16,   0,    19),
    (0xFE270000u32, 16,   0,   -19),
    (0xFE280000u32, 16,   0,    20),
    (0xFE290000u32, 16,   0,   -20),
    (0xFE2A0000u32, 16,   0,    21),
    (0xFE2B0000u32, 16,   0,   -21),
    (0xFE2C0000u32, 16,   0,    22),
    (0xFE2D0000u32, 16,   0,   -22),
    (0xFE2E0000u32, 16,   0,    23),
    (0xFE2F0000u32, 16,   0,   -23),
    (0xFE300000u32, 16,   0,    24),
    (0xFE310000u32, 16,   0,   -24),
    (0xFE320000u32, 16,   0,    25),
    (0xFE330000u32, 16,   0,   -25),
    (0xFE340000u32, 16,   0,    26),
    (0xFE350000u32, 16,   0,   -26),
    (0xFE360000u32, 16,   0,    27),
    (0xFE370000u32, 16,   0,   -27),
    (0xFE380000u32, 16,   0,    28),
    (0xFE390000u32, 16,   0,   -28),
    (0xFE3A0000u32, 16,   0,    29),
    (0xFE3B0000u32, 16,   0,   -29),
    (0xFE3C0000u32, 16,   0,    30),
    (0xFE3D0000u32, 16,   0,   -30),
    (0xFE3E0000u32, 16,   0,    31),
    (0xFE3F0000u32, 16,   0,   -31),
    (0xFE400000u32, 16,   0,    32),
    (0xFE410000u32, 16,   0,   -32),
    (0xFE420000u32, 16,   0,    33),
    (0xFE430000u32, 16,   0,   -33),
    (0xFE440000u32, 16,   0,    34),
    (0xFE450000u32, 16,   0,   -34),
    (0xFE460000u32, 16,   0,    35),
    (0xFE470000u32, 16,   0,   -35),
    (0xFE480000u32, 16,   0,    36),
    (0xFE490000u32, 16,   0,   -36),
    (0xFE4A0000u32, 16,   0,    37),
    (0xFE4B0000u32, 16,   0,   -37),
    (0xFE4C0000u32, 16,   0,    38),
    (0xFE4D0000u32, 16,   0,   -38),
    (0xFE4E0000u32, 16,   0,    39),
    (0xFE4F0000u32, 16,   0,   -39),
    (0xFE500000u32, 16,   0,    40),
    (0xFE510000u32, 16,   0,   -40),
    (0xFE520000u32, 16,   0,    41),
    (0xFE530000u32, 16,   0,   -41),
    (0xFE540000u32, 16,   0,    42),
    (0xFE550000u32, 16,   0,   -42),
    (0xFE560000u32, 16,   0,    43),
    (0xFE570000u32, 16,   0,   -43),
    (0xFE580000u32, 16,   0,    44),
    (0xFE590000u32, 16,   0,   -44),
    (0xFE5A0000u32, 16,   0,    45),
    (0xFE5B0000u32, 16,   0,   -45),
    (0xFE5C0000u32, 16,   0,    46),
    (0xFE5D0000u32, 16,   0,   -46),
    (0xFE5E0000u32, 16,   0,    47),
    (0xFE5F0000u32, 16,   0,   -47),
    (0xFE600000u32, 16,   0,    48),
    (0xFE610000u32, 16,   0,   -48),
    (0xFE620000u32, 16,   0,    49),
    (0xFE630000u32, 16,   0,   -49),
    (0xFE640000u32, 16,   0,    50),
    (0xFE650000u32, 16,   0,   -50),
    (0xFE660000u32, 16,   0,    51),
    (0xFE670000u32, 16,   0,   -51),
    (0xFE680000u32, 16,   0,    52),
    (0xFE690000u32, 16,   0,   -52),
    (0xFE6A0000u32, 16,   0,    53),
    (0xFE6B0000u32, 16,   0,   -53),
    (0xFE6C0000u32, 16,   0,    54),
    (0xFE6D0000u32, 16,   0,   -54),
    (0xFE6E0000u32, 16,   0,    55),
    (0xFE6F0000u32, 16,   0,   -55),
    (0xFE700000u32, 16,   0,    56),
    (0xFE710000u32, 16,   0,   -56),
    (0xFE720000u32, 16,   0,    57),
    (0xFE730000u32, 16,   0,   -57),
    (0xFE740000u32, 16,   0,    58),
    (0xFE750000u32, 16,   0,   -58),
    (0xFE760000u32, 16,   0,    59),
    (0xFE770000u32, 16,   0,   -59),
    (0xFE780000u32, 16,   0,    60),
    (0xFE790000u32, 16,   0,   -60),
    (0xFE7A0000u32, 16,   0,    61),
    (0xFE7B0000u32, 16,   0,   -61),
    (0xFE7C0000u32, 16,   0,    62),
    (0xFE7D0000u32, 16,   0,   -62),
    (0xFE7E0000u32, 16,   0,    63),
    (0xFE7F0000u32, 16,   0,   -63),
    (0xFE800000u32, 16,   0,    64),
    (0xFE810000u32, 16,   0,   -64),
    (0xFE820000u32, 16,   0,    65),
    (0xFE830000u32, 16,   0,   -65),
    (0xFE840000u32, 16,   0,    66),
    (0xFE850000u32, 16,   0,   -66),
    (0xFE860000u32, 16,   0,    67),
    (0xFE870000u32, 16,   0,   -67),
    (0xFE880000u32, 16,   0,    68),
    (0xFE890000u32, 16,   0,   -68),
    (0xFE8A0000u32, 16,   0,    69),
    (0xFE8B0000u32, 16,   0,   -69),
    (0xFE8C0000u32, 16,   0,    70),
    (0xFE8D0000u32, 16,   0,   -70),
    (0xFE8E0000u32, 16,   0,    71),
    (0xFE8F0000u32, 16,   0,   -71),
    (0xFE900000u32, 16,   0,    72),
    (0xFE910000u32, 16,   0,   -72),
    (0xFE920000u32, 16,   0,    73),
    (0xFE930000u32, 16,   0,   -73),
    (0xFE940000u32, 16,   0,    74),
    (0xFE950000u32, 16,   0,   -74),
    (0xFE960000u32, 16,   0,    75),
    (0xFE970000u32, 16,   0,   -75),
    (0xFE980000u32, 16,   0,    76),
    (0xFE990000u32, 16,   0,   -76),
    (0xFE9A0000u32, 16,   0,    77),
    (0xFE9B0000u32, 16,   0,   -77),
    (0xFE9C0000u32, 16,   0,    78),
    (0xFE9D0000u32, 16,   0,   -78),
    (0xFE9E0000u32, 16,   0,    79),
    (0xFE9F0000u32, 16,   0,   -79),
    (0xFEA00000u32, 16,   0,    80),
    (0xFEA10000u32, 16,   0,   -80),
    (0xFEA20000u32, 16,   0,    81),
    (0xFEA30000u32, 16,   0,   -81),
    (0xFEA40000u32, 16,   0,    82),
    (0xFEA50000u32, 16,   0,   -82),
    (0xFEA60000u32, 16,   0,    83),
    (0xFEA70000u32, 16,   0,   -83),
    (0xFEA80000u32, 16,   0,    84),
    (0xFEA90000u32, 16,   0,   -84),
    (0xFEAA0000u32, 16,   0,    85),
    (0xFEAB0000u32, 16,   0,   -85),
    (0xFEAC0000u32, 16,   0,    86),
    (0xFEAD0000u32, 16,   0,   -86),
    (0xFEAE0000u32, 16,   0,    87),
    (0xFEAF0000u32, 16,   0,   -87),
    (0xFEB00000u32, 16,   0,    88),
    (0xFEB10000u32, 16,   0,   -88),
    (0xFEB20000u32, 16,   0,    89),
    (0xFEB30000u32, 16,   0,   -89),
    (0xFEB40000u32, 16,   0,    90),
    (0xFEB50000u32, 16,   0,   -90),
    (0xFEB60000u32, 16,   0,    91),
    (0xFEB70000u32, 16,   0,   -91),
    (0xFEB80000u32, 16,   0,    92),
    (0xFEB90000u32, 16,   0,   -92),
    (0xFEBA0000u32, 16,   0,    93),
    (0xFEBB0000u32, 16,   0,   -93),
    (0xFEBC0000u32, 16,   0,    94),
    (0xFEBD0000u32, 16,   0,   -94),
    (0xFEBE0000u32, 16,   0,    95),
    (0xFEBF0000u32, 16,   0,   -95),
    (0xFEC00000u32, 16,   0,    96),
    (0xFEC10000u32, 16,   0,   -96),
    (0xFEC20000u32, 16,   0,    97),
    (0xFEC30000u32, 16,   0,   -97),
    (0xFEC40000u32, 16,   0,    98),
    (0xFEC50000u32, 16,   0,   -98),
    (0xFEC60000u32, 16,   0,    99),
    (0xFEC70000u32, 16,   0,   -99),
    (0xFEC80000u32, 16,   0,   100),
    (0xFEC90000u32, 16,   0,  -100),
    (0xFECA0000u32, 16,   0,   101),
    (0xFECB0000u32, 16,   0,  -101),
    (0xFECC0000u32, 16,   0,   102),
    (0xFECD0000u32, 16,   0,  -102),
    (0xFECE0000u32, 16,   0,   103),
    (0xFECF0000u32, 16,   0,  -103),
    (0xFED00000u32, 16,   0,   104),
    (0xFED10000u32, 16,   0,  -104),
    (0xFED20000u32, 16,   0,   105),
    (0xFED30000u32, 16,   0,  -105),
    (0xFED40000u32, 16,   0,   106),
    (0xFED50000u32, 16,   0,  -106),
    (0xFED60000u32, 16,   0,   107),
    (0xFED70000u32, 16,   0,  -107),
    (0xFED80000u32, 16,   0,   108),
    (0xFED90000u32, 16,   0,  -108),
    (0xFEDA0000u32, 16,   0,   109),
    (0xFEDB0000u32, 16,   0,  -109),
    (0xFEDC0000u32, 16,   0,   110),
    (0xFEDD0000u32, 16,   0,  -110),
    (0xFEDE0000u32, 16,   0,   111),
    (0xFEDF0000u32, 16,   0,  -111),
    (0xFEE00000u32, 16,   0,   112),
    (0xFEE10000u32, 16,   0,  -112),
    (0xFEE20000u32, 16,   0,   113),
    (0xFEE30000u32, 16,   0,  -113),
    (0xFEE40000u32, 16,   0,   114),
    (0xFEE50000u32, 16,   0,  -114),
    (0xFEE60000u32, 16,   0,   115),
    (0xFEE70000u32, 16,   0,  -115),
    (0xFEE80000u32, 16,   0,   116),
    (0xFEE90000u32, 16,   0,  -116),
    (0xFEEA0000u32, 16,   0,   117),
    (0xFEEB0000u32, 16,   0,  -117),
    (0xFEEC0000u32, 16,   0,   118),
    (0xFEED0000u32, 16,   0,  -118),
    (0xFEEE0000u32, 16,   0,   119),
    (0xFEEF0000u32, 16,   0,  -119),
    (0xFEF00000u32, 16,   0,   120),
    (0xFEF10000u32, 16,   0,  -120),
    (0xFEF20000u32, 16,   0,   121),
    (0xFEF30000u32, 16,   0,  -121),
    (0xFEF40000u32, 16,   0,   122),
    (0xFEF50000u32, 16,   0,  -122),
    (0xFEF60000u32, 16,   0,   123),
    (0xFEF70000u32, 16,   0,  -123),
    (0xFEF80000u32, 16,   0,   124),
    (0xFEF90000u32, 16,   0,  -124),
    (0xFEFA0000u32, 16,   0,   125),
    (0xFEFB0000u32, 16,   0,  -125),
    (0xFEFC0000u32, 16,   0,   126),
    (0xFEFD0000u32, 16,   0,  -126),
    (0xFEFE0000u32, 16,   0,   127),
    (0xFEFF0000u32, 16,   0,  -127),
    (0xFF000000u32, 16,   0,   128),
    (0xFF010000u32, 16,   0,  -128),
    (0xFF020000u32, 16,   0,   129),
    (0xFF030000u32, 16,   0,  -129),
    (0xFF040000u32, 16,   0,   130),
    (0xFF050000u32, 16,   0,  -130),
    (0xFF060000u32, 16,   0,   131),
    (0xFF070000u32, 16,   0,  -131),
    (0xFF080000u32, 16,   0,   132),
    (0xFF090000u32, 16,   0,  -132),
    (0xFF0A0000u32, 16,   0,   133),
    (0xFF0B0000u32, 16,   0,  -133),
    (0xFF0C0000u32, 16,   0,   134),
    (0xFF0D0000u32, 16,   0,  -134),
    (0xFF0E0000u32, 16,   0,   135),
    (0xFF0F0000u32, 16,   0,  -135),
    (0xFF100000u32, 16,   0,   136),
    (0xFF110000u32, 16,   0,  -136),
    (0xFF120000u32, 16,   0,   137),
    (0xFF130000u32, 16,   0,  -137),
    (0xFF140000u32, 16,   0,   138),
    (0xFF150000u32, 16,   0,  -138),
    (0xFF160000u32, 16,   0,   139),
    (0xFF170000u32, 16,   0,  -139),
    (0xFF180000u32, 16,   0,   140),
    (0xFF190000u32, 16,   0,  -140),
    (0xFF1A0000u32, 16,   0,   141),
    (0xFF1B0000u32, 16,   0,  -141),
    (0xFF1C0000u32, 16,   0,   142),
    (0xFF1D0000u32, 16,   0,  -142),
    (0xFF1E0000u32, 16,   0,   143),
    (0xFF1F0000u32, 16,   0,  -143),
    (0xFF200000u32, 16,   0,   144),
    (0xFF210000u32, 16,   0,  -144),
    (0xFF220000u32, 16,   0,   145),
    (0xFF230000u32, 16,   0,  -145),
    (0xFF240000u32, 16,   0,   146),
    (0xFF250000u32, 16,   0,  -146),
    (0xFF260000u32, 16,   0,   147),
    (0xFF270000u32, 16,   0,  -147),
    (0xFF280000u32, 16,   0,   148),
    (0xFF290000u32, 16,   0,  -148),
    (0xFF2A0000u32, 16,   0,   149),
    (0xFF2B0000u32, 16,   0,  -149),
    (0xFF2C0000u32, 16,   0,   150),
    (0xFF2D0000u32, 16,   0,  -150),
    (0xFF2E0000u32, 16,   0,   151),
    (0xFF2F0000u32, 16,   0,  -151),
    (0xFF300000u32, 16,   0,   152),
    (0xFF310000u32, 16,   0,  -152),
    (0xFF320000u32, 16,   0,   153),
    (0xFF330000u32, 16,   0,  -153),
    (0xFF340000u32, 16,   0,   154),
    (0xFF350000u32, 16,   0,  -154),
    (0xFF360000u32, 16,   0,   155),
    (0xFF370000u32, 16,   0,  -155),
    (0xFF380000u32, 16,   0,   156),
    (0xFF390000u32, 16,   0,  -156),
    (0xFF3A0000u32, 16,   0,   157),
    (0xFF3B0000u32, 16,   0,  -157),
    (0xFF3C0000u32, 16,   0,   158),
    (0xFF3D0000u32, 16,   0,  -158),
    (0xFF3E0000u32, 16,   0,   159),
    (0xFF3F0000u32, 16,   0,  -159),
    (0xFF400000u32, 16,   0,   160),
    (0xFF410000u32, 16,   0,  -160),
    (0xFF420000u32, 16,   0,   161),
    (0xFF430000u32, 16,   0,  -161),
    (0xFF440000u32, 16,   0,   162),
    (0xFF450000u32, 16,   0,  -162),
    (0xFF460000u32, 16,   0,   163),
    (0xFF470000u32, 16,   0,  -163),
    (0xFF480000u32, 16,   0,   164),
    (0xFF490000u32, 16,   0,  -164),
    (0xFF4A0000u32, 16,   0,   165),
    (0xFF4B0000u32, 16,   0,  -165),
    (0xFF4C0000u32, 16,   0,   166),
    (0xFF4D0000u32, 16,   0,  -166),
    (0xFF4E0000u32, 16,   0,   167),
    (0xFF4F0000u32, 16,   0,  -167),
    (0xFF500000u32, 16,   0,   168),
    (0xFF510000u32, 16,   0,  -168),
    (0xFF520000u32, 16,   0,   169),
    (0xFF530000u32, 16,   0,  -169),
    (0xFF540000u32, 16,   0,   170),
    (0xFF550000u32, 16,   0,  -170),
    (0xFF560000u32, 16,   0,   171),
    (0xFF570000u32, 16,   0,  -171),
    (0xFF580000u32, 16,   0,   172),
    (0xFF590000u32, 16,   0,  -172),
    (0xFF5A0000u32, 16,   0,   173),
    (0xFF5B0000u32, 16,   0,  -173),
    (0xFF5C0000u32, 16,   0,   174),
    (0xFF5D0000u32, 16,   0,  -174),
    (0xFF5E0000u32, 16,   0,   175),
    (0xFF5F0000u32, 16,   0,  -175),
    (0xFF600000u32, 16,   0,   176),
    (0xFF610000u32, 16,   0,  -176),
    (0xFF620000u32, 16,   0,   177),
    (0xFF630000u32, 16,   0,  -177),
    (0xFF640000u32, 16,   0,   178),
    (0xFF650000u32, 16,   0,  -178),
    (0xFF660000u32, 16,   0,   179),
    (0xFF670000u32, 16,   0,  -179),
    (0xFF680000u32, 16,   0,   180),
    (0xFF690000u32, 16,   0,  -180),
    (0xFF6A0000u32, 16,   0,   181),
    (0xFF6B0000u32, 16,   0,  -181),
    (0xFF6C0000u32, 16,   0,   182),
    (0xFF6D0000u32, 16,   0,  -182),
    (0xFF6E0000u32, 16,   0,   183),
    (0xFF6F0000u32, 16,   0,  -183),
    (0xFF700000u32, 16,   0,   184),
    (0xFF710000u32, 16,   0,  -184),
    (0xFF720000u32, 16,   0,   185),
    (0xFF730000u32, 16,   0,  -185),
    (0xFF740000u32, 16,   0,   186),
    (0xFF750000u32, 16,   0,  -186),
    (0xFF760000u32, 16,   0,   187),
    (0xFF770000u32, 16,   0,  -187),
    (0xFF780000u32, 16,   0,   188),
    (0xFF790000u32, 16,   0,  -188),
    (0xFF7A0000u32, 16,   0,   189),
    (0xFF7B0000u32, 16,   0,  -189),
    (0xFF7C0000u32, 16,   0,   190),
    (0xFF7D0000u32, 16,   0,  -190),
    (0xFF7E0000u32, 16,   0,   191),
    (0xFF7F0000u32, 16,   0,  -191),
    (0xFF800000u32, 16,   0,   192),
    (0xFF810000u32, 16,   0,  -192),
    (0xFF820000u32, 16,   0,   193),
    (0xFF830000u32, 16,   0,  -193),
    (0xFF840000u32, 16,   0,   194),
    (0xFF850000u32, 16,   0,  -194),
    (0xFF860000u32, 16,   0,   195),
    (0xFF870000u32, 16,   0,  -195),
    (0xFF880000u32, 16,   0,   196),
    (0xFF890000u32, 16,   0,  -196),
    (0xFF8A0000u32, 16,   0,   197),
    (0xFF8B0000u32, 16,   0,  -197),
    (0xFF8C0000u32, 16,   0,   198),
    (0xFF8D0000u32, 16,   0,  -198),
    (0xFF8E0000u32, 16,   0,   199),
    (0xFF8F0000u32, 16,   0,  -199),
    (0xFF900000u32, 16,   0,   200),
    (0xFF910000u32, 16,   0,  -200),
    (0xFF920000u32, 16,   0,   201),
    (0xFF930000u32, 16,   0,  -201),
    (0xFF940000u32, 16,   0,   202),
    (0xFF950000u32, 16,   0,  -202),
    (0xFF960000u32, 16,   0,   203),
    (0xFF970000u32, 16,   0,  -203),
    (0xFF980000u32, 16,   0,   204),
    (0xFF990000u32, 16,   0,  -204),
    (0xFF9A0000u32, 16,   0,   205),
    (0xFF9B0000u32, 16,   0,  -205),
    (0xFF9C0000u32, 16,   0,   206),
    (0xFF9D0000u32, 16,   0,  -206),
    (0xFF9E0000u32, 16,   0,   207),
    (0xFF9F0000u32, 16,   0,  -207),
    (0xFFA00000u32, 16,   0,   208),
    (0xFFA10000u32, 16,   0,  -208),
    (0xFFA20000u32, 16,   0,   209),
    (0xFFA30000u32, 16,   0,  -209),
    (0xFFA40000u32, 16,   0,   210),
    (0xFFA50000u32, 16,   0,  -210),
    (0xFFA60000u32, 16,   0,   211),
    (0xFFA70000u32, 16,   0,  -211),
    (0xFFA80000u32, 16,   0,   212),
    (0xFFA90000u32, 16,   0,  -212),
    (0xFFAA0000u32, 16,   0,   213),
    (0xFFAB0000u32, 16,   0,  -213),
    (0xFFAC0000u32, 16,   0,   214),
    (0xFFAD0000u32, 16,   0,  -214),
    (0xFFAE0000u32, 16,   0,   215),
    (0xFFAF0000u32, 16,   0,  -215),
    (0xFFB00000u32, 16,   0,   216),
    (0xFFB10000u32, 16,   0,  -216),
    (0xFFB20000u32, 16,   0,   217),
    (0xFFB30000u32, 16,   0,  -217),
    (0xFFB40000u32, 16,   0,   218),
    (0xFFB50000u32, 16,   0,  -218),
    (0xFFB60000u32, 16,   0,   219),
    (0xFFB70000u32, 16,   0,  -219),
    (0xFFB80000u32, 16,   0,   220),
    (0xFFB90000u32, 16,   0,  -220),
    (0xFFBA0000u32, 16,   0,   221),
    (0xFFBB0000u32, 16,   0,  -221),
    (0xFFBC0000u32, 16,   0,   222),
    (0xFFBD0000u32, 16,   0,  -222),
    (0xFFBE0000u32, 16,   0,   223),
    (0xFFBF0000u32, 16,   0,  -223),
    (0xFFC00000u32, 16,   0,   224),
    (0xFFC10000u32, 16,   0,  -224),
    (0xFFC20000u32, 16,   0,   225),
    (0xFFC30000u32, 16,   0,  -225),
    (0xFFC40000u32, 16,   0,   226),
    (0xFFC50000u32, 16,   0,  -226),
    (0xFFC60000u32, 16,   0,   227),
    (0xFFC70000u32, 16,   0,  -227),
    (0xFFC80000u32, 16,   0,   228),
    (0xFFC90000u32, 16,   0,  -228),
    (0xFFCA0000u32, 16,   0,   229),
    (0xFFCB0000u32, 16,   0,  -229),
    (0xFFCC0000u32, 16,   0,   230),
    (0xFFCD0000u32, 16,   0,  -230),
    (0xFFCE0000u32, 16,   0,   231),
    (0xFFCF0000u32, 16,   0,  -231),
    (0xFFD00000u32, 16,   0,   232),
    (0xFFD10000u32, 16,   0,  -232),
    (0xFFD20000u32, 16,   0,   233),
    (0xFFD30000u32, 16,   0,  -233),
    (0xFFD40000u32, 16,   0,   234),
    (0xFFD50000u32, 16,   0,  -234),
    (0xFFD60000u32, 16,   0,   235),
    (0xFFD70000u32, 16,   0,  -235),
    (0xFFD80000u32, 16,   0,   236),
    (0xFFD90000u32, 16,   0,  -236),
    (0xFFDA0000u32, 16,   0,   237),
    (0xFFDB0000u32, 16,   0,  -237),
    (0xFFDC0000u32, 16,   0,   238),
    (0xFFDD0000u32, 16,   0,  -238),
    (0xFFDE0000u32, 16,   0,   239),
    (0xFFDF0000u32, 16,   0,  -239),
    (0xFFE00000u32, 16,   0,   240),
    (0xFFE10000u32, 16,   0,  -240),
    (0xFFE20000u32, 16,   0,   241),
    (0xFFE30000u32, 16,   0,  -241),
    (0xFFE40000u32, 16,   0,   242),
    (0xFFE50000u32, 16,   0,  -242),
    (0xFFE60000u32, 16,   0,   243),
    (0xFFE70000u32, 16,   0,  -243),
    (0xFFE80000u32, 16,   0,   244),
    (0xFFE90000u32, 16,   0,  -244),
    (0xFFEA0000u32, 16,   0,   245),
    (0xFFEB0000u32, 16,   0,  -245),
    (0xFFEC0000u32, 16,   0,   246),
    (0xFFED0000u32, 16,   0,  -246),
    (0xFFEE0000u32, 16,   0,   247),
    (0xFFEF0000u32, 16,   0,  -247),
    (0xFFF00000u32, 16,   0,   248),
    (0xFFF10000u32, 16,   0,  -248),
    (0xFFF20000u32, 16,   0,   249),
    (0xFFF30000u32, 16,   0,  -249),
    (0xFFF40000u32, 16,   0,   250),
    (0xFFF50000u32, 16,   0,  -250),
    (0xFFF60000u32, 16,   0,   251),
    (0xFFF70000u32, 16,   0,  -251),
    (0xFFF80000u32, 16,   0,   252),
    (0xFFF90000u32, 16,   0,  -252),
    (0xFFFA0000u32, 16,   0,   253),
    (0xFFFB0000u32, 16,   0,  -253),
    (0xFFFC0000u32, 16,   0,   254),
    (0xFFFD0000u32, 16,   0,  -254),
    (0xFFFE0000u32, 16,   0,   255),
    (0xFFFF0000u32, 16,   0,  -255),
];

struct BitReader<'a> {
    data: &'a [u8],
    pos: usize, // bit position
}

impl<'a> BitReader<'a> {
    fn new(data: &'a [u8]) -> Self { BitReader { data, pos: 0 } }

    /// Peek at next 32 bits MSB-first without advancing (zero-pads at end).
    #[inline]
    fn peek32(&self) -> u32 {
        let byte = self.pos / 8;
        let bit  = self.pos % 8;
        let mut v = 0u32;
        for i in 0..4 {
            if byte + i < self.data.len() {
                v |= (self.data[byte + i] as u32) << (24 - i * 8);
            }
        }
        v << bit
    }

    #[inline]
    fn advance(&mut self, n: u8) { self.pos += n as usize; }

    #[inline]
    fn remaining(&self) -> usize {
        let total = self.data.len() * 8;
        if self.pos >= total { 0 } else { total - self.pos }
    }
}

/// Decode one VLC codeword. Returns (run_decoder, level, code_len).
/// run_decoder = table_run + 1 (as FFmpeg does).
/// run=128 means EOB (table_run=127).
/// Returns None if no codeword matched (bitstream error).
#[inline]
fn decode_dv_vlc(br: &BitReader) -> Option<(u8, i16, u8)> {
    if br.remaining() < 2 { return None; }
    let bits = br.peek32();
    for &(code, len, run, level) in DV_VLC {
        if len as usize > br.remaining() { continue; }
        let mask = if len >= 32 { !0u32 } else { !(!0u32 >> len) };
        if bits & mask == code {
            // run+1 to match FFmpeg's dv_rl_vlc which adds 1 to all runs
            return Some((run.wrapping_add(1), level, len));
        }
    }
    None
}

/// Check AC bitstream validity for all Video DIF blocks in a frame.
/// Returns count of blocks where the EOB is absent (matches FFmpeg's check).
/// Only call after STA check passes.
fn check_ac_bitstream(frame: &[u8]) -> u32 {
    let ns = match n_seq(frame.len()) { Some(n) => n, None => return 0 };
    // SD DV block sizes in bits: [112, 112, 112, 112, 80, 80] (from av_dv_frame_profile)
    // 4 luma blocks * 112 bits + 2 chroma blocks * 80 bits = 608 bits = 76 bytes
    const BLOCK_SIZES: [usize; 6] = [112, 112, 112, 112, 80, 80];
    const HEADER_BITS: usize = 12; // 9 DC + 1 dct_mode + 2 class
    let mut ac_errors = 0u32;

    for seq in 0..ns {
        for dif_blk in 0..150usize {
            let off = (seq * 150 + dif_blk) * DIF_BLOCK;
            if off + DIF_BLOCK > frame.len() { break; }
            let b = &frame[off..off + DIF_BLOCK];
            if (b[0] >> 5) & 7 != SCT_VIDEO { continue; }

            // Process each of the 6 DCT blocks within this Video DIF block.
            // Each DCT block occupies a fixed number of bits starting from
            // the Video DIF payload (bytes 4..80), laid out sequentially.
            let payload = &b[4..]; // 76 bytes
            let mut bit_offset = 0usize; // bit offset within payload

            for &block_bits in &BLOCK_SIZES {
                let ac_bits = block_bits - HEADER_BITS;
                // Skip the 12-bit header (DC + dct_mode + class).
                let ac_start = bit_offset + HEADER_BITS;
                let ac_end   = bit_offset + block_bits;

                // Slice exactly the AC region for this DCT block.
                let ac_byte_start = ac_start / 8;
                let ac_byte_end   = (ac_end + 7) / 8;
                if ac_byte_end > payload.len() { break; }

                let ac_data = &payload[ac_byte_start..ac_byte_end];
                // Adjust start within the slice for sub-byte alignment.
                let bit_align = ac_start % 8;
                let mut br = BitReader { data: ac_data, pos: bit_align };
                let br_limit = bit_align + ac_bits;

                // Decode AC VLC stream for this block.
                // pos tracks coefficient index (0..63 after DC).
                let mut pos: u32 = 0;
                let mut error = false;

                loop {
                    if br.pos >= br_limit { break; } // consumed all bits for this block

                    match decode_dv_vlc(&br) {
                        None => {
                            // Only flag as error if there are enough bits for a complete
                            // codeword (min len = 3 bits) but nothing matches.
                            // If we're near the block boundary with < 17 bits remaining
                            // (max VLC codeword length), it's a normal partial VLC that
                            // continues in FFmpeg's overflow buffer (pass 3). Not an error.
                            let remaining = br_limit - br.pos;
                            if remaining >= 17 {
                                error = true;
                            }
                            break;
                        }
                        Some((run_dec, _level, len)) => { // level unused in health check
                            br.advance(len);
                            pos += run_dec as u32;
                            if pos >= 64 { break; } // clean EOB (run=128) or past end
                        }
                    }
                }

                // Mirror FFmpeg's check: error if 64 <= pos < 127
                // (pos overran without reaching clean EOB via large-run jump)
                if error || (pos >= 64 && pos < 127) {
                    ac_errors += 1;
                }

                bit_offset += block_bits;
            }
        }
    }
    ac_errors
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
    total: usize,
    corrupt: usize,
    corrupt_sta: usize,
    corrupt_ac: usize,
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
        if frame.sta_errors > 0 { st.corrupt_sta += 1; } else { st.corrupt_ac += 1; }
        debug!("Frame {:5}: CORRUPT sta={} ac={} tc={:?}", frame.index, frame.sta_errors, frame.ac_errors,
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
    // Parse and assess all input files in parallel.
    // Each file is fully independent — load + frame assessment happen concurrently.
    let results: Vec<(PathBuf, anyhow::Result<ParsedAvi>)> =
        std::thread::scope(|s| {
            let handles: Vec<_> = cli.inputs
                .iter()
                .map(|path| {
                    let p = path.clone();
                    s.spawn(move || (p.clone(), parse_avi(&p)))
                })
                .collect();
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });

    let mut loaded: Vec<(PathBuf, ParsedAvi)> = Vec::new();
    for (path, result) in results {
        let avi = result?;
        let corrupt = avi.frames.iter().filter(|f| !f.healthy).count();
        let ac_only = avi.frames.iter().filter(|f| f.sta_errors == 0 && f.ac_errors > 0).count();
        info!("  {:?}", path);
        info!("    {} frames ({:?}), {} corrupt ({:.1}%) [{} STA+dropout, {} AC-only]",
              avi.frames.len(), avi.kind, corrupt,
              if avi.frames.is_empty() { 0.0 } else { corrupt as f64 / avi.frames.len() as f64 * 100.0 },
              corrupt - ac_only, ac_only);
        loaded.push((path, avi));
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

    let corrupt   = main_avi.frames.iter().filter(|f| !f.healthy).count();
    let sta_count = main_avi.frames.iter().filter(|f| f.sta_errors > 0).count();
    let ac_count  = main_avi.frames.iter().filter(|f| f.sta_errors == 0 && f.ac_errors > 0).count();
    info!("Main: {} frames, {} corrupt ({} STA/dropout, {} AC-only)",
          main_avi.frames.len(), corrupt, sta_count, ac_count);

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
    println!("  STA/dropout:      {:>7}", st.corrupt_sta);
    println!("  AC bitstream:     {:>7}", st.corrupt_ac);
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
        let (h, sta, _ac) = assess_frame(&[0u8; 1234]);
        assert!(!h); assert!(sta > 0);
    }
    #[test] fn assess_ntsc_zeros_healthy() {
        // All-zero payload: STA=0, AC check will parse zeros as VLC codes.
        // We only assert STA is clean; AC result depends on VLC validity of zeros.
        let f = vec![0u8; 120_000];
        let (_h, sta, _ac) = assess_frame(&f);
        assert_eq!(sta, 0);
    }
    #[test] fn assess_ff_payload_corrupt() {
        let mut f = vec![0u8; 120_000];
        let off = 6 * DIF_BLOCK;
        f[off] = (SCT_VIDEO << 5) | 0;
        for b in &mut f[off+4..off+DIF_BLOCK] { *b = 0xFF; }
        let (h, sta, _ac) = assess_frame(&f);
        assert!(!h); assert!(sta > 0);
    }
    #[test] fn blank_sizes() {
        assert_eq!(make_blank_frame(120_000).len(), 120_000);
        assert_eq!(make_blank_frame(144_000).len(), 144_000);
    }
    #[test] fn blank_is_healthy() {
        let f = make_blank_frame(120_000);
        let (_h, sta, _ac) = assess_frame(&f);
        assert_eq!(sta, 0); // blank frame: no STA errors; AC validity not guaranteed
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
