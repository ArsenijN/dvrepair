# dvrepair
![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)
![Language: Rust](https://img.shields.io/badge/language-Rust-orange.svg)

DV tape repair tool — conjoin multiple captures of the same tape into one healthy video.

## About

`dvrepair` solves a specific archival problem: when a MiniDV (or any DV format) tape is
captured multiple times, each capture will have slightly different read errors due to
natural tape wear, head alignment, and VCR mechanism tolerances. No single capture is
perfect — but across 2–6 captures of the same tape, every frame usually has at least
one healthy copy somewhere.

`dvrepair` detects corrupt frames using two layers of error detection, then replaces
them with healthy copies sourced from the spare captures. The result is a single video
that is as close to the original tape content as the available captures allow.

It works because DV is a **digital** format — corrupt frames are detectable by inspecting
error flags written by the VCR's own Reed-Solomon decoder, and by validating the DCT
coefficient bitstream. There is no guessing or quality-based interpolation: a frame is
either verifiably healthy or it isn't.

## Features

- **Two-layer corruption detection:**
  - **Level 1 — STA / dropout:** Reads Reed-Solomon error flags (STA nibble) directly
    from Video DIF block headers. STA ≠ 0 means the VCR's hardware declared the block
    unrecoverable. Also catches all-`0xFF` dropout patterns.
  - **Level 2 — AC bitstream:** Validates DCT coefficient VLC streams against the full
    DV Huffman table (IEC 61834-2 / SMPTE 314M, 409 entries, 746 with sign expansion).
    Catches frames where RS "recovered" the bytes to wrong values that pass STA checks
    but produce invalid coefficient streams — matching the same class of errors that
    FFmpeg reports as `AC EOB marker is absent`.
- **Type-1 and Type-2 AVI support** — handles both `iavs` interleaved streams and
  separate `00dc`/`00db` video streams. Includes a `--to-type2` converter.
- **NTSC and PAL** — 120,000-byte (525/60) and 144,000-byte (625/50) frames.
- **Frame matching by VAUX timecode** — uses the tape timecode embedded in DV VAUX
  packs to match frames across captures with different lengths or minor sync offsets.
  Falls back to frame index when timecodes are absent.
- **OpenDML / large AVI support** — handles files over 1 GB (multiple RIFF chunks).
- **Non-destructive** — always writes a new output file; inputs are never modified.
- **RIFF structure diagnostic** (`--dump-riff`) and **AC decode trace** (`--dump-frame`)
  for inspecting files and debugging.

## Installation

Requires Rust (edition 2021). Clone and build:

```sh
git clone https://github.com/ArsenijN/dvrepair
cd dvrepair
cargo build --release
```

Binary will be at `target/release/dvrepair` (or `dvrepair.exe` on Windows).

**Cargo.toml dependency note:** pin clap to `"=4.4.18"` if you are building with an
older Rust toolchain (e.g. Ubuntu's apt-packaged Rust 1.75). Newer toolchains can use
`"4"` freely.

## Usage

### Basic repair

```sh
dvrepair tape_a.avi tape_b.avi tape_c.avi -o repaired.avi
```

All input files must be roughly the same tape capture. If they all have equal frame
counts, the most-corrupt file is automatically chosen as the main stream and the others
become spares. The repaired output is written to `-o` (default: `repaired.avi`).

### Specifying the main stream

When input files have **different frame counts**, you must tell dvrepair which is the
primary reference:

```sh
dvrepair tape_a.avi tape_b.avi tape_c.avi --main-stream tape_a.avi -o repaired.avi
```

### Fallback policy

When a corrupt frame has no healthy replacement in any spare file:

```sh
--fallback keep    # leave the corrupt frame as-is [default — safest]
--fallback freeze  # duplicate the previous healthy frame
--fallback blank   # substitute a synthetic black DV frame
```

### Frame matching

```sh
--match-mode timecode-then-index  # VAUX timecode first, frame index fallback [default]
--match-mode index-only           # frame index only (fastest, assumes aligned captures)
--match-mode timecode-only        # VAUX timecode only
```

### Diagnostics

```sh
# Show the RIFF chunk structure of a file (useful when dvrepair can't parse it):
dvrepair --dump-riff tape_a.avi

# Trace the AC bitstream decoder on a specific frame (shows every VLC decode step):
dvrepair --dump-frame 0 tape_a.avi

# Convert a Type-1 DV AVI to Type-2 (required for some capture software output):
dvrepair --to-type2 type1_capture.avi -o type2_output.avi
```

### Verbosity

```sh
-v   # debug (shows each repaired frame)
-vv  # trace (maximum detail)
```

## Understanding the output

```
5090 frames (Type2), 17 corrupt (0.3%) [9 STA+dropout, 8 AC-only]
```

- **STA+dropout** — frames detected by Reed-Solomon error flags or `0xFF` dropout
- **AC-only** — frames that passed RS checks but have invalid DCT coefficient streams
  (the class FFmpeg reports as `AC EOB marker is absent`)

```
=== dvrepair summary ===
Total frames:          5090
Corrupt frames:          17
  STA/dropout:            9
  AC bitstream:           8
Repaired:                17
  via timecode:           0
  via index:             17
Fallback applied:         0
Repair rate:        100.0%
```

A **100% repair rate** means every corrupt frame found a healthy replacement in the
spare captures. A repair rate below 100% means some frames were damaged in all available
captures simultaneously — this is the hard ceiling for multi-capture repair, and those
frames are left as-is (or frozen/blanked per `--fallback`).

## About the DV format

DV stores video as a sequence of fixed-size frames (120,000 bytes NTSC / 144,000 bytes
PAL). Each frame is divided into **DIF sequences** (10 for NTSC, 12 for PAL), each
containing 150 **DIF blocks** of 80 bytes. There are five block types (Header, Subcode,
VAUX, Audio, Video), identified by a 3-bit SCT field in each block's header.

**Error detection layers in DV:**

| Layer | What it checks | How we use it |
|---|---|---|
| RS inner code | Byte-level correction across DIF blocks | Done by VCR hardware; invisible after capture |
| RS outer code | Block-level detection; sets STA flag | We read STA nibble from byte 3 of Video DIF blocks |
| AC bitstream | DCT VLC stream validity | We decode against the full IEC 61834-2 VLC table |

**VAUX timecodes** are stored in DIF blocks with SCT=2 at positions 3–5 in each
sequence. Pack type `0x13` / `0x63` carries the tape timecode in BCD
(`hh:mm:ss;ff` format). dvrepair uses these as the primary key for matching frames
across captures.

**Type-1 vs Type-2 AVI:** Type-2 (most common on Windows; WinDV, etc.) stores video
in `00dc`/`00db` chunks and duplicates audio in `01wb` chunks. Type-1 (Linux dvgrab,
some cameras) stores the full interleaved DV bitstream in a single `iavs` stream.
The raw DV frame bytes are identical in both — only the container differs.

## Known limitations

- **Audio is not repaired.** DV audio lacks frame-level checksums accessible after
  capture, so dvrepair accepts audio as-is. Corrupt audio in unrepaired frames is left
  untouched.
- **Frames corrupt in all captures** cannot be repaired. If all spare copies of a frame
  are also damaged, there is no healthy source to replace it with. These are typically
  physical tape defects that every playback reads identically.
- **AC errors that pass the boundary check.** Some RS-recovered-but-wrong frames
  produce AC bitstreams where the EOB overrun is so minor (e.g. `pos=64` vs clean `128`)
  that it occurs within the block's bit allocation rather than at the boundary. These
  match FFmpeg's `AC EOB marker is absent` output. If all captures share this damage,
  they remain in the output — FFmpeg's concealment handles them gracefully on playback.
- **No in-place editing.** dvrepair always writes a new file. The output AVI is
  typically the same size as the input (DV frames are fixed-size, patched in place).
- **NTSC ↔ PAL mixing is rejected.** All input files must be the same frame size.
- **RAM usage.** All input files are loaded into memory simultaneously. For a typical
  ~3 minute PAL capture at ~750 MB per file, processing 6 files uses ~4.5 GB RAM.
  This is a known limitation with a TODO for streaming/mmap support.

## Roadmap / TODO

- [ ] Streaming / memory-mapped I/O to remove the full-load-into-RAM requirement
- [ ] Anchor-frame drift matching for length-mismatched captures (hash previous 2
      healthy frames to locate the correct offset when captures are not frame-aligned)
- [ ] VAUX timecode presence statistics in summary output
- [ ] Progress bar for large files
- [ ] Output statistics: which spare file contributed the most repairs
- [ ] Per-frame repair log / CSV export for archival documentation
- [ ] `.dv` raw stream output option (in addition to AVI)
- [ ] Majority-vote byte-level reconstruction for frames damaged in most (but not all)
      captures — combine bytes from multiple partially-healthy copies
- [ ] Add edgecase for the different length inputs (perform previous frame(s) check 
      over index - since latter one makes stutters)

---

*I can make 1:1 non-corrupted copies of home videos now :>*

**Shoutout to the FFmpeg team for the open-source software, and for the DV VLC table (from `/libavcodec/dvdata.c`)**