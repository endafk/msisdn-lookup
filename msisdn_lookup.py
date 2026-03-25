#!/usr/bin/env python3
import argparse
import hashlib
import heapq
import mmap
import multiprocessing
import os
import shutil
import struct
import sys
import tempfile
import time
from pathlib import Path

# Two Kenyan mobile prefixes (Safaricom owns both):
#   global index   0 –  99,999,999  →  2547XXXXXXXX
#   global index 100,000,000 – 199,999,999  →  2541XXXXXXXX
PREFIXES = [b"2547", b"2541"]
SUFFIX_DIGITS = 8
NUMBERS_PER_PREFIX = 10 ** SUFFIX_DIGITS          # 100,000,000
TOTAL_NUMBERS = NUMBERS_PER_PREFIX * len(PREFIXES) # 200,000,000

HASH_SIZE = 32
SUFFIX_SIZE = 4
RECORD_SIZE = HASH_SIZE + SUFFIX_SIZE

DEFAULT_DB = Path(__file__).parent / "hashes.bin"


def _hash_number(global_index: int) -> bytes:
    prefix = PREFIXES[global_index // NUMBERS_PER_PREFIX]
    suffix = global_index % NUMBERS_PER_PREFIX
    phone = prefix + f"{suffix:0{SUFFIX_DIGITS}d}".encode()
    return hashlib.sha256(phone).digest()


def _decode_global_index(v: int) -> str:
    prefix = PREFIXES[v // NUMBERS_PER_PREFIX].decode()
    suffix = v % NUMBERS_PER_PREFIX
    return f"{prefix}{suffix:0{SUFFIX_DIGITS}d}"


def _build_chunk(args: tuple) -> str:
    start, end, tmp_dir = args

    records = []
    for global_idx in range(start, end):
        h = _hash_number(global_idx)
        records.append(h + struct.pack(">I", global_idx))

    records.sort()

    fd, path = tempfile.mkstemp(dir=tmp_dir, suffix=".chunk")
    with os.fdopen(fd, "wb") as f:
        f.write(b"".join(records))

    return path


def _merge_chunks(chunk_files: list[str], output_path: Path) -> None:
    handles = [open(p, "rb") for p in chunk_files]

    def _next(fh, idx):
        data = fh.read(RECORD_SIZE)
        return (data, idx) if data else None

    heap = []
    for i, fh in enumerate(handles):
        entry = _next(fh, i)
        if entry:
            heapq.heappush(heap, entry)

    try:
        written = 0
        report_every = 5_000_000
        buf_size = 64 * 1024 * 1024
        with open(output_path, "wb", buffering=buf_size) as out:
            while heap:
                rec, i = heapq.heappop(heap)
                out.write(rec)
                written += 1
                if written % report_every == 0:
                    pct = written / TOTAL_NUMBERS * 100
                    print(
                        f"\r  {written // 1_000_000}M / {TOTAL_NUMBERS // 1_000_000}M"
                        f"  ({pct:.0f}%)   ",
                        end="",
                        flush=True,
                    )
                entry = _next(handles[i], i)
                if entry:
                    heapq.heappush(heap, entry)
        print()
    finally:
        for fh in handles:
            try:
                fh.close()
            except Exception:
                pass
        for p in chunk_files:
            try:
                os.unlink(p)
            except Exception:
                pass


def cmd_build(args) -> None:
    db_path = Path(args.db)
    chunk_size = args.chunk_size
    workers = args.workers or multiprocessing.cpu_count()

    n_chunks = (TOTAL_NUMBERS + chunk_size - 1) // chunk_size
    est_bytes = TOTAL_NUMBERS * RECORD_SIZE

    print("Building MSISDN lookup database")
    print(f"  Output:    {db_path}")
    print(f"  Numbers:   {TOTAL_NUMBERS:,}  (2547XXXXXXXX + 2541XXXXXXXX, 100M each)")
    print(f"  Est. size: {est_bytes / 1e9:.2f} GB")
    print(f"  Chunks:    {n_chunks} × {chunk_size:,} records")
    print(f"  Workers:   {workers}")

    if db_path.exists() and not args.force:
        print(f"\nDatabase already exists at {db_path}")
        print("Use --force to rebuild.")
        return

    target_dir = db_path.parent
    target_dir.mkdir(parents=True, exist_ok=True)
    free = shutil.disk_usage(target_dir).free
    needed = int(est_bytes * 1.15)
    if free < needed:
        sys.exit(
            f"Not enough disk space: need ~{needed/1e9:.1f} GB, "
            f"only {free/1e9:.1f} GB free on {target_dir}"
        )

    t0 = time.monotonic()
    tmp_output = db_path.with_suffix(".bin.tmp")

    if tmp_output.exists():
        tmp_output.unlink()

    chunk_args = [
        (start, min(start + chunk_size, TOTAL_NUMBERS), None)
        for start in range(0, TOTAL_NUMBERS, chunk_size)
    ]

    try:
        with tempfile.TemporaryDirectory(dir=target_dir) as tmp_dir:
            chunk_args = [(s, e, tmp_dir) for s, e, _ in chunk_args]

            print(f"\n[1/2] Generating & sorting {n_chunks} chunks", flush=True)
            chunk_files = []
            done = 0
            with multiprocessing.Pool(workers) as pool:
                for chunk_file in pool.imap_unordered(_build_chunk, chunk_args):
                    chunk_files.append(chunk_file)
                    done += 1
                    elapsed = time.monotonic() - t0
                    rate = done / elapsed if elapsed else 0
                    eta = (n_chunks - done) / rate if rate else 0
                    print(
                        f"\r  {done}/{n_chunks} ({done/n_chunks*100:.0f}%)"
                        f"  elapsed {elapsed:.0f}s  ETA {eta:.0f}s   ",
                        end="",
                        flush=True,
                    )
            print()

            print(f"\n[2/2] Merging {len(chunk_files)} chunks → {db_path}")
            _merge_chunks(chunk_files, tmp_output)

        tmp_output.replace(db_path)

    except KeyboardInterrupt:
        print("\n\nInterrupted — cleaning up...")
        if tmp_output.exists():
            tmp_output.unlink()
        sys.exit(1)
    except Exception:
        if tmp_output.exists():
            tmp_output.unlink()
        raise

    elapsed = time.monotonic() - t0
    size_gb = db_path.stat().st_size / 1e9
    print(f"\nDone in {elapsed:.1f}s  |  DB size: {size_gb:.2f} GB")
    print(f"Saved to: {db_path}")


def _binary_search(mm: mmap.mmap, target: bytes, n_records: int) -> int | None:
    lo, hi = 0, n_records - 1
    while lo <= hi:
        mid = (lo + hi) >> 1
        offset = mid * RECORD_SIZE
        record_hash = bytes(mm[offset : offset + HASH_SIZE])
        if record_hash == target:
            return struct.unpack(">I", mm[offset + HASH_SIZE : offset + RECORD_SIZE])[0]
        elif record_hash < target:
            lo = mid + 1
        else:
            hi = mid - 1
    return None


def cmd_lookup(args) -> None:
    db_path = Path(args.db)
    hash_hex = args.hash.strip().lower()

    if len(hash_hex) != 64:
        sys.exit(f"Error: expected 64 hex chars, got {len(hash_hex)}")
    try:
        target = bytes.fromhex(hash_hex)
    except ValueError:
        sys.exit("Error: invalid hex string")

    if not db_path.exists():
        sys.exit(
            f"Database not found: {db_path}\n"
            f"Run:  python msisdn_lookup.py build"
        )

    db_size = db_path.stat().st_size
    if db_size % RECORD_SIZE != 0:
        sys.exit(
            f"Error: database size {db_size} bytes is not a multiple of {RECORD_SIZE}. "
            f"The file may be corrupt — rebuild with: python msisdn_lookup.py build --force"
        )
    n_records = db_size // RECORD_SIZE

    with open(db_path, "rb") as f:
        with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
            global_index = _binary_search(mm, target, n_records)

    if global_index is None:
        print("Not found")
        sys.exit(1)

    print(_decode_global_index(global_index))


def cmd_info(args) -> None:
    db_path = Path(args.db)
    if not db_path.exists():
        print(f"Database not found: {db_path}")
        print(f"Build it with:  python msisdn_lookup.py build")
        return

    size = db_path.stat().st_size
    if size % RECORD_SIZE != 0:
        print(f"Warning: file size {size} is not a multiple of {RECORD_SIZE} — may be corrupt")
    n_records = size // RECORD_SIZE
    print(f"Database:  {db_path}")
    print(f"Size:      {size / 1e9:.3f} GB  ({size:,} bytes)")
    print(f"Records:   {n_records:,} of {TOTAL_NUMBERS:,} ({n_records/TOTAL_NUMBERS*100:.1f}%)")
    print(f"Coverage:  2547XXXXXXXX (0–99,999,999) + 2541XXXXXXXX (100,000,000–199,999,999)")


def cmd_upload(args) -> None:
    try:
        import boto3
        from boto3.s3.transfer import TransferConfig
    except ImportError:
        sys.exit("boto3 is required for upload: pip install boto3")

    db_path = Path(args.db)
    if not db_path.exists():
        sys.exit(f"Database not found: {db_path}\nBuild it first: python msisdn_lookup.py build")

    size = db_path.stat().st_size
    n_records = size // RECORD_SIZE
    key = args.key or db_path.name

    session = boto3.Session(profile_name=args.profile) if args.profile else boto3.Session()
    s3 = session.client("s3", region_name=args.region)

    print(f"Uploading {db_path}")
    print(f"  → s3://{args.bucket}/{key}")
    print(f"  Size:    {size / 1e9:.2f} GB")
    print(f"  Records: {n_records:,}")

    t0 = time.monotonic()
    uploaded = [0]

    def _progress(n: int) -> None:
        uploaded[0] += n
        pct = uploaded[0] / size * 100
        print(
            f"\r  {uploaded[0]/1e9:.2f} GB / {size/1e9:.2f} GB  ({pct:.0f}%)   ",
            end="",
            flush=True,
        )

    config = TransferConfig(
        multipart_threshold=128 * 1024 * 1024,
        multipart_chunksize=128 * 1024 * 1024,
        max_concurrency=8,
    )
    s3.upload_file(str(db_path), args.bucket, key, Callback=_progress, Config=config)

    elapsed = time.monotonic() - t0
    print(f"\nDone in {elapsed:.1f}s")
    print(f"S3 URI: s3://{args.bucket}/{key}")
    print(f"Record count (set as LOOKUP_RECORD_COUNT env var): {n_records}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Reverse SHA256 hashes of Kenyan MPesa phone numbers (2547/2541 XXXXXXXX)",
    )
    parser.add_argument(
        "--db",
        default=str(DEFAULT_DB),
        metavar="PATH",
        help=f"Path to database file (default: {DEFAULT_DB})",
    )

    sub = parser.add_subparsers(dest="command", required=True)

    p_build = sub.add_parser("build", help="Build the lookup database (one-time, ~7.2 GB)")
    p_build.add_argument(
        "--chunk-size",
        type=int,
        default=1_000_000,
        metavar="N",
        help="Records per chunk / worker task (default: 1,000,000)",
    )
    p_build.add_argument(
        "--workers",
        type=int,
        default=None,
        metavar="N",
        help="Parallel worker count (default: CPU count)",
    )
    p_build.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing database",
    )

    p_lookup = sub.add_parser("lookup", help="Reverse-lookup a SHA256 hash")
    p_lookup.add_argument("hash", help="64-char hex SHA256 hash")

    sub.add_parser("info", help="Show database stats")

    p_upload = sub.add_parser("upload", help="Upload hashes.bin to S3 (requires boto3)")
    p_upload.add_argument("bucket", help="S3 bucket name")
    p_upload.add_argument("--key", default=None, metavar="KEY",
                          help="S3 object key (default: hashes.bin)")
    p_upload.add_argument("--region", default="us-east-1", metavar="REGION",
                          help="AWS region (default: us-east-1)")
    p_upload.add_argument("--profile", default=None, metavar="PROFILE",
                          help="AWS credentials profile (e.g. arch-cli-user)")

    args = parser.parse_args()

    if args.command == "build":
        cmd_build(args)
    elif args.command == "lookup":
        cmd_lookup(args)
    elif args.command == "info":
        cmd_info(args)
    elif args.command == "upload":
        cmd_upload(args)


if __name__ == "__main__":
    main()
