"""
AWS Lambda handler. Reverses an MSISDN SHA-256 hash by fetching a small byte
range of hashes.bin from S3 and binary-searching within it.

Required environment variables:
  LOOKUP_BUCKET        S3 bucket holding hashes.bin
  LOOKUP_KEY           S3 object key (e.g. "hashes.bin")
  LOOKUP_RECORD_COUNT  Number of records in the file (printed by `upload`)

The IAM role must have s3:GetObject on the bucket/key.

Invoke with `{"hash": "<64-char hex>"}`. Returns the MSISDN as `254XXXXXXXXX`.
"""
import hashlib
import json
import os
import struct

import boto3

PREFIXES = ("2547", "2541")
NUMBERS_PER_PREFIX = 100_000_000
HASH_SIZE = 32
STORED_HASH_SIZE = 10
SUFFIX_SIZE = 4
RECORD_SIZE = STORED_HASH_SIZE + SUFFIX_SIZE  # 14
TWO_64 = 1 << 64
WINDOW = 1 * 1024 * 1024  # 1 MB each side, 2 MB total per request

BUCKET = os.environ["LOOKUP_BUCKET"]
KEY = os.environ["LOOKUP_KEY"]
N = int(os.environ["LOOKUP_RECORD_COUNT"])
FILE_SIZE = N * RECORD_SIZE

_s3 = boto3.client("s3")


def _decode(idx: int) -> str:
    prefix = PREFIXES[idx // NUMBERS_PER_PREFIX]
    suffix = idx % NUMBERS_PER_PREFIX
    return f"{prefix}{suffix:08d}"


def _range_for(target: bytes) -> tuple[int, int]:
    # SHA-256 output is uniform, so the first 8 bytes interpolate-search to
    # the approximate sorted-file position. Fetch a window around it.
    prefix_u64 = int.from_bytes(target[:8], "big")
    est_record = (prefix_u64 * N) // TWO_64
    centre = est_record * RECORD_SIZE
    start = max(0, centre - WINDOW)
    end = min(FILE_SIZE, start + 2 * WINDOW) - 1
    return start, end


def _search(body: bytes, target: bytes) -> str | None:
    target_trunc = target[:STORED_HASH_SIZE]
    n = len(body) // RECORD_SIZE
    lo, hi = 0, n - 1
    while lo <= hi:
        mid = (lo + hi) >> 1
        o = mid * RECORD_SIZE
        rec_trunc = body[o : o + STORED_HASH_SIZE]
        if rec_trunc == target_trunc:
            left = mid
            while left > 0:
                lo2 = (left - 1) * RECORD_SIZE
                if body[lo2 : lo2 + STORED_HASH_SIZE] != target_trunc:
                    break
                left -= 1
            i = left
            while i < n:
                o2 = i * RECORD_SIZE
                if body[o2 : o2 + STORED_HASH_SIZE] != target_trunc:
                    break
                idx = struct.unpack(">I", body[o2 + STORED_HASH_SIZE : o2 + RECORD_SIZE])[0]
                cand = _decode(idx)
                if hashlib.sha256(cand.encode()).digest() == target:
                    return cand
                i += 1
            return None
        elif rec_trunc < target_trunc:
            lo = mid + 1
        else:
            hi = mid - 1
    return None


def lookup(hash_hex: str) -> str | None:
    target = bytes.fromhex(hash_hex)
    if len(target) != HASH_SIZE:
        raise ValueError(f"expected {HASH_SIZE * 2} hex chars, got {len(hash_hex)}")
    start, end = _range_for(target)
    body = _s3.get_object(
        Bucket=BUCKET, Key=KEY, Range=f"bytes={start}-{end}"
    )["Body"].read()
    return _search(body, target)


def handler(event, context):
    raw = event.get("hash") if isinstance(event, dict) else None
    if not raw:
        return {"statusCode": 400, "body": json.dumps({"error": "missing 'hash'"})}
    hash_hex = raw.strip().lower()
    if len(hash_hex) != 64:
        return {"statusCode": 400, "body": json.dumps({"error": "expected 64 hex chars"})}
    try:
        phone = lookup(hash_hex)
    except ValueError as e:
        return {"statusCode": 400, "body": json.dumps({"error": str(e)})}
    if phone is None:
        return {"statusCode": 404, "body": json.dumps({"error": "not found"})}
    return {"statusCode": 200, "body": json.dumps({"phone": phone})}
