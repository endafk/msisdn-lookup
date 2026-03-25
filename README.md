# MSISDN Binary Search Hash Lookup

Reverse-lookup SHA256 hashes of Kenyan phone numbers from [MPesa Daraja API](https://developer.safaricom.co.ke/) callbacks.

**No external dependencies for core usage — pure Python stdlib.**

---

## The problem

When MPesa Daraja API sends a payment callback, the subscriber's phone number is not returned in plain text — it arrives as a SHA256 hash:

```json
{
  "Body": {
    "stkCallback": {
      "CallbackMetadata": {
        "Item": [
          { "Name": "PhoneNumber", "Value": "fc418dcfe94c732a..." }
        ]
      }
    }
  }
}
```

SHA256 is a one-way function — you cannot mathematically reverse it. The only way to recover the original phone number is to hash every possible candidate and check for a match.

For Kenyan Safaricom numbers this means checking up to 200 million candidates (`2547XXXXXXXX` and `2541XXXXXXXX`). Doing that on every callback request is not viable.

---

## Why not other approaches

| Approach | Lookup time | Notes |
|---|---|---|
| Brute-force per request | ~2 min | Hashes 200M numbers on every lookup. Completely unusable in practice. |
| Store hashes in a database (SQLite, Postgres) | ~5–50 ms | Requires a DB server or large SQLite file (~15 GB with indexes), plus query overhead. |
| Rainbow tables | Saves some space | Complex to implement correctly, slower than direct lookup, and the search space here is small enough not to need them. |
| **Sorted binary file + binary search** | **< 1 ms** | Pre-computed once. No server, no dependencies. Memory-mapped reads mean the OS page cache warms up after the first few queries. |

The key insight is that the phone number space is **closed and enumerable** — there are exactly 200 million valid numbers. That makes it practical to pre-hash all of them once, sort the results, and store them in a flat file. Every subsequent lookup is just a binary search: ~28 comparisons against a 7.2 GB file, completing in under a millisecond.

---

## Quick start

### 1. Clone

```bash
git clone https://github.com/endafk/msisdn-lookup.git
cd msisdn-lookup
```

### 2. Build the database (one-time, ~7.2 GB)

```bash
python msisdn_lookup.py build
```

Generates `hashes.bin` in the project directory. Takes roughly **60–120 seconds** on a modern laptop (parallelised across all CPU cores). Only needs to be done once.

> **Requires ~8 GB of free disk space.**

### 3. Start the web UI

```bash
python server.py
```

Open **http://localhost:8765**. Paste a hash, get the phone number back in `7XXXXXXXX` / `1XXXXXXXX` format with one click to copy.

---

## CLI usage

```bash
# Reverse a hash directly
python msisdn_lookup.py lookup 172509f6416f41d1ce3b78a757c1d4ce90fc1ab1c9d4cdf1edf25ab7bf3fbdfd
# → 254700000001

# Show database stats
python msisdn_lookup.py info

# Upload hashes.bin to S3 (requires: pip install boto3)
python msisdn_lookup.py upload my-s3-bucket
```

### Verify it's working

| Hash | Phone number |
|------|-------------|
| `172509f6416f41d1ce3b78a757c1d4ce90fc1ab1c9d4cdf1edf25ab7bf3fbdfd` | `254700000001` |
| `339aae32bde3244b98ec81d44f6991c200fd3cfbc3e5bc7f453b862b59e397ac` | `254700000002` |
| `e21e3b5a41124bbf690368843745bb69fa13da41b156d2b0302553ad52273a67` | `254100000001` |
| `9ce288e90f1e81d98009a5b5020a82d60f5f06e1686540743511d10d5677adba` | `254100000002` |

---

## How it works

The hashing algorithm is plain SHA256 of the phone number string:

```python
import hashlib
hashlib.sha256(b"254700000001").hexdigest()
# → 172509f6416f41d1ce3b78a757c1d4ce90fc1ab1c9d4cdf1edf25ab7bf3fbdfd
```

The database is a flat binary file of 200 million 36-byte records `(hash[32], global_index[4])` sorted by hash. At lookup time the file is memory-mapped and binary-searched in ~28 comparisons. Once the OS page cache warms up, repeated lookups are effectively instant.

The build process parallelises across all CPU cores: each worker hashes and sorts a chunk of numbers independently, then the chunks are merged in a single streaming pass — keeping peak memory usage low regardless of chunk count.

---

## Options

### `build`
```
python msisdn_lookup.py build
python msisdn_lookup.py build --workers 4      # limit CPU usage
python msisdn_lookup.py build --chunk-size 2000000
python msisdn_lookup.py build --force          # rebuild existing database
```

### `lookup`
```
python msisdn_lookup.py lookup <hash>
python msisdn_lookup.py --db /path/to/hashes.bin lookup <hash>
```

### `info`
```
python msisdn_lookup.py info
```

### `upload` (requires `pip install boto3`)
```
python msisdn_lookup.py upload <bucket>
python msisdn_lookup.py upload <bucket> --key hashes.bin
python msisdn_lookup.py upload <bucket> --region eu-west-1
python msisdn_lookup.py upload <bucket> --profile my-aws-profile
```

### `server`
```
python server.py
python server.py --port 9000
python server.py --db /path/to/hashes.bin
```
