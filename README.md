# MSISDN Binary Search Hash Lookup

Reverse-lookup SHA256 hashes of Kenyan phone numbers from [MPesa Daraja API](https://developer.safaricom.co.ke/) callbacks.

The Daraja API returns the subscriber's phone number as a SHA256 hash. This tool pre-computes a lookup table covering all 200 million numbers across both Safaricom prefixes (`2547XXXXXXXX` and `2541XXXXXXXX`) so any hash can be reversed instantly.

**No external dependencies for core usage — pure Python stdlib.**

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

This generates `hashes.bin` in the project directory. It takes roughly **60–120 seconds** on a modern laptop (parallelised across all CPU cores). You only ever need to do this once.

> **Requires ~8 GB of free disk space.**

### 3. Start the web UI

```bash
python server.py
```

Then open **http://localhost:8765** in your browser. Paste a hash, get the phone number back in `7XXXXXXXX` / `1XXXXXXXX` format with one click to copy.

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

The database is a flat binary file of 200 million 36-byte records `(hash[32], global_index[4])` sorted by hash. Lookups use a memory-mapped binary search (~28 comparisons) and return in under a millisecond once the OS page cache is warm.

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
python msisdn_lookup.py info                   # show database stats
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
