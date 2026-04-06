# MSISDN Binary Search Hash Lookup

Reverse-lookup SHA256 hashes of Kenyan phone numbers from [MPesa Daraja API](https://developer.safaricom.co.ke/) callbacks.

**No external dependencies for core usage — pure Python stdlib.**

---

## The problem

Safaricom's MPesa Daraja API used to include the subscriber's phone number in plain text (partially masked) in payment callbacks — for example, in C2B Pay Bill callbacks:

```json
{
  "TransactionType": "Pay Bill",
  "TransID": "RKL51ZDR4F",
  "TransTime": "20231121121325",
  "TransAmount": "5.00",
  "BusinessShortCode": "600966",
  "BillRefNumber": "Sample Transaction",
  "InvoiceNumber": "",
  "OrgAccountBalance": "25.00",
  "ThirdPartyTransID": "",
  "MSISDN": "fc418dcfe94c732a...",  // ← SHA256 hash of the phone number (used to be plaintext e.g. "2547 ***** 126")
  "FirstName": "NICHOLAS",
  "MiddleName": "",
  "LastName": ""
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

**Binary search:**

Given a sorted file of *N* records, a lookup requires at most ⌈log₂(N)⌉ comparisons:

```
comparisons = ⌈log₂(N)⌉

N = 200,000,000
log₂(200,000,000) ≈ 27.575
⌈27.575⌉ = 28
```

At each step, the search range halves:

```
mid = (low + high) // 2

if hash[mid] == target → found
if hash[mid]  < target → low  = mid + 1
if hash[mid]  > target → high = mid - 1
```

28 comparisons against a 7.2 GB file. Sub-millisecond.

The build process parallelises across all CPU cores: each worker hashes and sorts a chunk of numbers independently, then the chunks are merged in a single streaming pass — keeping peak memory usage low regardless of chunk count.

---

## Using from AWS Lambda (S3 Range GET)

You don't need to download the full 7.2 GB file inside a Lambda function. Because SHA256 hashes are **uniformly distributed**, the first 8 bytes of any target hash directly estimate its position in the sorted file. A single S3 Range GET of ~2 MB centred on that position is statistically guaranteed to contain the answer — resolving any number in one request, with no cold-start overhead.

**Setup:**

1. Upload `hashes.bin` to S3:
```bash
python msisdn_lookup.py upload my-s3-bucket
# Prints LOOKUP_RECORD_COUNT at the end — save this value
```

2. Set environment variables on your Lambda:
```
LOOKUP_BUCKET=my-s3-bucket
LOOKUP_KEY=hashes.bin
LOOKUP_RECORD_COUNT=<value printed by upload command>
```

3. Grant the Lambda's execution role `s3:GetObject` on the bucket.

**How the lookup works:**

```
target hash (hex) → first 8 bytes as uint64
                  → scale to [0, RECORD_COUNT) via interpolation
                  → fetch 2 MB block centred on estimated position (one Range GET)
                  → binary search within the block
                  → decode global index → phone number string
```

The 2 MB block covers ±55,000 records around the interpolated position — more than enough margin for the uniform distribution of SHA256 hashes. Every lookup costs exactly one S3 `GetObject` call (~20–30 ms).

**Cost**

An S3 Range GET costs $0.0004 per 1,000 requests. At 100,000 MPesa callbacks a month — a busy merchant — that's **$0.04**. Storage for `hashes.bin` is around **$0.17/month**. The total running cost is effectively zero.

**Integrating with other functions**

Any Lambda that receives an MPesa hash can do the same lookup — confirmation handlers, reconciliation jobs, analytics pipelines, CRM/Sheets sync functions. The file lives in S3 once and all functions share it. Each function just needs the three environment variables and `s3:GetObject` on the bucket; the lookup logic is self-contained and adds ~25 ms to any invocation that needs a phone number resolved.

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
