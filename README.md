# MSISDN Binary Search Hash Lookup

Reverse-lookup SHA256 hashes of Kenyan phone numbers from [MPesa Daraja API](https://developer.safaricom.co.ke/) callbacks.

The Daraja API returns the subscriber's phone number as a SHA256 hash. This tool pre-computes a lookup table covering all 100 million valid `2547XXXXXXXX` numbers so any hash can be reversed instantly.

**No external dependencies — pure Python stdlib.**

---

## Quick start

### 1. Clone

```bash
git clone https://github.com/endafk/msisdn-lookup.git
cd msisdn-lookup
```

### 2. Build the database (one-time, ~3.6 GB)

```bash
python msisdn_lookup.py build
```

This generates `hashes.bin` in the project directory. It takes roughly **30–60 seconds** on a modern laptop (parallelised across all CPU cores). You only ever need to do this once.

> **Requires ~4 GB of free disk space.**

### 3. Start the web UI

```bash
python server.py
```

Then open **http://localhost:8765** in your browser. Paste a hash, get the phone number back in `07XXXXXXXX` format with one click to copy.

---

## CLI usage

```bash
# Reverse a hash directly
python msisdn_lookup.py lookup 172509f6416f41d1ce3b78a757c1d4ce90fc1ab1c9d4cdf1edf25ab7bf3fbdfd
# → 254700000001

# Show database stats
python msisdn_lookup.py info
```

### Verify it's working

These two hashes are included as test vectors:

| Hash | Phone number |
|------|-------------|
| `172509f6416f41d1ce3b78a757c1d4ce90fc1ab1c9d4cdf1edf25ab7bf3fbdfd` | `254700000001` |
| `339aae32bde3244b98ec81d44f6991c200fd3cfbc3e5bc7f453b862b59e397ac` | `254700000002` |

---

## How it works

The hashing algorithm is plain SHA256 of the phone number string:

```python
import hashlib
hashlib.sha256(b"254700000001").hexdigest()
# → 172509f6416f41d1ce3b78a757c1d4ce90fc1ab1c9d4cdf1edf25ab7bf3fbdfd
```

The database is a flat binary file of 100 million 36-byte records `(hash[32], suffix[4])` sorted by hash. Lookups use a memory-mapped binary search (~27 comparisons) and return in under a millisecond once the OS page cache is warm.

---

## Options

```
python msisdn_lookup.py build --workers 4    # limit CPU usage
python msisdn_lookup.py build --force        # rebuild existing database
python msisdn_lookup.py --db /path/to/hashes.bin lookup <hash>

python server.py --port 9000                 # change port
python server.py --db /path/to/hashes.bin    # custom database path
```
