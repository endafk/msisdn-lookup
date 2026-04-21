#!/usr/bin/env python3
import hashlib
import mmap
import struct
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from msisdn_lookup import (
    HASH_SIZE,
    NUMBERS_PER_PREFIX,
    PREFIXES,
    RECORD_SIZE,
    SUFFIX_SIZE,
    TOTAL_NUMBERS,
    _binary_search,
    _decode_global_index,
    _hash_number,
)


class TestHashNumber(unittest.TestCase):
    def test_first_2547_number(self):
        self.assertEqual(_hash_number(0), hashlib.sha256(b"254700000000").digest())

    def test_last_2547_number(self):
        self.assertEqual(
            _hash_number(NUMBERS_PER_PREFIX - 1),
            hashlib.sha256(b"254799999999").digest(),
        )

    def test_first_2541_number(self):
        self.assertEqual(
            _hash_number(NUMBERS_PER_PREFIX),
            hashlib.sha256(b"254100000000").digest(),
        )

    def test_last_2541_number(self):
        self.assertEqual(
            _hash_number(TOTAL_NUMBERS - 1),
            hashlib.sha256(b"254199999999").digest(),
        )

    def test_returns_32_bytes(self):
        for idx in [0, 1, 12345678, NUMBERS_PER_PREFIX, TOTAL_NUMBERS - 1]:
            with self.subTest(idx=idx):
                self.assertEqual(len(_hash_number(idx)), HASH_SIZE)

    def test_known_number_712345678(self):
        # 0712345678 → E.164 254712345678 → global_index 12345678
        self.assertEqual(_hash_number(12345678), hashlib.sha256(b"254712345678").digest())

    def test_deterministic(self):
        self.assertEqual(_hash_number(42), _hash_number(42))

    def test_unique_per_index(self):
        hashes = {_hash_number(i) for i in range(1000)}
        self.assertEqual(len(hashes), 1000)


class TestDecodeGlobalIndex(unittest.TestCase):
    def test_first_2547(self):
        self.assertEqual(_decode_global_index(0), "254700000000")

    def test_last_2547(self):
        self.assertEqual(_decode_global_index(NUMBERS_PER_PREFIX - 1), "254799999999")

    def test_first_2541(self):
        self.assertEqual(_decode_global_index(NUMBERS_PER_PREFIX), "254100000000")

    def test_last_2541(self):
        self.assertEqual(_decode_global_index(TOTAL_NUMBERS - 1), "254199999999")

    def test_zero_padding(self):
        # index 1 must be "254700000001", not "25471"
        result = _decode_global_index(1)
        self.assertEqual(result, "254700000001")
        self.assertEqual(len(result), 12)

    def test_roundtrip(self):
        # hash(_decode_global_index(i)) must equal _hash_number(i) for all i
        for idx in [0, 1, 999, 12345678, NUMBERS_PER_PREFIX, NUMBERS_PER_PREFIX + 1, TOTAL_NUMBERS - 1]:
            with self.subTest(idx=idx):
                phone = _decode_global_index(idx)
                self.assertEqual(
                    hashlib.sha256(phone.encode()).digest(),
                    _hash_number(idx),
                    f"Round-trip failed: index {idx} → {phone}",
                )


class TestBinarySearch(unittest.TestCase):
    def _make_db(self, indices):
        records = sorted(
            _hash_number(i) + struct.pack(">I", i)
            for i in indices
        )
        f = tempfile.TemporaryFile()
        f.write(b"".join(records))
        f.flush()
        return f, len(records)

    def test_finds_first_record(self):
        f, n = self._make_db([0, 100, 200, 300])
        with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
            self.assertEqual(_binary_search(mm, _hash_number(0), n), 0)
        f.close()

    def test_finds_last_record(self):
        f, n = self._make_db([0, 100, 200, 300])
        with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
            self.assertEqual(_binary_search(mm, _hash_number(300), n), 300)
        f.close()

    def test_finds_middle_record(self):
        indices = [1_000_000, 5_000_000, 10_000_000, 50_000_000, 99_999_999]
        f, n = self._make_db(indices)
        with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
            self.assertEqual(_binary_search(mm, _hash_number(10_000_000), n), 10_000_000)
        f.close()

    def test_returns_none_for_missing_hash(self):
        f, n = self._make_db([0, 100, 200, 300])
        with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
            self.assertIsNone(_binary_search(mm, _hash_number(150), n))
        f.close()

    def test_returns_none_below_range(self):
        f, n = self._make_db([500, 600, 700])
        with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
            self.assertIsNone(_binary_search(mm, _hash_number(1), n))
        f.close()

    def test_returns_none_above_range(self):
        f, n = self._make_db([0, 1, 2])
        with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
            self.assertIsNone(_binary_search(mm, _hash_number(999_999), n))
        f.close()

    def test_single_record_found(self):
        f, n = self._make_db([42])
        with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
            self.assertEqual(_binary_search(mm, _hash_number(42), n), 42)
        f.close()

    def test_single_record_not_found(self):
        f, n = self._make_db([42])
        with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
            self.assertIsNone(_binary_search(mm, _hash_number(43), n))
        f.close()

    def test_2541_prefix_lookup(self):
        # NUMBERS_PER_PREFIX is the boundary index between 2547 and 2541
        indices = [NUMBERS_PER_PREFIX - 1, NUMBERS_PER_PREFIX, NUMBERS_PER_PREFIX + 1]
        f, n = self._make_db(indices)
        with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
            self.assertEqual(
                _binary_search(mm, _hash_number(NUMBERS_PER_PREFIX), n),
                NUMBERS_PER_PREFIX,
            )
        f.close()

    def test_all_records_in_small_db(self):
        indices = list(range(0, 1000, 7))  # 143 evenly-spaced records
        f, n = self._make_db(indices)
        with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
            for idx in indices:
                with self.subTest(idx=idx):
                    self.assertEqual(_binary_search(mm, _hash_number(idx), n), idx)
        f.close()


class TestConstants(unittest.TestCase):
    def test_record_size_is_36(self):
        self.assertEqual(RECORD_SIZE, 36)

    def test_record_size_equals_hash_plus_suffix(self):
        self.assertEqual(RECORD_SIZE, HASH_SIZE + SUFFIX_SIZE)

    def test_total_numbers(self):
        self.assertEqual(TOTAL_NUMBERS, 200_000_000)

    def test_numbers_per_prefix(self):
        self.assertEqual(NUMBERS_PER_PREFIX, 100_000_000)

    def test_prefixes_contain_both_safaricom_ranges(self):
        self.assertIn(b"2547", PREFIXES)
        self.assertIn(b"2541", PREFIXES)
        self.assertEqual(len(PREFIXES), 2)


if __name__ == "__main__":
    unittest.main(verbosity=2)
