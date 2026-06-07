#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import zlib

SUPPORT = int(os.environ.get("PCY_SUPPORT", "3"))

NUM_BUCKETS = int(os.environ.get("PCY_BUCKETS", "1000"))


def pair_bucket(a, b, num_buckets=NUM_BUCKETS):
  if a > b:
      a, b = b, a
  key = (a + "\x01" + b).encode("utf-8")
  return zlib.crc32(key) % num_buckets
