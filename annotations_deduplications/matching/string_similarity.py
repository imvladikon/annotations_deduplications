#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from typing import Callable, List, Any

import numpy as np


def longest_common_substring(s1, s2):
    m = [[0] * (1 + len(s2)) for i in range(1 + len(s1))]
    longest, x_longest = 0, 0
    for x in range(1, 1 + len(s1)):
        for y in range(1, 1 + len(s2)):
            if s1[x - 1] == s2[y - 1]:
                m[x][y] = m[x - 1][y - 1] + 1
                if m[x][y] > longest:
                    longest = m[x][y]
                    x_longest = x
            else:
                m[x][y] = 0
    return s1[x_longest - longest : x_longest]


def is_overlap(s1, s2):
    s1, s2 = s1.strip(), s2.strip()
    if s1 in s2 or s2 in s1:
        return 1
    return 0


def overlapping_ratio(s1, s2):
    if len(s1) < len(s2):
        s1, s2 = s2, s1
    idx = s1.find(s2)
    if idx != -1:
        return len(s2) / len(s1)
    return 0


class LCStringSimilarity:

    def __init__(
        self, normalizer: Callable[[List[float]], float] = max, **kwargs: Any
    ) -> None:
        self._normalizer = normalizer

    def lcsstr(self, src: str, tar: str) -> str:
        lengths = np.zeros((len(src) + 1, len(tar) + 1), dtype="int")
        longest, i_longest = 0, 0
        for i in range(1, len(src) + 1):
            for j in range(1, len(tar) + 1):
                if src[i - 1] == tar[j - 1]:
                    lengths[i, j] = lengths[i - 1, j - 1] + 1
                    if lengths[i, j] > longest:
                        longest = lengths[i, j]
                        i_longest = i
                else:
                    lengths[i, j] = 0
        return src[i_longest - longest : i_longest]

    def sim(self, src: str, tar: str) -> float:
        if src == tar:
            return 1.0
        elif not src or not tar:
            return 0.0
        return len(self.lcsstr(src, tar)) / self._normalizer([len(src), len(tar)])
