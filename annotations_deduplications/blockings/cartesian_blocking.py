#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from annotations_deduplications.blockings.base_blocking import BaseBlocking
from itertools import combinations


class CartesianBlocking(BaseBlocking):

    def __init__(self, **kwargs):
        super().__init__(blocking_attributes=[], **kwargs)
        self._data = None

    def fit(self, records, **kwargs):
        self._data = records
        return self

    def all_combinations(self):
        for i, j in combinations(range(len(self._data)), 2):
            yield self._data[i], self._data[j]

    def iter_blocks(self, **kwargs):
        yield 0, list(self.all_combinations())

    @property
    def num_blocks(self):
        return 1
