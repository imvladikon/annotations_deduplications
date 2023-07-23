#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from abc import ABC
from typing import List, Any, Sequence


class BaseBlocking(ABC):

    def __init__(self, blocking_attributes: List[str], **kwargs: Any) -> None:
        self.blocking_attributes = blocking_attributes

    def fit(self, records: Sequence[dict], **kwargs) -> "BaseBlocking":
        raise NotImplementedError("abstract method")

    def iter_blocks(self, **kwargs):
        raise NotImplementedError("abstract method")

    @property
    def num_blocks(self) -> int:
        raise NotImplementedError("abstract property")
