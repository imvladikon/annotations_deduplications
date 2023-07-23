#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from typing import Sequence, Optional

import pandas as pd
from groupbyrule import Match, LinkageRule
from tqdm import tqdm

from annotations_deduplications.blockings.base_blocking import BaseBlocking


class RuleBasedBlocking(BaseBlocking):

    def __init__(
            self, blocking_attributes=[], blocking_rule: Optional[LinkageRule] = None,
            **kwargs
    ):
        super().__init__(blocking_attributes, **kwargs)

        if blocking_rule:
            self.rule = blocking_rule
        elif blocking_attributes:
            self.rule = Match(*blocking_attributes)
        else:
            raise ValueError(
                "Either blocking_attributes" " or blocking_rule must be provided"
            )
        self._data = None
        self._num_blocks = 0

    def fit(self, records: Sequence[dict], **kwargs) -> "RuleBasedBlocking":
        data = []
        for record in records:
            item = {}
            for k, v in record.items():
                if isinstance(v, dict):
                    for kk, vv in v.items():
                        item[f"{k}.{kk}"] = vv
                else:
                    item[k] = v
            data.append(item)
        if not data:
            raise ValueError("No data to fit")
        data = pd.DataFrame(data)
        self.rule.fit(data)
        data["blocking_id"] = self.rule.groups
        self._data = data
        self._num_blocks = len(data["blocking_id"].unique())
        return self

    def iter_blocks(self, verbose=False, **kwargs):
        groups = self._data.groupby("blocking_id")
        pbar = groups if not verbose else tqdm(groups, total=len(groups))
        for group_id, group in pbar:
            yield group_id, group.to_dict(orient="records")

    @property
    def num_blocks(self) -> int:
        return self._num_blocks
