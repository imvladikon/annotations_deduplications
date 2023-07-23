#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
from concurrent.futures import ThreadPoolExecutor
from itertools import combinations
from math import comb
from typing import List, Sequence, Iterable, Dict, Tuple, Callable, Any, Optional

import numpy as np
from groupbyrule import LinkageRule
from tqdm import tqdm

from annotations_deduplications.blockings import RuleBasedBlocking, CartesianBlocking
from annotations_deduplications.blockings.base_blocking import BaseBlocking
from annotations_deduplications.clusterings import DBScanClusterer
from annotations_deduplications.utils import new_id


class Deduplicator:

    def __init__(
            self,
            comparators: List[Tuple[str, Callable]],
            aggregation_strategy: str,
            blocking_attributes: List[str] = [],
            blocking_rule: Optional[LinkageRule] = None,
            clust_kwargs: Dict[str, Any] = None
    ) -> None:
        self.aggregation_strategy = aggregation_strategy
        self.aggregation_fn = {
            "mean": np.mean,
            "median": np.median,
            "max": np.max,
            "min": np.min,
        }[aggregation_strategy]
        self.comparators = comparators
        self.clusterer = DBScanClusterer(**(clust_kwargs or {}))
        self.blocking_fn = self.blocking_factory(blocking_attributes, blocking_rule)

    def blocking_factory(self,
                         blocking_attributes: List[str],
                         blocking_rule: LinkageRule) -> BaseBlocking:
        if not blocking_attributes:
            blocking_fn = CartesianBlocking()
        else:
            blocking_fn = RuleBasedBlocking(blocking_attributes=blocking_attributes,
                                            blocking_rule=blocking_rule)
        return blocking_fn

    def iter_over_candidates(self,
                             records: Sequence[Dict],
                             candidate_links: Iterable[Tuple[int, int]]):
        for ind_a, ind_b in candidate_links:
            yield records[ind_a], records[ind_b]

    def compute_pairwise(self, it, total=None, verbose=False):
        pbar = it if not verbose else tqdm(it, total=total)
        for a, b in pbar:
            scores = []
            for attribute_name, comparator in self.comparators:
                scores.append(comparator(a[attribute_name], b[attribute_name]))
            yield self.aggregation_fn(scores)

    def to_iter(self, cluster_labels: List[int], records: List[Dict]) -> List[Dict]:
        uniq_cluster_labels = set(cluster_labels) - {-1}
        clusters = {cl: {'cluster_id': new_id()} for cl in uniq_cluster_labels}

        for label, mention in zip(cluster_labels, records):
            if label != -1:
                clusters[label].setdefault('mentions', []).append(mention)

        for cluster in list(clusters.values()):
            yield cluster['cluster_id'], list(cluster['mentions'])

    def __call__(self,
                 records: Sequence[Dict],
                 num_threads: Optional[int] = None,
                 similarity_threshold: int = 0.8) -> Iterable[Tuple[str, List[Dict]]]:
        self.blocking_fn.fit(records)

        if num_threads is None:
            num_threads = min(min(os.cpu_count() // 2, 1), len(records))

        def _block_process(group_id, group_records):
            n_records = len(group_records)
            total = comb(n_records, 2)
            candidate_links = list(combinations(range(n_records), 2))
            sim_matrix = np.eye(n_records)
            it = self.iter_over_candidates(group_records, candidate_links)
            iter_sim = self.compute_pairwise(it, total=total, verbose=False)
            i, j = np.triu_indices(n_records, 1)
            sim = np.fromiter(iter_sim, dtype="float")
            sim_matrix[i, j] = sim_matrix[j, i] = sim
            sim_matrix[sim_matrix < similarity_threshold] = 0
            labels = self.clusterer(records, sim_matrix)
            return self.to_iter(labels, group_records)

        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            blocks = zip(*self.blocking_fn.iter_blocks())
            for result in tqdm(executor.map(_block_process, *blocks),
                               total=self.blocking_fn.num_blocks):
                yield from result
