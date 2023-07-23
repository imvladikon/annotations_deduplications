#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from typing import Any, List, Dict, Optional

import numpy as np
from sklearn.cluster import DBSCAN

from annotations_deduplications.clusterings.base_clusterer import BaseClusterer


class DBScanClusterer(BaseClusterer):

    def __init__(
        self, eps: float = 0.5, min_samples: int = 2, metric: str = 'precomputed'
    ) -> None:
        super().__init__(clust_alg=DBSCAN(eps=eps, min_samples=min_samples, metric=metric))

    def fit_predict(self, *args, **kwargs) -> Any:
        return self.clust_alg.fit_predict(*args, **kwargs)

    def __call__(
        self,
        mentions_list: List[Dict],
        similarity_matrix: np.ndarray,
        num_threads: Optional[int] = None,
        **kwargs
    ) -> List[int]:
        dist_matrix = 1.0 - similarity_matrix
        indices = self.fit_predict(dist_matrix, **kwargs)
        labels = list(indices)
        return labels
