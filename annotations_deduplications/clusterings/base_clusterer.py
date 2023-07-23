#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from typing import Any, List, Dict, Optional

import numpy as np


class BaseClusterer:

    def __init__(self, clust_alg: Any, *args: Any, **kwargs: Any) -> None:
        self.clust_alg = clust_alg

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
