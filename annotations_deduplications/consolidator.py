#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import copy

import logging
import textspan

logger = logging.getLogger(__name__)


class AnnotationsConsolidator:

    def __init__(self, **kwargs):
        self.consolidated = {}
        self.merged_ids = {}

    def add(self, record):
        if not self.consolidated:
            self.consolidated[record["id"]] = record
            return record

        merging_info = {}
        for id in self.consolidated:
            record_b = self.consolidated[id]
            ret = self.merge(record, record_b)
            if ret is None:
                merging_info = {
                    "ids": [record["id"], record_b["id"]],
                    "consolidations": [record, record_b]
                }
            else:
                merging_info = {
                    "ids": [record["id"], record_b["id"]],
                    "consolidations": [ret],
                }
                self.merged_ids[record["id"]] = ret["id"]
                self.merged_ids[record_b["id"]] = ret["id"]
                break

        if merging_info:
            for id in merging_info["ids"]:
                self.consolidated.pop(id, None)

            for consolidation in merging_info["consolidations"]:
                self.consolidated[consolidation["id"]] = consolidation

    def merge(self, record_a, record_b):

        if not record_a:
            return None

        span_a, text_a = record_a["label"], record_a["text"]
        span_b, text_b = record_b["label"], record_b["text"]

        if not span_b:
            return None

        if not span_a:
            return None

        if len(text_a) < len(text_b):
            text_a, text_b = text_b, text_a
            span_a, span_b = span_b, span_a

        try:
            aligned_spans = textspan.align_spans([(sp[0], sp[1]) for sp in span_b],
                                                 text_b, text_a)
        except Exception as e:
            logger.warning("merging error for %s and %s"
                           " due to %s", record_a["id"], record_b["id"], e)
            return None

        if not len(aligned_spans):
            return None

        idx = text_a.find(text_b)
        if idx != -1:
            start_idx = idx
            end_idx = start_idx + len(text_b)
        else:
            return None

        def _is_within(span):
            start, end = span
            return start_idx <= start <= end <= end_idx

        aligned_spans = [
            span for spans in aligned_spans for span in spans if _is_within(span)
        ]
        aligned_spans = [
            (start, end, tag) for (start, end), (_, _, tag) in zip(aligned_spans, span_b)
        ]
        span_a.extend(aligned_spans)
        span_a = list(set(map(tuple, span_a)))
        span_a = sorted(span_a, key=lambda x: x[0])
        ret = copy.deepcopy(record_a)
        ret["id"] = min(record_a["id"], record_b["id"])
        ret["label"] = span_a
        ret["text"] = text_a
        return ret
