#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import json
import logging
from argparse import ArgumentParser
from itertools import groupby
from pathlib import Path

from annotations_deduplications.deduplicator import Deduplicator
from annotations_deduplications.matching.string_similarity import is_overlap

logger = logging.getLogger(__name__)

if __name__ == '__main__':
    arg_parser = ArgumentParser()
    arg_parser.add_argument('--input_path', type=str, required=True)
    arg_parser.add_argument('--output', type=str, required=True)
    arg_parser.add_argument('--skipped_records', type=str, required=True)
    arg_parser.add_argument('--unique_id', type=str, required=False, default="id")

    args = arg_parser.parse_args()

    records = []
    skipped_records = []
    for file in Path(args.input_path).glob("*.jsonl"):
        file_id = file.stem
        for l in open(file, 'r'):
            record = {"file_id": file_id, **json.loads(l)}
            if not record.get(args.unique_id):
                skipped_records.append(record)
                continue
            records.append(record)

    if skipped_records:
        logger.warning(f"Skipped {len(skipped_records)} records due to missing unique id")
        if args.skipped_records:
            logger.info(f"Writing skipped records to {args.skipped_records}")

            def without(record, key):
                record.pop(key, None)
                return record

            with open(args.skipped_records, 'w') as f:
                for file_id, rs in groupby(skipped_records, key=lambda x: x["file_id"]):
                    f.write(
                        json.dumps(
                            {
                                "file_id": file_id,
                                "records": list(map(lambda x: without(x, "file_id"), rs)),
                            },
                            ensure_ascii=False,
                        )
                    )
                    f.write("\n")
        del skipped_records

    deduplicator = Deduplicator(
        comparators=[("text", is_overlap)],
        aggregation_strategy="mean",
        blocking_attributes=["user", "metadata.url"],
        clust_kwargs={"eps": 0.1, "min_samples": 2, "metric": "precomputed"},
    )
    with open(args.output, 'w') as f:
        for cluster_id, duplicates in deduplicator(records):
            f.write(
                json.dumps(
                    {
                        "cluster_id": cluster_id,
                        "duplicates": [str(_[args.unique_id]) for _ in duplicates],
                    },
                    ensure_ascii=False,
                )
            )
            f.write("\n")

        # LCStringSimilarity().sim(duplictes[0]["text"], duplictes[1]["text"])
