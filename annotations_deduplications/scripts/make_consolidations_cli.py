#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import json
import logging
from argparse import ArgumentParser
from pathlib import Path

from annotations_deduplications import AnnotationsConsolidator

logger = logging.getLogger(__name__)

if __name__ == '__main__':
    arg_parser = ArgumentParser()
    arg_parser.add_argument('--input_path', type=str, required=True)
    arg_parser.add_argument('--duplicates_file', type=str, required=True)
    arg_parser.add_argument('--output', type=str, required=True)
    arg_parser.add_argument('--unique_id', type=str, required=False, default="id")
    args = arg_parser.parse_args()

    unique_id = args.unique_id

    duplicates = {}
    with open(args.duplicates_file, 'r') as f:
        for l in f:
            d = json.loads(l)
            for id in d["duplicates"]:
                duplicates[id] = d["cluster_id"]

    duplicated_records = {}
    for file in Path(args.input_path).glob("*.jsonl"):
        file_id = file.stem
        for l in open(file, 'r'):
            record = {"file_id": file_id, **json.loads(l)}
            if not record.get(unique_id):
                continue
            if str(record["id"]) in duplicates:
                cluster_id = duplicates[str(record["id"])]
                duplicated_records.setdefault(cluster_id, []).append(record)

    with open(args.output, 'w') as f:
        for cluster_id, duplicated_records in duplicated_records.items():
            if len(duplicated_records) == 1:
                continue
            merger = AnnotationsConsolidator()
            for record in duplicated_records:
                merger.add(record)
            consolidation_info = {
                "cluster_id": cluster_id,
                "merged_ids": merger.merged_ids,
                "consolidated": merger.consolidated,
            }
            f.write(json.dumps(consolidation_info, ensure_ascii=False))
            f.write("\n")
