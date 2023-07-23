### Deduplication and consolidation utilities

Simple scripts to deduplicate a jsonl file based on a unique id.
Specifically, it's written to deduplicate the annotations from different annotators for
example and to refine NER spans or analyze the differences.

### Deduplication usage

Usage from python:

```python
from annotations_deduplications import Deduplicator

records = [{"user": ..., "metadata": ...}, ...]

deduplicator = Deduplicator(
    comparators=[("text", is_overlap)],  # list of tuples (attribute, comparator)
    aggregation_strategy="mean",
    # attributes to block on, nested attributes can be specified with a dot
    blocking_attributes=["user", "metadata.url"],
    clust_kwargs={"eps": 0.1, "min_samples": 2, "metric": "precomputed"},
)
```

Optionally, you can specify a custom `blocking_rule` instead of `blocking_attributes` (it
was done using [groupbyrule](https://github.com/OlivierBinette/groupbyrule) package):

```python
blocking_rule = Any(Match("attr1", "attr2"),
                    Match("attr2", "attr3"),
                    Match("attr3", "attr4"))
deduplicator = Deduplicator(
    comparators=[("text", is_overlap)],  # list of tuples (attribute, comparator)
    aggregation_strategy="mean",
    # attributes to block on, nested attributes can be specified with a dot
    blocking_rule=blocking_rule,
    clust_kwargs={"eps": 0.1, "min_samples": 2, "metric": "precomputed"},
)

for cluster_id, duplicates in deduplicator(records):
    print(cluster_id, duplicates)
```

### Scripts usage

#### Deduplication

```bash
python3 -m annotations_deduplications.scripts.find_duplicates_cli \
                       --input_path ../../data/alarab-unlemmatized \ 
                       --output ./duplicates_file.jsonl  \
                       --skipped_records ./skipped_records.jsonl \
                       --unique_id id
```

If records were skipped due to absense of the unique id, they will be saved
in `skipped_records.jsonl` file.

#### Consolidation

A consolidation is a record that contains the aggregated annotations from the duplicates.    
It was done using `pytextspan` package, for more details see [pytextspan](https://github.com/tamuhey/textspan).    
To consolidate the duplicates, you can use the following script using previously
generated `duplicates_file.jsonl`:

```bash
python3 -m annotations_deduplications.scripts.make_consolidations_cli \
                       --input_path ../../data/alarab-unlemmatized \ 
                       --duplicates_file ./duplicates_file.jsonl  \
                       --output ./consolidations.jsonl \
                       --unique_id id
```
