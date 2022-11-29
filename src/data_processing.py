from itertools import chain
import logging
from typing import Callable, Iterable, Mapping, MutableMapping, Optional

from keboola.utils.header_normalizer import DefaultHeaderNormalizer

from csv_table import Table

HEADER_NORMALIZER = DefaultHeaderNormalizer()


def rename_dict_keys(d: MutableMapping, key_name_mapping: Mapping):
    return {key_name_mapping.get(key, key): value for key, value in d.items()}


def flatten_dict(d: Mapping, parent_key: str = '', sep: str = '_'):
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, MutableMapping):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def create_table(records: Iterable[dict],
                 table_name: str,
                 primary_key: list[str],
                 delete_where_spec: Optional[dict] = None,
                 record_processor: Callable[[dict], dict] = flatten_dict,
                 normalize_header: bool = True):
    records = iter(records)
    records_processed = (record_processor(d) for d in records)
    record_processed: dict = next(records_processed, None)
    if record_processed is None:
        logging.warning(f"API returned no records for output table '{table_name}'.")
        return Table(name=table_name, columns=None, primary_key=primary_key, records=records_processed)
    records_processed = chain((record_processed,), records_processed)
    columns = list(record_processed.keys())
    if normalize_header:
        denormalized_columns = columns
        columns = HEADER_NORMALIZER.normalize_header(columns)
        column_name_mapping = {
            denorm_name: norm_name
            for denorm_name, norm_name in zip(denormalized_columns, columns)
            if denorm_name != norm_name
        }
        records_processed = (rename_dict_keys(d, column_name_mapping) for d in records_processed)
    for pk in primary_key:
        if pk not in columns:
            raise ValueError(f"Invalid primary key. Primary key element '{pk}' not found in columns: {columns}.")
    return Table(name=table_name,
                 columns=columns,
                 primary_key=primary_key,
                 records=records_processed,
                 delete_where_spec=delete_where_spec)
