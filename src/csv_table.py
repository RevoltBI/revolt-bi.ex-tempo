from dataclasses import dataclass
from itertools import chain
import logging
from typing import Collection, Iterable, Iterator, Optional, Sequence
import os
import csv

from keboola.component.base import ComponentBase
from keboola.component.dao import TableMetadata
from keboola.csvwriter import ElasticDictWriter


@dataclass(slots=True)
class Table:
    name: str
    columns: Optional[list[str]]
    primary_key: list[str]
    records: Iterable[dict]
    metadata: Optional[TableMetadata] = None
    delete_where_spec: Optional[dict] = None
    file_path: Optional[str] = None
    _saved: bool = False
    _header_included: bool = False
    _is_empty: Optional[bool] = None

    def _is_empty_internal(self):
        if self._saved:    # Empty tables will never be saved.
            return False
        invalid_columns = not self.columns
        if invalid_columns:
            return True
        if isinstance(self.records, Collection):
            return len(self.records) > 0
        records_iterator = iter(self.records)
        test_record = next(records_iterator, None)
        if test_record is None:
            return True
        else:
            self.records = chain((test_record,), records_iterator)
            return False

    @property
    def is_empty(self):
        if self._is_empty is None:
            self._is_empty = self._is_empty_internal()
        return self._is_empty

    def save_as_csv_with_manifest(self,
                                  component: ComponentBase,
                                  incremental: bool,
                                  include_csv_header: bool = False,
                                  overwrite=False):
        if self._saved and not overwrite:
            logging.debug(f"Table already saved, not overwriting. Table: {self}")
            return
        if self.is_empty:
            logging.warning(f"Attempting to save an empty table{' increment' if incremental else ''},"
                            f" nothing will be loaded into table '{self.name}'.")
            return

        table_def = component.create_out_table_definition(name=f"{self.name}.csv",
                                                          is_sliced=False,
                                                          primary_key=self.primary_key,
                                                          columns=self.columns,
                                                          incremental=incremental,
                                                          table_metadata=self.metadata,
                                                          delete_where=self.delete_where_spec)
        os.makedirs(component.tables_out_path, exist_ok=True)
        self.file_path = table_def.full_path
        with ElasticDictWriter(self.file_path, dialect='kbc', fieldnames=table_def.columns.copy()) as csv_writer:
            if include_csv_header:
                csv_writer.writeheader()
                self._header_included = True
            csv_writer.writerows(self.records)
        self.columns = table_def.columns = csv_writer.fieldnames
        component.write_manifest(table_def)
        self._saved = True

    def get_refreshed_records_iterator(self) -> Iterator[dict]:
        if isinstance(self.records, Sequence) or self.is_empty:
            return iter(self.records)    # No need to do anything, we can just iterate over records again
        # Records are not directly recoverable, we need to read them from the created CSV:
        assert self._saved

        def generator():
            with open(self.file_path, "r") as f:
                csv_reader = csv.DictReader(f, fieldnames=self.columns, dialect='kbc')
                if self._header_included:
                    next(csv_reader)    # skipping CSV header
                yield from csv_reader

        self.records: Iterable[dict] = generator()
        return iter(self.records)
