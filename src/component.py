from enum import Enum, unique
import logging
from datetime import datetime, timezone
from typing import Optional

from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException

import dateparser

from tempoapiclient import client_v4

from data_processing import create_table

# Global config keys:
KEY_DEBUG = "debug"
KEY_API_TOKEN = '#api_token'

# Row config keys:
KEY_ENDPOINT = 'endpoint'
KEY_SYNC_OPTIONS = "sync_options"
KEY_DESTINATION = "destination"

# Destination config dict params:
KEY_LOAD_TYPE = "load_type"

# Sync options config dict params:
KEY_DATE_FROM = "date_from"
KEY_DATE_TO = "date_to"

# Config dict constants:
VAL_LAST_RUN = "last run"
VAL_NONE = "none"

# State keys:
KEY_LAST_RUN_DATETIME = "last_run_downloaded_data_up_to_datetime"


# Row config enums:
@unique
class LoadType(Enum):
    FULL = "full_load"
    INCREMENTAL = "incremental_load"


@unique
class Endpoint(Enum):
    WORKLOGS = "worklogs"


# list of mandatory parameters => if some is missing,
# component will fail with readable message on initialization.
REQUIRED_PARAMETERS = (KEY_API_TOKEN, KEY_ENDPOINT, KEY_DESTINATION)
REQUIRED_IMAGE_PARS = []

# Other constants:
SYNC_OPTIONS_NEEDED_ENDPOINTS = (Endpoint.WORKLOGS,)


class TempoExtractor(ComponentBase):
    """
        Extends base class for general Python components. Initializes the CommonInterface
        and performs configuration validation.

        For easier debugging the data folder is picked up by default from `../data` path,
        relative to working directory.

        If `debug` parameter is present in the `config.json`, the default logger is set to verbose DEBUG mode.
    """
    def __init__(self,
                 data_path_override: Optional[str] = None,
                 required_parameters: Optional[list] = None,
                 required_image_parameters: Optional[list] = None):
        super().__init__(data_path_override, required_parameters, required_image_parameters)

        self.validate_configuration_parameters(REQUIRED_PARAMETERS)
        self.validate_image_parameters(REQUIRED_IMAGE_PARS)
        params: dict = self.configuration.parameters

        self.debug = bool(params.get(KEY_DEBUG))

        api_token: str = params[KEY_API_TOKEN]

        self.client = client_v4.Tempo(auth_token=api_token)

        self.endpoint = Endpoint(params[KEY_ENDPOINT])
        self.incremental = LoadType(params[KEY_DESTINATION][KEY_LOAD_TYPE]) is LoadType.INCREMENTAL
        self.sync_options: Optional[dict] = params.get(KEY_SYNC_OPTIONS)

        self.tmp_state = self.get_state_file()

    def run(self):
        """
        Main execution code
        """
        if self.sync_options:
            min_date_modified, max_date_modified = self.handle_sync_options(self.sync_options)
        elif self.endpoint in SYNC_OPTIONS_NEEDED_ENDPOINTS:
            raise UserException("Sync Options must be defined.")

        if self.endpoint is Endpoint.WORKLOGS:
            self.extract_worklogs(date_from=min_date_modified, date_to=max_date_modified)
        else:
            raise Exception("Unexpected execution branch.")

        if self.tmp_state:
            self.write_state_file(self.tmp_state)

    def handle_sync_options(self, sync_options: dict):
        last_run_datetime_str: Optional[str] = self.tmp_state.get(KEY_LAST_RUN_DATETIME, VAL_NONE)
        date_from_str: str = sync_options[KEY_DATE_FROM]
        date_to_str: str = sync_options[KEY_DATE_TO]
        if date_from_str == VAL_LAST_RUN:
            date_from_str = last_run_datetime_str

        if date_from_str != VAL_NONE:
            date_from = dateparser.parse(date_from_str)
            if not date_from:
                raise UserException("Date From parameter could not be parsed.")
        else:
            date_from = None

        if date_to_str != VAL_NONE:
            date_to = dateparser.parse(date_to_str)
            if not date_to:
                raise UserException("Date To parameter could not be parsed.")
        else:
            date_to = None

        self.tmp_state[KEY_LAST_RUN_DATETIME] = datetime.now(tz=timezone.utc).isoformat(timespec="seconds")
        return date_from, date_to

    def extract_worklogs(self, date_from: datetime, date_to: datetime):
        worklogs = self.client.get_worklogs(dateFrom=date_from, dateTo=date_to)
        if isinstance(worklogs, list):
            worklogs_table = create_table(records=worklogs, table_name="worklogs", primary_key=["tempoWorklogId"])
            worklogs_table.save_as_csv_with_manifest(self, incremental=self.incremental, include_csv_header=self.debug)
        else:
            raise UserException(f"API call returned unexpected response: {worklogs}")


"""
        Main entrypoint
"""
if __name__ == "__main__":
    try:
        comp = TempoExtractor()
        # this triggers the run method by default and is controlled by the configuration.action parameter
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
