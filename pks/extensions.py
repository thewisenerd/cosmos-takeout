import json
import logging
import math
import os
import time
import typing
from dataclasses import dataclass, field
from datetime import timedelta
from os import path

import requests

from .cosmos import CosmosClient, CosmosCollection, PartitionKeyRange, Address, Headers, Keys, extract_response_header
from .fs import LocalCacheConfig

logger = logging.getLogger('pks.Cosmos.Extensions')


# region QueryCollectionWithCheckpoint
# if a task seems to be completed already
class QueryCompleteException(BaseException):
    def __init__(self, msg):
        super().__init__(msg)


class QueryCollectionWithCheckpoint:
    def __init__(self, client: CosmosClient, cache_config: LocalCacheConfig, checkpoint_interval: int = 10):
        self.client = client
        self.local_cache_config = cache_config
        self.checkpoint_interval = checkpoint_interval

        assert checkpoint_interval > 0, f"checkpoint_interval must be greater than zero, provided='{checkpoint_interval}'"

    def _checkpoint_read_impl(self, pk: PartitionKeyRange) -> tuple[typing.Optional[str], int]:
        token: typing.Optional[str] = None
        start = 0

        pt_filename = self.local_cache_config.get_output_filename(pk.id(), f'continuation-token')
        if path.isfile(pt_filename) and os.stat(pt_filename).st_size > 0:
            logger.debug(f"loading continuation overrides for {pk}")

            with open(pt_filename) as fp0:
                pt_data = json.load(fp0)
            token = pt_data[Headers.ms_continuation]
            start = pt_data[Keys.z_start]

            if start < 0:
                raise QueryCompleteException(
                    "continuation token exists with sub-zero value."
                    " this is indicative of the checkpointed query having already completed.")

        return token, start

    def _checkpoint_write_impl(self, pk: PartitionKeyRange, ct: typing.Optional[str], ct_start: int):
        pt_filename = self.local_cache_config.get_output_filename(pk.id(), f'continuation-token')
        with open(pt_filename, 'w') as fp_int:
            json.dump({
                Headers.ms_continuation: ct,
                Keys.z_start: ct_start + 1
            }, fp_int)

    def query_collection(
            self,

            # immutable
            coll: CosmosCollection,
            pk: PartitionKeyRange,
            address_list: list[Address],

            # query
            query_str: str,
            query_params: typing.Optional[list[tuple]] = None,

            # params
            max_item_count: int = 1000,
    ) -> typing.Generator[tuple[int, requests.Response], None, None]:
        req_continuation_token, req_start = self._checkpoint_read_impl(pk)
        iterable = self.client.query_collection(coll, pk, address_list, query_str,
                                                query_params, max_item_count, req_continuation_token)
        for idx, page in enumerate(iterable, req_start):
            yield idx, page

            if idx > 0 and idx % self.checkpoint_interval == 0:
                self._checkpoint_write_impl(pk, extract_response_header(page, Headers.ms_continuation), idx)

        self._checkpoint_write_impl(pk, None, -2)


# endregion

# region ProgressBar
@dataclass()
class ProgressIndicator:
    fetched: int
    indeterminate: int
    total: typing.Optional[int]

    elapsed_seconds: float
    estimate_seconds: float = field(default=math.nan)
    estimate_remaining_seconds: float = field(default=math.nan)

    speed: float = field(default=math.nan)

    # calculated
    percent: float = field(init=False)
    width: int = field(init=False)

    def __post_init__(self):
        if self.total is not None:
            self.percent = self.fetched / float(self.total)
            self.width = int(math.log10(self.total)) + 1
            if math.isnan(self.estimate_seconds):
                self.estimate_seconds = self.elapsed_seconds / self.percent
            if math.isnan(self.estimate_remaining_seconds):
                self.estimate_remaining_seconds = self.estimate_seconds - self.elapsed_seconds
            if math.isnan(self.speed):
                self.speed = self.fetched / self.elapsed_seconds
        else:
            self.percent = math.nan

    def __str__(self) -> str:
        r_fetched = '{:{}d}'.format(self.fetched, self.width)
        r_indeterminate = f" and {self.indeterminate} pages" if self.indeterminate > 0 else ""
        speed_1k = self.speed / 1000
        speed_decimal_digits = 2 if speed_1k < 10 else 1
        r_percent = ' {:>6.2f}% complete, speed={:>4.{}f}k'.format(
            self.percent * 100,
            speed_1k,
            speed_decimal_digits
        ) if self.total > 0 else ''
        r_eta = ' (elapsed=%s, eta=%s)' % (
            timedelta(seconds=int(self.elapsed_seconds)),
            timedelta(seconds=int(self.estimate_remaining_seconds))
        ) if self.total > 0 else ''

        return ' '.join([
            f'{r_fetched}{r_indeterminate}/{self.total}',
            r_percent,
            r_eta
        ])


class ProgressBar:
    def __init__(self, name: str):
        self.name = name
        self.docs_count: typing.Optional[int] = None
        self.fetch_count: int = 0
        self.indeterminate_pages: int = 0
        self.start_time: float = time.time()

    def _setup_docs_count(self, page: requests.Response) -> typing.Optional[int]:
        resource_usage = extract_response_header(page, Headers.ms_resource_usage)
        logger.debug(f"inferred resourceUsage={resource_usage}")
        for kv in resource_usage.split(';'):
            if len(kv) > 0:
                k, v = kv.split('=')
                if k == 'documentsCount':
                    self.docs_count = int(v)
        return self.docs_count

    def current_progress_indicator(self) -> ProgressIndicator:
        return ProgressIndicator(
            self.fetch_count,
            self.indeterminate_pages,
            self.docs_count,
            time.time() - self.start_time
        )

    def process_page(self, page: requests.Response) -> ProgressIndicator:
        item_count = extract_response_header(page, Headers.ms_item_count)
        if item_count is not None:
            self.fetch_count += int(item_count)
        else:
            self.indeterminate_pages += 1

        if self.docs_count is None:
            self._setup_docs_count(page)

        return self.current_progress_indicator()
# endregion
