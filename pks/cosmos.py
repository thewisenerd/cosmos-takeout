import base64
import hashlib
import hmac
import json
import logging
import time
import typing
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from email.utils import formatdate
from urllib.parse import urljoin, quote, urlparse, quote_plus

import decorator
import requests

logger = logging.getLogger('pks.Cosmos')


# region Constants
class Headers:
    ms_version = 'x-ms-version'
    ms_continuation = 'x-ms-continuation'
    ms_session_token = 'x-ms-session-token'
    ms_request_units = 'x-ms-request-charge'
    ms_resource_usage = 'x-ms-resource-usage'
    ms_item_count = 'x-ms-item-count'
    ms_max_item_count = 'x-ms-max-item-count'
    ms_retry_after = 'x-ms-retry-after-ms'
    ms_throughput_fraction_key = 'throughputFraction'


class HeaderValues:
    ms_version = '2020-07-15'


class Keys:
    ms_pkranges = 'PartitionKeyRanges'
    ms_address_key = 'Addresss'
    ms_uri_key = 'physcialUri'

    z_start = "x-vendor-start"


class Constants:
    allowed_conn_modes = ['direct', 'gateway']
    http_methods = ['get', 'post']
    resource_types = ['docs', 'colls', 'pkranges']
    resource_usage_keys = ['documentSize', 'documentsSize', 'documentsCount', 'collectionSize']


# endregion

# region SessionProvider
class SessionProvider(ABC):
    @abstractmethod
    def provide_session(self, url: str):
        pass


class DefaultSessionProvider(SessionProvider):
    def __init__(self):
        self.session_cache: dict[tuple[str, str], requests.Session] = {}

    def provide_session(self, url: str) -> requests.Session:
        parsed = urlparse(url)
        key = parsed[0], parsed[1]

        if key not in self.session_cache:
            logger.debug(f"initializing new session for {key}")
            self.session_cache[key] = requests.Session()
        else:
            logger.debug(f"reusing session for {key}")
        return self.session_cache[key]


# endregion

# region Helpers
def decode_continuation_token(token: str) -> tuple[int, int]:
    decoded = base64.urlsafe_b64decode(token)
    int0 = int.from_bytes(decoded[0:7], 'little')
    int1 = int.from_bytes(decoded[8:16], 'little')
    return int0, int1


def extract_response_header(
        response: requests.Response,
        header: str,
        default: typing.Optional[str] = None
) -> str:
    return response.headers[header] if header in response.headers else default


@decorator.decorator
def retry(func, retry_count=3, *args, **kwargs):
    retry_ctr = 0
    last_exception: typing.Optional[requests.HTTPError] = None
    while retry_ctr < retry_count:
        retry_ctr += 1
        if retry_ctr == retry_count:
            logger.warning(f"retry={retry_ctr}, {retry_count - retry_ctr} attempts left")
        try:
            return func(*args, **kwargs)
        except requests.HTTPError as e:
            response: requests.Response = e.response
            last_exception = e
            if response.status_code == 429:
                ttl = None
                retry_after = extract_response_header(response, Headers.ms_retry_after)
                if retry_after is not None:
                    ttl = float(retry_after) / 1000.0
                if ttl is not None:
                    ttl = ttl + 0.050  # 50ms for RTT
                    logger.debug(f"function failed, retry after {ttl} seconds")
                    time.sleep(ttl)
                else:
                    logger.warning("function failed, but no info about retry-after; retrying after 100ms")
                    time.sleep(0.1)
            elif response.status_code == 410:
                # note: assume arg0 does not work for class functions
                request: CosmosRequest = args[1]
                if request.has_alternate_locations() > 0:
                    request.cycle_next_location()
                else:
                    logger.warning("resource gone, no alternative hosts available")
            else:
                raise e
    if last_exception is not None:
        raise last_exception
    else:
        raise RuntimeError("out of retries, but no exception captured")


# endregion

# region Layer0.Models
@dataclass
class ClientOptions:
    account: str
    database: str
    collection: str
    master_key: str

    conn_mode: str = 'direct'

    proxies: dict = field(default_factory=dict)
    use_proxies: bool = False

    session_provider: SessionProvider = field(default_factory=DefaultSessionProvider)

    def __post_init__(self):
        if self.use_proxies:
            assert 'http' in self.proxies, "cannot use_proxies without providing a 'http' proxy"
            assert 'https' in self.proxies, "cannot use_proxies without providing a 'https' proxy"
        assert self.conn_mode in Constants.allowed_conn_modes, f"conn_mode must be one of {Constants.allowed_conn_modes}"

    # region http
    def http_request_proxies(self) -> dict:
        return self.proxies if self.use_proxies else {}

    def http_request_verify(self) -> bool:
        return not self.use_proxies

    # endregion

    # region db
    def db_conn_mode_direct(self) -> bool:
        return self.conn_mode == 'direct'

    def db_default_location(self) -> str:
        return f"https://{self.account}.documents.azure.com/"

    @staticmethod
    def db_default_headers() -> dict:
        return {
            'accept': 'application/json',
            Headers.ms_version: HeaderValues.ms_version,
        }

    def db_auth_header(
            self,
            verb: str,
            resource_type: str,
            resource_link: str,
            ts: typing.Optional[float] = None
    ) -> dict:
        if ts is None:
            ts = time.time()

        if verb not in Constants.http_methods:
            raise Exception(f"provided verb='{verb}' not in {Constants.http_methods}")
        if resource_type not in Constants.resource_types:
            raise Exception(f"provided resource_type='{resource_type}' not in {Constants.resource_types}")
        fmt_date = formatdate(timeval=ts, localtime=False, usegmt=True)

        payload = '\n'.join([
            verb,
            resource_type,
            resource_link.lower(),
            fmt_date.lower()
        ]) + "\n\n"

        hm = hmac.new(base64.b64decode(self.master_key), payload.encode(), hashlib.sha256)
        sig = base64.b64encode(hm.digest()).decode()

        key_type = 'master'
        version = '1.0'
        auth = f'type={key_type}&ver={version}&sig={sig}'

        return {
            'authorization': quote(auth),
            'x-ms-date': fmt_date
        }

    # endregion


# endregion

# region Layer0
@dataclass
class QueryString:
    query: str

    def __str__(self):
        if len(self.query) == 0:
            return ''
        return '?' + self.query


def make_query_str(params: dict) -> QueryString:
    ret = ''
    for (idx, k) in enumerate(params):
        ret += '' if idx == 0 else '&'
        ret += k
        ret += '='
        ret += quote_plus(params[k])
    return QueryString(ret)


@dataclass
class CosmosRequest:
    # request method
    method: str

    # post headers
    headers: dict | typing.Callable[[], dict]

    # locations to request
    locations: list[str]

    # path to request
    path: str

    # query
    query: typing.Optional[QueryString] = None

    # post body
    data: typing.Optional[str] = None

    # stream response
    stream: bool = False

    # internal
    location_idx: int = 0
    location_gone_loop_marker: int = -1

    def __post_init__(self):
        assert len(self.locations) > 0, "at least one location must be provided for request"
        for location in self.locations:
            assert location.endswith('/'), "location must end with '/' for urljoin to work properly"

    def get_headers(self) -> dict:
        if type(self.headers) == dict:
            return self.headers
        return self.headers()

    def get_location(self) -> str:
        return self.locations[self.location_idx]

    def cycle_next_location(self) -> None:
        self.location_idx = (self.location_idx + 1) % len(self.locations)

    def has_alternate_locations(self) -> bool:
        return len(self.locations) > 1


class CosmosClient0:
    def __init__(self, options: ClientOptions):
        self.options = options

    @retry
    def aio_request(
            self,
            request: CosmosRequest
    ) -> requests.Response:
        assert request.method in Constants.http_methods, f"method='{request.method}' not in {Constants.http_methods}"
        logger.debug(f"preparing request for location='{request.get_location()}' path='{request.path}'")
        url = urljoin(request.get_location(), request.path)
        if request.query is not None:
            url += str(request.query)

        prepared = requests.Request(
            method=request.method,
            url=url,
            headers=request.get_headers(),
            data=request.data
        ).prepare()

        session = self.options.session_provider.provide_session(prepared.url)
        response = session.send(
            prepared,
            stream=request.stream,
            proxies=self.options.http_request_proxies(),
            verify=self.options.http_request_verify()
        )
        response.raise_for_status()
        return response

    def aio_request_paginated(
            self,
            request: CosmosRequest,
            start_continuation_token: typing.Optional[str] = None
    ) -> typing.Generator[requests.Response, None, None]:
        continuation_token = start_continuation_token if start_continuation_token is not None else ''

        session_token = ''
        request_headers_orig = request.headers
        while continuation_token is not None:
            continuation_headers = {}
            if continuation_token != '':
                continuation_headers[Headers.ms_continuation] = continuation_token
            if session_token is not None and session_token != '':
                continuation_headers[Headers.ms_session_token] = session_token

            # fix header
            def get_headers_with_continuation() -> dict:
                if type(request_headers_orig) == dict:
                    return request_headers_orig | continuation_headers
                else:
                    return request_headers_orig() | continuation_headers

            request.headers = get_headers_with_continuation

            # cycle to next address for next request
            if request.has_alternate_locations():
                request.cycle_next_location()

            response: requests.Response = self.aio_request(request)
            yield response

            continuation_token = extract_response_header(response, Headers.ms_continuation)
            session_token = extract_response_header(response, Headers.ms_session_token)


# endregion

# region Client.Models
@dataclass
class CosmosData:
    data: dict

    def id(self) -> str:
        return self.data['id']

    def rid(self) -> str:
        return self.data['_rid']

    def self(self) -> str:
        return self.data['_self']


DbDat = typing.TypeVar("DbDat", bound=CosmosData)


def _db_dat(response: requests.Response | dict, t: typing.Type[DbDat]):
    if type(response) == dict:
        return t(response)
    if type(response) == requests.Response:
        return t(response.json())
    raise RuntimeError(f"failed to identify type for class type casting, type='{type(response)}'")


@dataclass
class CosmosCollection(CosmosData):
    pass


@dataclass
class PartitionKeyRange(CosmosData):
    def range(self):
        return '%s-%s' % (self.data['minInclusive'], self.data['maxExclusive'])


@dataclass
class PartitionKeyRangesPage(CosmosData):
    def pk_ranges(self) -> list[PartitionKeyRange]:
        return [PartitionKeyRange(x) for x in self.data[Keys.ms_pkranges]]


@dataclass
class Address(CosmosData):
    def get_uri(self) -> str:
        return self.data[Keys.ms_uri_key]


@dataclass
class AddressQueryResult(CosmosData):
    def get_address_list(self) -> list[Address]:
        return [Address(x) for x in self.data[Keys.ms_address_key]]


# endregion

# region Client
class CosmosClient(CosmosClient0):
    def get_collection(self) -> CosmosCollection:
        locations = [self.options.db_default_location()]
        resource_link = f'dbs/{self.options.database}/colls/{self.options.collection}'

        request = CosmosRequest(
            'get',
            lambda: self.options.db_auth_header('get', 'colls', resource_link) | self.options.db_default_headers(),
            locations,
            resource_link
        )

        return _db_dat(self.aio_request(request), CosmosCollection)

    def get_pkranges_paginated(self) -> typing.Generator[PartitionKeyRangesPage, None, None]:
        locations = [self.options.db_default_location()]
        resource_link = f'dbs/{self.options.database}/colls/{self.options.collection}'

        request = CosmosRequest(
            'get',
            lambda: self.options.db_auth_header('get', 'pkranges', resource_link) | self.options.db_default_headers(),
            locations,
            resource_link + '/pkranges'
        )

        for page in self.aio_request_paginated(request):
            yield _db_dat(page, PartitionKeyRangesPage)

    def get_address_list(self, coll: CosmosCollection, pk: PartitionKeyRange) -> AddressQueryResult:
        locations = [self.options.db_default_location()]
        path = '/addresses/'
        query_str = make_query_str({
            '$resolveFor': coll.self() + "docs",
            '$filter': 'protocol eq https',
            '$partitionKeyRangeIds': pk.id()
        })

        request = CosmosRequest(
            'get',
            lambda: self.options.db_auth_header('get', 'docs', coll.rid().lower()) | self.options.db_default_headers(),
            locations,
            path,
            query_str
        )

        return _db_dat(self.aio_request(request), AddressQueryResult)

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
            continuation_token: typing.Optional[str] = None
    ) -> typing.Generator[requests.Response, None, None]:
        resource_link = f'dbs/{self.options.database}/colls/{self.options.collection}'

        body = {
            "query": query_str,
        }

        if query_params is not None:
            parameters = []
            for (name, value) in query_params:
                parameters.append({
                    "name": name,
                    "value": value,
                })
            if len(parameters) > 0:
                body["parameters"] = parameters

        locations = []
        if self.options.db_conn_mode_direct():
            for address in address_list:
                locations.append(address.get_uri())
            assert len(locations) > 0, "at least one location must be present in address_list, is empty"

        def generate_headers() -> dict:
            return self.options.db_auth_header('post', 'docs', resource_link) | \
                self.options.db_default_headers() | {
                    'content-type': 'application/query+json',
                    'accept': 'application/json',

                    'x-ms-documentdb-collection-rid': coll.rid(),
                    'x-ms-documentdb-partitionkeyrangeid': pk.id(),
                    'x-ms-documentdb-isquery': 'true',

                    Headers.ms_max_item_count: str(max_item_count),
                }

        request = CosmosRequest(
            'post',
            generate_headers,
            locations,
            f'{resource_link}/docs',
            data=json.dumps(body),
            stream=True
        )

        return self.aio_request_paginated(request, start_continuation_token=continuation_token)
# endregion
