import configparser
import logging
import sys

from pks import setup_logger as setup_pks_logger
from pks.cosmos import ClientOptions, CosmosClient
from pks.extensions import QueryCollectionWithCheckpoint, ProgressBar
from pks.fs import LocalCacheConfig

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(logging.Formatter(logging.BASIC_FORMAT))

_log_level = logging.INFO
logger = logging.getLogger('Runner')
logger.propagate = False
logger.addHandler(ch)
logger.setLevel(_log_level)
setup_pks_logger(handlers=[ch], level=_log_level)

_cfg_file = 'config.ini'


def main() -> int:
    cfg = configparser.ConfigParser()
    cfg.read(_cfg_file)
    options = ClientOptions(
        account=cfg['root']['account'],
        database=cfg['root']['database'],
        collection=cfg['root']['collection'],
        master_key=cfg['root']['key'].strip('"')
    )
    lc_config = LocalCacheConfig(options)
    client = CosmosClient(options)

    coll = client.get_collection()
    for pk_page_idx, pk_page in enumerate(client.get_pkranges_paginated()):
        for pk_idx, pk in enumerate(pk_page.pk_ranges()):
            address_result = client.get_address_list(coll, pk)
            address_list = address_result.get_address_list()

            pg = ProgressBar(pk.id())
            querier = QueryCollectionWithCheckpoint(client, lc_config)
            query = querier.query_collection(coll, pk, address_list, 'select * from c')

            for qr_idx, qr_page in query:
                progress = pg.process_page(qr_page)
                lc_config.dump_response(qr_page, pk.id(), 'docs', qr_idx)
                print(f'[page={pk_page_idx}][pk={pk_idx}] {progress}')

    return 0


if __name__ == '__main__':
    rc = main()
    sys.exit(rc)
