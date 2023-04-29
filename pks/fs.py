import os
import typing
from os import path

import requests

from .cosmos import ClientOptions


# region LocalCache
class LocalCacheConfig:
    def __init__(self, client_options: ClientOptions, output_dir: str = 'output', page_chunk_size: int = (1024 * 1024)):
        self.account = client_options.account
        self.database = client_options.database
        self.collection = client_options.collection

        self.page_chunk_size = page_chunk_size

        self._output_dir = output_dir

        self._fx_path: typing.Optional[str] = None

    def _get_output_dir(self):
        if self._fx_path is not None:
            return self._fx_path

        nfx_path = self._output_dir
        nfx_path = path.join(nfx_path, self.account)
        nfx_path = path.join(nfx_path, self.database)
        nfx_path = path.join(nfx_path, self.collection)
        os.makedirs(nfx_path, exist_ok=True)

        self._fx_path = nfx_path
        return self._fx_path

    def get_output_filename(self, pk_id: typing.Optional[str], key: str, idx: typing.Optional[int] = None) -> str:
        fx_path = self._get_output_dir()
        if pk_id is not None:
            assert len(pk_id) > 0, "length of provided pk_id must be greater than zero"
            fx_path = os.path.join(fx_path, pk_id)
            os.makedirs(fx_path, exist_ok=True)

        filename: str = '%s-%05d.json' % (key, idx) if idx is not None \
            else '%s.json' % key
        return path.join(fx_path, filename)

    @staticmethod
    def valid(filepath: str) -> bool:
        return os.path.exists(filepath) and os.stat(filepath).st_size > 0

    def dump_response(
            self,
            response: requests.Response,
            pk_id: typing.Optional[str],
            key: str,
            idx: typing.Optional[int] = None
    ):
        filename = self.get_output_filename(pk_id, key, idx)
        with open(filename, 'wb') as fp:
            for chunk in response.iter_content(chunk_size=self.page_chunk_size):
                fp.write(chunk)

# endregion
