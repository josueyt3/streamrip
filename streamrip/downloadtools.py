import asyncio
import functools
import hashlib
import logging
import os
import re
from tempfile import gettempdir
from typing import Callable, Dict, Iterable, List, Optional

import aiofiles
import aiohttp
from Cryptodome.Cipher import Blowfish

from .exceptions import NonStreamable
from .utils import gen_threadsafe_session

logger = logging.getLogger("streamrip")

class DownloadStream:
    """An iterator over chunks of a stream."""

    is_encrypted = re.compile("/m(?:obile|edia)/")

    def __init__(
        self,
        url: str,
        source: str = None,
        params: dict = None,
        headers: dict = None,
        item_id: str = None,
    ):
        self.source = source
        self.session = gen_threadsafe_session(headers=headers)

        self.id = item_id
        if isinstance(self.id, int):
            self.id = str(self.id)

        if params is None:
            params = {}

        self.request = self.session.get(
            url, allow_redirects=True, stream=True, params=params
        )
        self.file_size = int(self.request.headers.get("Content-Length", 0))

        if self.file_size < 20000 and not self.url.endswith(".jpg"):
            import json

            try:
                info = self.request.json()
                raise NonStreamable(f"{info['error']} - {info['message']}")

            except json.JSONDecodeError:
                raise NonStreamable("File not found.")

    def __iter__(self) -> Iterable:
        if self.source == "deezer" and self.is_encrypted.search(self.url) is not None:
            assert isinstance(self.id, str), self.id
            blowfish_key = self._generate_blowfish_key(self.id)
            CHUNK_SIZE = 9000000  # Tamaño de chunk aumentado
            return (
                (self._decrypt_chunk(blowfish_key, chunk[:9000000]) + chunk[9000000:])
                if len(chunk) >= 9000000
                else chunk
                for chunk in self.request.iter_content(CHUNK_SIZE)
            )

        return self.request.iter_content(chunk_size=9000000)  # Aumentar tamaño de chunk a 1 MB

    @property
    def url(self):
        return self.request.url

    def __len__(self) -> int:
        return self.file_size

    @staticmethod
    def _generate_blowfish_key(track_id: str):
        SECRET = "g4el58wc0zvf9na1"
        md5_hash = hashlib.md5(track_id.encode()).hexdigest()
        return "".join(
            chr(functools.reduce(lambda x, y: x ^ y, map(ord, t)))
            for t in zip(md5_hash[:16], md5_hash[16:], SECRET)
        ).encode()

    @staticmethod
    def _decrypt_chunk(key, data):
        return Blowfish.new(
            key,
            Blowfish.MODE_CBC,
            b"\x00\x01\x02\x03\x04\x05\x06\x07",
        ).decrypt(data)


class DownloadPool:
    """Asynchronously download a set of urls."""

    def __init__(
        self,
        urls: Iterable,
        max_connections: int = 8000,  # Aumentar número máximo de conexiones
        tempdir: str = None,
        chunk_callback: Optional[Callable] = None,
    ):
        self.finished: bool = False
        self.urls = dict(enumerate(urls))
        self._downloaded_urls: List[str] = []
        self._paths: Dict[str, str] = {}
        self.task: Optional[asyncio.Task] = None
        self.semaphore = asyncio.Semaphore(max_connections)  # Límite de conexiones

        if tempdir is None:
            tempdir = gettempdir()
        self.tempdir = tempdir

    async def getfn(self, url):
        path = os.path.join(self.tempdir, f"__streamrip_partial_{abs(hash(url))}")
        self._paths[url] = path
        return path

    async def _download_urls(self):
        connector = aiohttp.TCPConnector(limit=8000, force_close=False)
        async with aiohttp.ClientSession(connector=connector) as session:
            tasks = [
                asyncio.create_task(self._download_url(session, url))
                for url in self.urls.values()
            ]
            await asyncio.wait(tasks)

    async def _download_url(self, session, url):
        async with self.semaphore:  # Limitar conexiones simultáneas
            filename = await self.getfn(url)
            logger.debug("Downloading %s", url)
            retries = 3
            for attempt in range(retries):
                try:
                    async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as response:
                        response.raise_for_status()  # Verificar errores de respuesta
                        async with aiofiles.open(filename, "wb") as f:
                            while True:
                                chunk = await response.content.read(1048576)  # Leer en bloques de 1 MB
                                if not chunk:
                                    break
                                await f.write(chunk)
                    break  # Salir del bucle si la descarga es exitosa
                except Exception as e:
                    logger.warning(f"Attempt {attempt + 1}/{retries} failed for {url}: {e}")
                    if attempt == retries - 1:
                        logger.error(f"Failed to download {url} after {retries} attempts.")

        logger.debug("Finished %s", url)

    def download(self, callback=None):
        self.callback = callback
        asyncio.run(self._download_urls())

    @property
    def files(self):
        if len(self._paths) != len(self.urls):
            raise Exception("Must run DownloadPool.download() before accessing files")

        return [
            os.path.join(self.tempdir, self._paths[self.urls[i]])
            for i in range(len(self.urls))
        ]

    def __len__(self):
        return len(self.urls)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        logger.debug("Removing tempfiles %s", self._paths)
        for file in self._paths.values():
            try:
                os.remove(file)
            except FileNotFoundError:
                pass

        return False
