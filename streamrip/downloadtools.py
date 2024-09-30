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
    """An iterator over chunks of a stream.

    Usage:

        >>> stream = DownloadStream('https://google.com', None)
        >>> with open('google.html', 'wb') as file:
        >>>     for chunk in stream:
        >>>         file.write(chunk)

    """

    is_encrypted = re.compile("/m(?:obile|edia)/")

    def __init__(
        self,
        url: str,
        source: str = None,
        params: dict = None,
        headers: dict = None,
        item_id: str = None,
    ):
        """Create an iterable DownloadStream of a URL.

        :param url: The url to download
        :type url: str
        :param source: Only applicable for Deezer
        :type source: str
        :param params: Parameters to pass in the request
        :type params: dict
        :param headers: Headers to pass in the request
        :type headers: dict
        :param item_id: (Only for Deezer) the ID of the track
        :type item_id: str
        """
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
                try:
                    # Usually happens with deezloader downloads
                    raise NonStreamable(f"{info['error']} - {info['message']}")
                except KeyError:
                    raise NonStreamable(info)

            except json.JSONDecodeError:
                raise NonStreamable("File not found.")

    def __iter__(self) -> Iterable:
        """Iterate through chunks of the stream.

        :rtype: Iterable
        """
        if self.source == "deezer" and self.is_encrypted.search(self.url) is not None:
            assert isinstance(self.id, str), self.id

            blowfish_key = self._generate_blowfish_key(self.id)
            # decryptor = self._create_deezer_decryptor(blowfish_key)
            CHUNK_SIZE = 2048 * 3
            return (
                # (decryptor.decrypt(chunk[:2048]) + chunk[2048:])
                (self._decrypt_chunk(blowfish_key, chunk[:2048]) + chunk[2048:])
                if len(chunk) >= 2048
                else chunk
                for chunk in self.request.iter_content(CHUNK_SIZE)
            )

        return self.request.iter_content(chunk_size=1024)

    @property
    def url(self):
        """Return the requested url."""
        return self.request.url

    def __len__(self) -> int:
        """Return the value of the "Content-Length" header.

        :rtype: int
        """
        return self.file_size

    def _create_deezer_decryptor(self, key) -> Blowfish:
        return Blowfish.new(key, Blowfish.MODE_CBC, b"\x00\x01\x02\x03\x04\x05\x06\x07")

    @staticmethod
    def _generate_blowfish_key(track_id: str):
        """Generate the blowfish key for Deezer downloads.

        :param track_id:
        :type track_id: str
        """
        SECRET = "g4el58wc0zvf9na1"
        md5_hash = hashlib.md5(track_id.encode()).hexdigest()
        # good luck :)
        return "".join(
            chr(functools.reduce(lambda x, y: x ^ y, map(ord, t)))
            for t in zip(md5_hash[:16], md5_hash[16:], SECRET)
        ).encode()

    @staticmethod
    def _decrypt_chunk(key, data):
        """Decrypt a chunk of a Deezer stream.

        :param key:
        :param data:
        """
        return Blowfish.new(
            key,
            Blowfish.MODE_CBC,
            b"\x00\x01\x02\x03\x04\x05\x06\x07",
        ).decrypt(data)


class DownloadPool:
    """Asynchronously download a set of URLs with high concurrency and support for segmented downloads."""

    def __init__(
        self,
        urls: Iterable,
        tempdir: str = None,
        chunk_callback: Optional[Callable] = None,
        max_concurrent_downloads: int = 800,  # Adjustable concurrency limit
        segment_count: int = 400,  # Number of segments to split the download into
    ):
        self.finished: bool = False
        self.urls = dict(enumerate(urls))
        self._downloaded_urls: List[str] = []
        self._paths: Dict[str, str] = {}
        self.task: Optional[asyncio.Task] = None
        self.max_concurrent_downloads = max_concurrent_downloads
        self.semaphore = asyncio.Semaphore(max_concurrent_downloads)  # Semaphore for concurrency control
        self.segment_count = segment_count  # Number of segments for multi-threaded downloads

        if tempdir is None:
            tempdir = gettempdir()
        self.tempdir = tempdir

    async def getfn(self, url):
        path = os.path.join(self.tempdir, f"__streamrip_partial_{abs(hash(url))}")
        self._paths[url] = path
        return path

    async def _download_urls(self):
        async with aiohttp.ClientSession() as session:
            tasks = [
                self._download_url(session, url)  # Download single files
                for url in self.urls.values()
            ]
            await asyncio.gather(*tasks)

    async def _download_url(self, session, url):
        # Call segmented download
        await self._download_file_in_segments(session, url)

    async def _download_file_in_segments(self, session, url):
        filename = await self.getfn(url)
        logger.debug("Starting segmented download for %s", url)

        async with session.head(url) as response:
            response.raise_for_status()
            file_size = int(response.headers.get("Content-Length", 0))

        # Calculate the size of each segment
        segment_size = file_size // self.segment_count

        # Create tasks for each segment download
        tasks = [
            self._download_segment(session, url, filename, i * segment_size, 
                                   segment_size if i < self.segment_count - 1 else None)
            for i in range(self.segment_count)
        ]
        await asyncio.gather(*tasks)
        logger.debug("Completed segmented download for %s", url)

    async def _download_segment(self, session, url, filename, start, size):
        async with self.semaphore:
            headers = {'Range': f'bytes={start}-{(start + size - 1) if size is not None else ""}'}
            logger.debug("Downloading segment %d from %s", start, url)

            async with session.get(url, headers=headers) as response:
                response.raise_for_status()
                async with aiofiles.open(filename, "r+b") as f:
                    await f.seek(start)
                    await f.write(await response.read())

            logger.debug("Finished downloading segment starting at byte %d for %s", start, url)

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
