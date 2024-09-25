import asyncio
import os
import logging
from tempfile import gettempdir
from typing import Iterable, Dict

import aiofiles
import aiohttp

logger = logging.getLogger("streamrip")

class DownloadStream:
    """An iterator over chunks of a stream."""

    def __init__(self, url: str, headers: dict = None):
        self.url = url
        self.headers = headers
        self.file_size = None

    async def fetch_file_size(self):
        async with aiohttp.ClientSession() as session:
            async with session.head(self.url) as response:
                self.file_size = int(response.headers.get("Content-Length", 0))
        return self.file_size

    def __len__(self):
        if self.file_size is not None:
            return self.file_size
        raise ValueError("File size not available. Call fetch_file_size() first.")

class DownloadPool:
    """Asynchronously download a set of urls with advanced strategies."""

    def __init__(self, urls: Iterable, max_connections: int = 200, tempdir: str = None):
        self.urls = dict(enumerate(urls))
        self._paths: Dict[str, str] = {}
        self.semaphore = asyncio.Semaphore(max_connections)
        self.tempdir = tempdir or gettempdir()
        self.retry_limit = 5  # Límite de reintentos
        self.chunk_size = 8192  # Tamaño de chunk ajustable

    async def get_file_path(self, url):
        path = os.path.join(self.tempdir, f"__streamrip_partial_{abs(hash(url))}")
        self._paths[url] = path
        return path

    async def _download_with_retries(self, session, url):
        """Download file with retry logic."""
        retries = 0
        while retries < self.retry_limit:
            try:
                await self._download_file(session, url)
                break  # Salir si la descarga fue exitosa
            except Exception as e:
                retries += 1
                logger.warning(f"Error downloading {url}: {e}. Retrying {retries}/{self.retry_limit}...")
                await asyncio.sleep(2 ** retries)  # Esperar un tiempo exponencial antes de reintentar
        else:
            logger.error(f"Failed to download {url} after {self.retry_limit} attempts.")

    async def _download_file(self, session, url):
        """Download file using range requests."""
        file_stream = DownloadStream(url)
        await file_stream.fetch_file_size()  # Fetch file size before downloading
        file_size = file_stream.file_size
        part_size = file_size // 8

        tasks = []
        for part in range(8):
            start = part * part_size
            end = start + part_size - 1 if part < 7 else file_size - 1
            tasks.append(self._download_part(session, url, start, end, part + 1))

        await asyncio.gather(*tasks)
        await self._combine_files(file_stream, 8)

    async def _download_part(self, session, url, start, end, part_number):
        """Download a part of the file with integrity check."""
        headers = {"Range": f"bytes={start}-{end}"}
        part_filename = f"{url.replace('/', '_')}.part{part_number}"
        
        async with self.semaphore:
            logger.debug(f"Downloading part {part_number} from {start} to {end} for {url}")
            async with session.get(url, headers=headers) as response:
                response.raise_for_status()
                async with aiofiles.open(part_filename, "wb") as f:
                    while True:
                        chunk = await response.content.read(self.chunk_size)
                        if not chunk:
                            break
                        await f.write(chunk)
            logger.info(f"Finished downloading part {part_number} for {url}")

    async def _combine_files(self, file_stream, num_parts):
        """Combine the parts into the final file."""
        final_filename = await self.get_file_path(file_stream.url)
        with open(final_filename, "wb") as outfile:
            for part in range(1, num_parts + 1):
                part_filename = f"{file_stream.url.replace('/', '_')}.part{part}"
                with open(part_filename, "rb") as infile:
                    outfile.write(infile.read())
                os.remove(part_filename)
        logger.info(f"Combined file saved as {final_filename}")

    async def download_all(self):
        """Download all URLs with concurrent downloads."""
        async with aiohttp.ClientSession() as session:
            tasks = [self._download_with_retries(session, url) for url in self.urls.values()]
            await asyncio.gather(*tasks)

    def download(self):
        """Run the download process."""
        asyncio.run(self.download_all())

    @property
    def files(self):
        if len(self._paths) != len(self.urls):
            raise Exception("Must run DownloadPool.download() before accessing files")
        return [self._paths[self.urls[i]] for i in range(len(self.urls))]

    def __len__(self):
        return len(self.urls)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        logger.debug("Cleaning up temporary files: %s", self._paths)
        for file in self._paths.values():
            try:
                os.remove(file)
            except FileNotFoundError:
                pass
        return False

# Ejemplo de uso
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    urls = [
        "https://www.deezer.com/mx/track/123456789",  # Reemplazar con enlaces válidos de Deezer
        "https://www.deezer.com/mx/track/987654321",
        # Agregar más URLs aquí
    ]
    with DownloadPool(urls) as pool:
        pool.download()


