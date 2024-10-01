import asyncio
import base64
import functools
import hashlib
import itertools
import json
import logging
import os
import re
import shutil
import tempfile
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Callable, Optional

import aiofiles
import aiohttp
import m3u8
from Cryptodome.Cipher import AES, Blowfish
from Cryptodome.Util import Counter

from .. import converter
from ..exceptions import NonStreamableError

logger = logging.getLogger("streamrip")

BLOWFISH_SECRET = "g4el58wc0zvf9na1"

def generate_temp_path(url: str):
    return os.path.join(
        tempfile.gettempdir(),
        f"__streamrip_{hash(url)}_{time.time()}.download",
    )

async def fast_async_download(path, url, headers, callback):
    """Fast download with larger chunks."""
    chunk_size: int = 2**20  # 1 MB
    counter = 0
    yield_every = 8  # 8 MB
    async with aiofiles.open(path, "wb") as file:
        async with aiohttp.ClientSession().get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=60)) as resp:
            resp.raise_for_status()
            async for chunk in resp.content.iter_any(chunk_size):
                await file.write(chunk)
                callback(len(chunk))
                if counter % yield_every == 0:
                    await asyncio.sleep(0)
                counter += 1

@dataclass(slots=True)
class Downloadable(ABC):
    session: aiohttp.ClientSession
    url: str
    extension: str
    source: str = "Unknown"
    _size_base: Optional[int] = None

    async def download(self, path: str, callback: Callable[[int], Any]):
        await self._download(path, callback)

    async def size(self) -> int:
        if hasattr(self, "_size") and self._size is not None:
            return self._size

        async with self.session.head(self.url) as response:
            response.raise_for_status()
            content_length = response.headers.get("Content-Length", 0)
            self._size = int(content_length)
            return self._size

    @property
    def _size(self):
        return self._size_base

    @_size.setter
    def _size(self, v):
        self._size_base = v

    @abstractmethod
    async def _download(self, path: str, callback: Callable[[int], None]):
        raise NotImplementedError

class BasicDownloadable(Downloadable):
    """Just downloads a URL."""

    def __init__(self, session: aiohttp.ClientSession, url: str, extension: str, source: str | None = None):
        self.session = session
        self.url = url
        self.extension = extension
        self._size = None
        self.source: str = source or "Unknown"

    async def _download(self, path: str, callback):
        await fast_async_download(path, self.url, self.session.headers, callback)

class DeezerDownloadable(Downloadable):
    is_encrypted = re.compile("/m(?:obile|edia)/")

    def __init__(self, session: aiohttp.ClientSession, info: dict):
        logger.debug("Deezer info for downloadable: %s", info)
        self.session = session
        self.url = info["url"]
        self.source: str = "deezer"
        qualities_available = [i for i, size in enumerate(info["quality_to_size"]) if size > 0]
        if len(qualities_available) == 0:
            raise NonStreamableError("Missing download info. Skipping.")
        max_quality_available = max(qualities_available)
        self.quality = min(info["quality"], max_quality_available)
        self._size = info["quality_to_size"][self.quality]
        self.extension = "mp3" if self.quality <= 1 else "flac"
        self.id = str(info["id"])

    async def _download(self, path: str, callback):
        async with self.session.get(self.url, allow_redirects=True) as resp:
            resp.raise_for_status()
            self._size = int(resp.headers.get("Content-Length", 0))
            if self._size < 20000 and not self.url.endswith(".jpg"):
                try:
                    info = await resp.json()
                    raise NonStreamableError(f"{info['error']} - {info['message']}")
                except json.JSONDecodeError:
                    raise NonStreamableError("File not found.")

            if self.is_encrypted.search(self.url) is None:
                logger.debug(f"Deezer file at {self.url} not encrypted.")
                await fast_async_download(path, self.url, self.session.headers, callback)
            else:
                blowfish_key = self._generate_blowfish_key(self.id)
                logger.debug("Deezer file (id %s) at %s is encrypted. Decrypting with %s", self.id, self.url, blowfish_key)

                buf = bytearray()
                async for data in resp.content.iter_any():
                    buf += data
                    callback(len(data))

                encrypt_chunk_size = 3 * 2048
                async with aiofiles.open(path, "wb") as audio:
                    buflen = len(buf)
                    for i in range(0, buflen, encrypt_chunk_size):
                        data = buf[i: min(i + encrypt_chunk_size, buflen)]
                        decrypted_chunk = (
                            self._decrypt_chunk(blowfish_key, data[:2048]) + data[2048:]
                        ) if len(data) >= 2048 else data
                        await audio.write(decrypted_chunk)

    @staticmethod
    def _decrypt_chunk(key, data):
        return Blowfish.new(key, Blowfish.MODE_CBC, b"\x00\x01\x02\x03\x04\x05\x06\x07").decrypt(data)

    @staticmethod
    def _generate_blowfish_key(track_id: str) -> bytes:
        md5_hash = hashlib.md5(track_id.encode()).hexdigest()
        return "".join(
            chr(functools.reduce(lambda x, y: x ^ y, map(ord, t)))
            for t in zip(md5_hash[:16], md5_hash[16:], BLOWFISH_SECRET)
        ).encode()

class SoundcloudDownloadable(Downloadable):
    def __init__(self, session, info: dict):
        self.session = session
        self.file_type = info["type"]
        self.source = "soundcloud"
        self.extension = "mp3" if self.file_type == "mp3" else "flac"
        self.url = info["url"]

    async def _download(self, path: str, callback):
        if self.file_type == "mp3":
            await self._download_mp3(path, callback)
        else:
            await self._download_original(path, callback)

    async def _download_original(self, path: str, callback):
        downloader = BasicDownloadable(self.session, self.url, "flac", source="soundcloud")
        await downloader.download(path, callback)

    async def _download_mp3(self, path: str, callback):
        async with self.session.get(self.url) as resp:
            content = await resp.text("utf-8")
        parsed_m3u = m3u8.loads(content)
        self._size = len(parsed_m3u.segments)
        tasks = [asyncio.create_task(self._download_segment(segment.uri)) for segment in parsed_m3u.segments]
        segment_paths = await asyncio.gather(*tasks)
        await concat_audio_files(segment_paths, path, "mp3")

async def concat_audio_files(paths: list[str], out: str, ext: str, max_files_open=128):
    if shutil.which("ffmpeg") is None:
        raise Exception("FFmpeg must be installed.")
    if len(paths) == 1:
        shutil.move(paths[0], out)
        return
    it = iter(paths)
    num_batches = len(paths) // max_files_open + (1 if len(paths) % max_files_open != 0 else 0)
    tempdir = tempfile.gettempdir()
    outpaths = [os.path.join(tempdir, f"__streamrip_ffmpeg_{hash(paths[i*max_files_open])}.{ext}") for i in range(num_batches)]
    for p, outpath in zip(itertools.zip_longest(*[it] * max_files_open), outpaths):
        cmd = ["ffmpeg", "-y"]
        for segment in p:
            if segment:
                cmd += ["-i", segment]
        cmd += ["-filter_complex", f"concat=n={len(p)}:v=0:a=1", outpath]
        subprocess.run(cmd)
    cmd = ["ffmpeg", "-y"]
    for outpath in outpaths:
        cmd += ["-i", outpath]
    cmd += ["-filter_complex", f"concat=n={len(outpaths)}:v=0:a=1", out]
    subprocess.run(cmd)
    for segment in paths:
        os.remove(segment)

async def download_playlist(session: aiohttp.ClientSession, playlist_info: dict):
    downloadables = []
    for track_info in playlist_info["tracks"]:
        downloadable = DeezerDownloadable(session, track_info)
        downloadables.append(downloadable)

    async with aiofiles.open("output_playlist.txt", "w") as f:
        for downloadable in downloadables:
            await downloadable.download(f, lambda x: logger.info(f"Downloaded {x} bytes"))

async def main():
    async with aiohttp.ClientSession() as session:
        playlist_info = {
            "tracks": [
                {"url": "http://example.com/track1", "id": 1, "quality": 0, "quality_to_size": [1000, 2000]},
                {"url": "http://example.com/track2", "id": 2, "quality": 0, "quality_to_size": [1500, 2500]},
            ]
        }
        await download_playlist(session, playlist_info)

if __name__ == "__main__":
    asyncio.run(main())
