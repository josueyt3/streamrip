import asyncio
import logging
import os
import subprocess
from tempfile import gettempdir
from typing import Iterable, List, Optional

logger = logging.getLogger("streamrip")

class Aria2DownloadPool:
    """Manages asynchronous downloads using aria2c."""

    def __init__(
        self,
        urls: Iterable,
        tempdir: str = None,
        max_connections: int = 16,  # Máximo de conexiones por descarga
        max_cpu: int = 4,  # Número de CPUs a utilizar
        max_ram: int = 12 * 1024,  # RAM máxima en MB (12 GB)
        download_speed_limit: Optional[str] = None,  # Limitar velocidad de descarga (ej. '1M' para 1MB/s)
        split_size: int = 16,  # Número de fragmentos por archivo
    ):
        self.urls = list(urls)
        self.tempdir = tempdir or gettempdir()
        self.max_connections = max_connections
        self.max_cpu = max_cpu
        self.max_ram = max_ram
        self.split_size = split_size
        self.download_speed_limit = download_speed_limit

    async def _download_url(self, url):
        filename = os.path.join(self.tempdir, f"{abs(hash(url))}.part")
        logger.debug(f"Downloading {url} to {filename}")

        # Construir el comando aria2c para descargar el archivo
        command = [
            "aria2c",
            "--dir", self.tempdir,  # Directorio de descarga
            "--out", os.path.basename(filename),  # Nombre del archivo
            "--max-connection-per-server", str(self.max_connections),  # Conexiones por servidor
            "--split", str(self.split_size),  # Fragmentación del archivo
            "--enable-rpc=false",  # Deshabilitar RPC para mejor rendimiento
            "--min-split-size", "1M",  # Mínimo tamaño de fragmento
            "--file-allocation=trunc",  # Preasignar espacio para el archivo
            "--max-overall-download-limit", f"{self.max_ram // 10}M",  # Límite de uso de RAM
        ]

        if self.download_speed_limit:
            command.extend(["--max-download-limit", self.download_speed_limit])

        command.append(url)  # URL del archivo a descargar

        # Ejecutar el comando aria2c
        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if result.returncode != 0:
            logger.error(f"Failed to download {url}: {result.stderr.decode()}")

        logger.debug(f"Finished {url}")

    async def _download_urls(self):
        tasks = [self._download_url(url) for url in self.urls]
        await asyncio.gather(*tasks)

    def download(self):
        asyncio.run(self._download_urls())

    @property
    def files(self):
        return [os.path.join(self.tempdir, f"{abs(hash(url))}.part") for url in self.urls]

    def __len__(self):
        return len(self.urls)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        for file in self.files:
            try:
                os.remove(file)
            except FileNotFoundError:
                pass

# Ejemplo de uso
urls = [
    "https://example.com/file1.zip",
    "https://example.com/file2.zip",
    # Añade más URLs aquí
]

with Aria2DownloadPool(urls=urls, max_connections=32, max_cpu=4, max_ram=12*1024) as downloader:
    downloader.download()

# Obtener los archivos descargados
files = downloader.files
print(files)
