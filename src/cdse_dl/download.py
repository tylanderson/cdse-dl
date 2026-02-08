"""CDSE download functions."""

import hashlib
import logging
import re
import warnings
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Protocol, Self

from tqdm.auto import tqdm

from cdse_dl.auth import CDSEAuthSession, Credentials
from cdse_dl.odata.search import ProductSearch

try:
    import blake3

    BLAKE3_AVAILABLE = True
except ImportError:
    BLAKE3_AVAILABLE = False

logger = logging.getLogger(__name__)

BASE_URL = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products"


class Hasher(Protocol):  # noqa: D101
    @property
    def name(self) -> str: ...  # noqa: D102
    def update(self, data: bytes) -> Self | None: ...  # noqa: D102
    def digest(self) -> bytes: ...  # noqa: D102
    def hexdigest(self) -> str: ...  # noqa: D102


def _check_hash(
    file_path: Any, product_info: dict[str, Any], block_size: int = 1024 * 1024
) -> bool:
    """Compare a given MD5 checksum with one calculated from a file.

    Args:
        file_path (Any): file path to hash
        product_info (dict): CDSE product info
        block_size (int, optional): block size to read. Defaults to 1024*1024.

    Returns:
        bool: True if file matches hash in product info

    Raises:
        ValueError: error with checksum
    """
    checksums_info = {i["Algorithm"]: i for i in product_info["Checksum"]}
    checksums_available = checksums_info.keys()

    if len(checksums_available) == 0:
        raise ValueError("No checksum information found in product information.")

    if "BLAKE3" in checksums_info and BLAKE3_AVAILABLE:
        checksum = checksums_info["BLAKE3"]["Value"]
        hasher: Hasher = blake3.blake3()
    elif "MD5" in checksums_available:
        checksum = checksums_info["MD5"]["Value"]
        hasher = hashlib.md5()
    else:
        raise ValueError(
            f"No support for checksums available in product: {checksums_available}"
        )

    file_size = product_info["ContentLength"]

    with tqdm(
        desc=f"{hasher.name.upper()} checksumming",
        total=file_size,
        unit="B",
        unit_scale=True,
        leave=False,
    ) as progress:
        with open(file_path, "rb") as f:
            while True:
                block_data = f.read(block_size)
                if not block_data:
                    break
                hasher.update(block_data)
                progress.update(len(block_data))
        return bool(hasher.hexdigest().lower() == checksum.lower())


class Downloader:
    """Downloader for CDSE."""

    def __init__(
        self,
        credentials: Credentials | None = None,
        max_num_workers: int | None = 4,
    ) -> None:
        """Downloader for CDSE.

        Args:
            credentials (Credentials | None, optional): Credentials for authorizing session. Defaults to None.
            max_num_workers (int | None, optional): Max number of workers to download with. Defaults to 4.
        """
        self.session = CDSEAuthSession(credentials)
        self.dl_executor = ThreadPoolExecutor(
            max_workers=max_num_workers,
            thread_name_prefix="cdse-dl",
        )

    def download(
        self,
        product: dict[str, Any],
        path: Any,
        check: bool = True,
        quiet: bool = False,
    ) -> None:
        """Download product info to specified path.

        If `check` is `True`, with attempt to check the file for completeness
        using checksums are provided in the product info.

        Args:
            product (dict): product info
            path (Any): path to download file to
            check (bool, optional): check the downloaded file against checksums. Defaults to True.
            quiet (bool): disable progress bar. Defaults to False.

        Raises:
            Exception: checksums do not match
        """
        try:
            product_id = product["Id"]
            product_name = product["Name"]
        except KeyError:
            product_id = product["id"]
            product_name = product["properties"]["title"]

        path = Path(path) / product_name
        path.parent.mkdir(parents=True, exist_ok=True)
        download_url = f"{BASE_URL}({product_id})/$value"

        self._download_url(download_url, path, quiet=quiet)

        if check:
            if not product["Checksum"]:
                warnings.warn(f"{product_id}: Product has no checksums available")
            else:
                valid = _check_hash(path, product)
                if not valid:
                    raise Exception(
                        "Checksum of downloaded file does not match product info"
                    )

    def download_all(
        self,
        products: Iterable[dict[str, Any]],
        path: Any,
        check: bool = True,
        quiet: bool = False,
    ) -> None:
        """Download product info to specified path.

        If `check` is `True`, with attempt to check the file for completeness
        using checksums are provided in the product info.

        Downloads are limited to the max number of workers specified on Downloader creation

        Args:
            products (Iterable[dict]): multiple product infos to download
            path (Any): path to download file to
            check (bool, optional): check the downloaded file against checksums. Defaults to True.
            quiet (bool): disable progress bar. Defaults to False.
        """
        with self.dl_executor as pool:
            _ = [
                pool.submit(self.download, product, path, check, quiet)
                for product in products
            ]

    def _download_from_name_or_id(
        self,
        path: str,
        collection: str | None = None,
        name: str | None = None,
        product_id: str | None = None,
        check: bool = True,
        quiet: bool = False,
    ) -> None:
        """Download from name or id.

        Args:
            path (str): path to download to
            collection (str | None, optional): collection to download from. Defaults to None.
            name (str | None, optional): product name. Defaults to None.
            product_id (str | None, optional): product id. Defaults to None.
            check (bool, optional): check the downloaded file against checksums. Defaults to True.
            quiet (bool): disable progress bar. Defaults to False.

        Raises:
            ValueError: name ore product_id not provided
        """
        if name is None and product_id is None:
            raise ValueError("must provide one of `name` or `product_id`")
        search = ProductSearch(collection=collection, name=name, product_id=product_id)
        product = next(search.get(1))
        self.download(product, path, check, quiet)

    def download_from_id(
        self,
        collection: str,
        product_id: str,
        path: str,
        check: bool = True,
        quiet: bool = False,
    ) -> None:
        """Download from product id.

        Args:
            collection (str): collection to download from
            product_id (str): product product id
            path (str): path to download to
            check (bool, optional): check the downloaded file against checksums. Defaults to True.
            quiet (bool): disable progress bar. Defaults to False.
        """
        self._download_from_name_or_id(path, collection, None, product_id, check, quiet)

    def download_from_name(
        self,
        collection: str,
        name: str,
        path: str,
        check: bool = True,
        quiet: bool = False,
    ) -> None:
        """Download from product name.

        Args:
            collection (str): collection to download from
            name (str): product name
            path (str): path to download to
            check (bool, optional): check the downloaded file against checksums. Defaults to True.
            quiet (bool): disable progress bar. Defaults to False.
        """
        self._download_from_name_or_id(path, collection, name, None, check, quiet)

    def _download_url(
        self, url: str, path: Any, chunk_size: int = 2**13, quiet: bool = False
    ) -> None:
        """Download a CDSE url to path.

        Args:
            url (str): url to download
            path (Any): path to save file as
            chunk_size (int, optional): chunk size to download in. Defaults to 2**13.
            quiet (bool): disable progress bar. Defaults to False.
        """
        path = Path(path)
        if path.exists():
            mode = "ab"
            downloaded_bytes = path.stat().st_size
            headers = {"Range": f"bytes={downloaded_bytes}-"}

        else:
            mode = "wb"
            downloaded_bytes = 0
            headers = None

        response = self.session.get(url, stream=True, headers=headers)
        response.raise_for_status()
        length = int(response.headers["Content-Length"])
        name = re.findall("filename=(.+)", response.headers["content-disposition"])[0]

        # already downloaded
        if downloaded_bytes == length:
            logger.info(f"Already Downloaded: {name}")
            return

        # downloaded but larger than expected size
        elif downloaded_bytes > length:
            path.unlink()
            mode = "wb"
            downloaded_bytes = 0
            headers = None

        progress_bar = tqdm(
            desc=f"Downloading {name}",
            total=length,
            unit="B",
            unit_scale=True,
            initial=downloaded_bytes,
            disable=quiet,
        )

        with open(path, mode) as file:
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:
                    file.write(chunk)
                    progress_bar.update(len(chunk))
        progress_bar.close()
