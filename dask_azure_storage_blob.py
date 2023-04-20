"""
dask-azure-blob-storage
"""
import datetime
import random
import dask
import dask.bag
from typing import Optional, Union, Dict, TypedDict

import azure.storage.blob
from azure.core.credentials import (
    AzureNamedKeyCredential,
    AzureSasCredential,
    TokenCredential,
)
import pandas as pd

__version__ = "0.1.0"

CredentialT = Optional[
    Union[
        str,
        Dict[str, str],
        "AzureNamedKeyCredential",
        "AzureSasCredential",
        "TokenCredential",
    ]
]


class StorageOptions(TypedDict):
    account_url: str
    container_name: str
    credential: CredentialT


@dask.delayed
def list_prefixes(
    prefix: str, depth: int, storage_options: StorageOptions
) -> list[str]:
    """
    List the prefixes at a given depth under a prefix.

    This can be useful for listing blobs (or "folders") quickly. This will
    call itself recursively, using Dask's support for nested parallelism.

    Combine with get_blob_properties to ...

    Parameters
    ----------
    prefix: str
        The prefix to start under.
    depth: int
        The number of "folders" to recurse into. The results here will
        have at most ``depth`` slashes.
    storage_options: dict
        Keyword options for :class:`azure.storage.blob.ContainerClient`.

    Returns
    -------
    prefixes
        A delayed object that, when computed, returns a list of string
        prefixes.
    """
    prefix = prefix.rstrip("/") + "/"
    d = prefix.count("/")
    cc = azure.storage.blob.ContainerClient(**storage_options)
    blob_names = []
    with cc:
        if d < depth:
            prefixes = [x.name for x in cc.walk_blobs(prefix)]
            if d == depth - 1:
                # no need to recurse here, just to return it.
                blob_names.extend(prefixes)
            else:
                xs = [list_prefixes(x, depth, storage_options) for x in prefixes]
                xs = dask.compute(*xs)
                for x in xs:
                    blob_names.extend(x)
        elif d == depth:
            return [prefix]
    return blob_names


@dask.delayed
def read_under_prefix(x: str, storage_options, sample=1):
    """
    Read all the blobs under a prefix.
    """
    cc = azure.storage.blob.ContainerClient(**storage_options)
    assert 0 <= sample <= 1

    items = []
    blobs = list(cc.list_blobs(x))
    blobs = list(random.sample(blobs, int(len(blobs) * sample)))

    with cc:
        for blob in blobs:
            content = cc.get_blob_client(blob).download_blob().readall()
            items.append(content)

    return items


def list_properties(
    prefix: str, storage_options: StorageOptions
) -> list[tuple[str, datetime.datetime, datetime.datetime, int]]:
    """
    List the properties of all blobs under a prefix.
    """
    with azure.storage.blob.ContainerClient(**storage_options) as cc:
        blobs_properties = cc.list_blobs(prefix)
        records = [
            (bp.name, bp.last_modified, bp.creation_time, bp.size)
            for bp in blobs_properties
        ]
    return records
