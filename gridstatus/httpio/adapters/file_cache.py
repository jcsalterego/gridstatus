import json
import os
import re
import sys
import urllib.parse
import zipfile

import pandas as pd
import requests.models

from gridstatus.httpio.adapters.base import BaseAdapter


class FileCacheAdapter(BaseAdapter):
    """This cache adapter will pickle values in tmp_dir, using method,
    args and kwargs to generate a SHA256 hash for the filename. There
    is no time-to-live support currently.
    """

    def __init__(self, tmp_dir="/tmp"):
        super().__init__("file_cache")
        self.tmp_dir = tmp_dir

    ALLOWED_METHODS = (
        "get",
        "post",
        "read_csv",
        "read_excel",
        "read_html",
        "session.get",
        "session.post",
    )

    def after_hook(self, method, args, kwargs, value, is_new_value=False):
        if method in self.ALLOWED_METHODS:
            if is_new_value:
                save_value = None
                if isinstance(value, requests.models.Response):
                    save_value = value.content
                    suffix = "data"
                    pass
                elif isinstance(value, pd.DataFrame):
                    save_value = value.to_csv()
                    suffix = "csv"
                    pass
                elif isinstance(value, str):
                    save_value = value
                    suffix = "data"
                    pass

                if save_value:
                    if isinstance(save_value, str):
                        save_value = save_value.encode("utf-8")
                    path = self._get_full_path(method, args, kwargs, suffix)
                    self._ensure_dir(path)
                    with open(path, "wb") as f:
                        f.write(save_value)
                    metadata_path = self._get_full_path(
                        method,
                        args,
                        kwargs,
                        "metadata.json",
                    )
                    self._ensure_dir(metadata_path)
                    with open(metadata_path, "w") as f:
                        f.write(
                            json.dumps(
                                {
                                    "methods": method,
                                    "args": self._transform_safe_args(args),
                                    "kwargs": kwargs,
                                },
                            ),
                        )
                else:
                    print(
                        f"ERROR: No content in value: {value} of class {type(value)}",
                        file=sys.stderr,
                    )
        else:
            print(
                f"WARN: PickleCacheAdapter after_hook: method {method} not allowed",
                file=sys.stderr,
            )

    def _get_full_path(self, method, args, kwargs, suffix):
        return (
            self.tmp_dir + "/" + self._get_path(method, args, kwargs, suffix)
        ).replace("//", "/")

    @staticmethod
    def _get_path(method, args, kwargs, suffix):
        url = None
        if len(args) > 0:
            if isinstance(args[0], str):
                url = args[0]
            elif isinstance(args[0], zipfile.ZipExtFile):
                url = args[0].name
        elif "url" in kwargs:
            url = kwargs["url"]

        if url is None:
            raise ValueError("No URL found in args or kwargs")
        urlinfo = urllib.parse.urlparse(url)
        url_host = urlinfo.hostname
        url_path = urlinfo.path
        url_query = urlinfo.query
        if url_query:
            url_path += f"?{url_query}"
        if kwargs:
            url_path += f"?{json.dumps(kwargs)}"
        filename = re.sub(r"[/\.&=\? \{\}\":]", "_", url_path).lower()
        path = f"/{url_host}/{filename}.{suffix}"
        return path

    @staticmethod
    def _ensure_dir(path):
        dirname = os.path.dirname(path)
        if not os.path.exists(dirname):
            os.makedirs(dirname)

    @staticmethod
    def _transform_safe_args(args):
        rv = []
        for arg in args:
            if isinstance(arg, zipfile.ZipExtFile):
                rv.append({"zip": {"name": arg.name}})
            else:
                rv.append(arg)
        return rv
