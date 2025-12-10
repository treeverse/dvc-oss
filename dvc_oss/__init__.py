import logging
import os
import threading
from typing import ClassVar

from funcy import wrap_prop

from dvc.utils.objects import cached_property
from dvc_objects.fs.base import ObjectFileSystem

logger = logging.getLogger(__name__)


# pylint:disable=abstract-method
class OSSFileSystem(ObjectFileSystem):
    protocol = "oss"
    REQUIRES: ClassVar[dict[str, str]] = {"ossfs": "ossfs"}
    PARAM_CHECKSUM = "etag"
    LIST_OBJECT_PAGE_SIZE = 100

    def _prepare_credentials(self, **config):
        login_info = {}
        login_info["key"] = config.get("oss_key_id") or os.getenv("OSS_ACCESS_KEY_ID")
        login_info["secret"] = config.get("oss_key_secret") or os.getenv(
            "OSS_ACCESS_KEY_SECRET"
        )
        login_info["endpoint"] = config.get("oss_endpoint")
        return login_info

    @wrap_prop(threading.Lock())
    @cached_property
    def fs(self):
        from aiohttp import ClientTimeout
        from ossfs import AioOSSFileSystem as _OSSFileSystem

        fs_args = dict(self.fs_args)

        try:
            config_dict = self.config if hasattr(self, "config") and self.config else {}
        except AttributeError:
            config_dict = {}

        connect_timeout = float(
            config_dict.get("oss_connect_timeout")
            or os.getenv("OSS_CONNECT_TIMEOUT", "60")
        )
        read_timeout = float(
            config_dict.get("oss_read_timeout") or os.getenv("OSS_READ_TIMEOUT", "300")
        )
        total_timeout = float(
            config_dict.get("oss_total_timeout") or os.getenv("OSS_TOTAL_TIMEOUT", "0")
        )

        client_kwargs = fs_args.get("client_kwargs", {})
        if "timeout" not in client_kwargs:
            timeout_kwargs = {
                "connect": connect_timeout,
                "sock_read": read_timeout,
            }
            if total_timeout > 0:
                timeout_kwargs["total"] = total_timeout
            client_kwargs["timeout"] = ClientTimeout(**timeout_kwargs)
            fs_args["client_kwargs"] = client_kwargs

        return _OSSFileSystem(**fs_args)

    @classmethod
    def _strip_protocol(cls, path: str) -> str:
        from fsspec.utils import infer_storage_options

        options = infer_storage_options(path)
        return options["host"] + options["path"]

    def unstrip_protocol(self, path):
        return "oss://" + path.lstrip("/")
