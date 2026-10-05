from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any
from urllib.parse import quote

import aiohttp

from airflow.hooks.base import BaseHook
from airflow.sdk import Variable
from airflow.triggers.base import BaseEventTrigger, TriggerEvent


class HDFSFileTrigger(BaseEventTrigger):

    def __init__(
        self,
        *,
        webhdfs_conn_id: str,
        path_var_name: str = "test_path",
        poll_interval: int = 60,
        request_timeout: int = 30,
        max_consecutive_failures: int = 10,
    ) -> None:
        super().__init__()

        self.webhdfs_conn_id = webhdfs_conn_id
        self.path_var_name = path_var_name
        self.poll_interval = poll_interval
        self.request_timeout = request_timeout
        self.max_consecutive_failures = max_consecutive_failures

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "triggers.eod_trigger.HDFSFileTrigger",
            {
                "webhdfs_conn_id": self.webhdfs_conn_id,
                "path_var_name": self.path_var_name,
                "poll_interval": self.poll_interval,
                "request_timeout": self.request_timeout,
                "max_consecutive_failures": self.max_consecutive_failures,
            },
        )

    def _get_path(self) -> str:
        path = Variable.get(
            self.path_var_name,
            default=None,
        )

        if path is None:
            raise RuntimeError(
                f"Airflow Variable '{self.path_var_name}' does not exist"
            )

        path = str(path).strip()

        if not path:
            raise RuntimeError(
                f"Airflow Variable '{self.path_var_name}' is empty"
            )

        return "/" + path.lstrip("/")

    def _get_webhdfs_config(
        self,
    ) -> tuple[str, aiohttp.BasicAuth | None]:

        conn = BaseHook.get_connection(self.webhdfs_conn_id)

        scheme = conn.schema or "http"
        host = conn.host
        port = conn.port or 9870

        if not host:
            raise RuntimeError(
                f"Connection '{self.webhdfs_conn_id}' has no host"
            )

        base_url = f"{scheme}://{host}:{port}"

        auth = None

        if conn.login:
            auth = aiohttp.BasicAuth(
                login=conn.login,
                password=conn.password or "",
            )

        return base_url, auth

    async def _rename(
        self,
        *,
        session: aiohttp.ClientSession,
        base_url: str,
        source_path: str,
        destination_path: str,
    ) -> bool:

        source = quote(source_path, safe="/")
        destination = quote(destination_path, safe="/")

        url = (
            f"{base_url}/webhdfs/v1{source}"
            f"?op=RENAME"
            f"&destination={destination}"
        )

        async with session.put(url) as response:

            if response.status != 200:
                body = await response.text()

                raise RuntimeError(
                    f"WebHDFS RENAME failed: "
                    f"{response.status} - {body[:500]}"
                )

            data = await response.json()

            return bool(data.get("boolean", False))

    async def _exists(
        self,
        *,
        session: aiohttp.ClientSession,
        base_url: str,
        path: str,
    ) -> bool:

        encoded_path = quote(path, safe="/")

        url = (
            f"{base_url}/webhdfs/v1{encoded_path}"
            f"?op=GETFILESTATUS"
        )

        async with session.get(url) as response:

            if response.status == 200:
                return True

            if response.status == 404:
                return False

            body = await response.text()

            raise RuntimeError(
                f"WebHDFS GETFILESTATUS failed: "
                f"{response.status} - {body[:500]}"
            )

    async def _claim_file(
        self,
        *,
        session: aiohttp.ClientSession,
        base_url: str,
        success_path: str,
    ) -> str | None:

        """
        Atomically move:

            _SUCCESS -> _CLAIMED

        Returns the claimed path if successful.

        Returns None if another watcher has already claimed it.
        """

        claimed_path = success_path.rsplit("/", 1)[0] + "/_CLAIMED"

        # Check that SUCCESS exists.
        if not await self._exists(
            session=session,
            base_url=base_url,
            path=success_path,
        ):
            return None

        try:
            renamed = await self._rename(
                session=session,
                base_url=base_url,
                source_path=success_path,
                destination_path=claimed_path,
            )

            if renamed:
                self.log.info(
                    "Successfully claimed HDFS event: %s -> %s",
                    success_path,
                    claimed_path,
                )

                return claimed_path

        except Exception:
            # Another watcher may have claimed it between the
            # existence check and rename.
            self.log.info(
                "Could not claim %s. "
                "It may already have been claimed.",
                success_path,
            )

        return None

    async def run(self) -> AsyncIterator[TriggerEvent]:

        base_url, auth = self._get_webhdfs_config()

        # Read Variable when this trigger instance starts.
        success_path = self._get_path()

        self.log.info(
            "Watching HDFS path: %s",
            success_path,
        )

        timeout = aiohttp.ClientTimeout(
            total=self.request_timeout
        )

        connector = aiohttp.TCPConnector(
            limit=10,
            ttl_dns_cache=300,
        )

        consecutive_failures = 0

        async with aiohttp.ClientSession(
            auth=auth,
            timeout=timeout,
            connector=connector,
        ) as session:

            while True:

                try:

                    claimed_path = await self._claim_file(
                        session=session,
                        base_url=base_url,
                        success_path=success_path,
                    )

                    consecutive_failures = 0

                    if claimed_path:

                        self.log.info(
                            "HDFS event claimed. "
                            "Triggering DAG."
                        )

                        yield TriggerEvent(
                            {
                                "status": "FILE_CLAIMED",
                                "hdfs_path": claimed_path,
                                "original_path": success_path,
                            }
                        )

                        # VERY IMPORTANT:
                        # Do not keep polling after firing.
                        return

                except asyncio.CancelledError:
                    raise

                except Exception:

                    consecutive_failures += 1

                    self.log.exception(
                        "Error checking/claiming %s "
                        "(failure %s/%s)",
                        success_path,
                        consecutive_failures,
                        self.max_consecutive_failures,
                    )

                    if (
                        consecutive_failures
                        >= self.max_consecutive_failures
                    ):
                        raise

                await asyncio.sleep(
                    self.poll_interval
                )
