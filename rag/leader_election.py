import asyncio
import logging
import time
from decimal import Decimal

import boto3
from botocore.exceptions import ClientError  # type: ignore[import]

logger = logging.getLogger(__name__)

_LEASE_PK = "current"
_CONDITIONAL_CHECK_FAILED = "ConditionalCheckFailedException"


class LeaderElection:
    def __init__(
        self,
        worker_id: str,
        lease_duration_seconds: int = 30,
        *,
        region_name: str = "us-east-1",
    ):
        self._worker_id = worker_id
        self._lease_duration = lease_duration_seconds
        self._renew_interval = lease_duration_seconds / 3
        self._acquire_interval = lease_duration_seconds / 2
        dynamodb = boto3.resource("dynamodb", region_name=region_name)
        self._table = dynamodb.Table("leader_info")
        self._is_leader: bool = False
        self._current_leader_id: str | None = None
        self._known_epoch: int | None = None
        self._task: asyncio.Task | None = None

    # ------------------------------------------------------------------ #
    # Public API                                                           #
    # ------------------------------------------------------------------ #

    async def start(self) -> None:
        self._task = asyncio.create_task(
            self._election_loop(), name=f"leader-election-{self._worker_id}"
        )

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        self._is_leader = False

    def is_leader(self) -> bool:
        return self._is_leader

    def get_leader_id(self) -> str | None:
        return self._current_leader_id

    async def wait_for_leader(self, timeout: float = 30.0) -> str | None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self._current_leader_id is not None:
                return self._current_leader_id
            await asyncio.sleep(1.0)
        return self._current_leader_id

    # ------------------------------------------------------------------ #
    # Background loop                                                      #
    # ------------------------------------------------------------------ #

    async def _election_loop(self) -> None:
        await self._attempt_acquire()
        while True:
            if self._is_leader:
                await asyncio.sleep(self._renew_interval)
                await self._attempt_renew()
            else:
                await asyncio.sleep(self._acquire_interval)
                await self._attempt_acquire()

    async def _attempt_acquire(self) -> None:
        try:
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(None, self._try_acquire)
            if result is not None:
                self._is_leader = True
                self._known_epoch = int(result["lease_epoch"])
                self._current_leader_id = self._worker_id
                msg = (
                    f"[leader-election {self._worker_id}] acquired leadership "
                    f"epoch={self._known_epoch}"
                )
                logger.info(msg)
                print(msg, flush=True)
            else:
                loop = asyncio.get_event_loop()
                leader_id = await loop.run_in_executor(None, self._read_leader)
                self._current_leader_id = leader_id
        except Exception as exc:
            msg = f"[leader-election {self._worker_id}] WARN: acquisition error: {exc}"
            logger.warning(msg)
            print(msg, flush=True)

    async def _attempt_renew(self) -> None:
        try:
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(None, self._try_renew)
            if result is not None:
                self._known_epoch = int(result["lease_epoch"])
                logger.debug(
                    "[leader-election %s] renewed lease epoch=%d",
                    self._worker_id,
                    self._known_epoch,
                )
            else:
                # ConditionalCheckFailed — another node took the lease
                self._is_leader = False
                self._known_epoch = None
                msg = (
                    f"[leader-election {self._worker_id}] WARN: lost leadership "
                    f"(ConditionalCheckFailed on renewal)"
                )
                logger.warning(msg)
                print(msg, flush=True)
        except Exception as exc:
            # Transient DynamoDB error — stay leader optimistically; lease is still live
            msg = (
                f"[leader-election {self._worker_id}] WARN: renewal network error "
                f"(staying leader): {exc}"
            )
            logger.warning(msg)
            print(msg, flush=True)

    # ------------------------------------------------------------------ #
    # Synchronous DynamoDB helpers (run via run_in_executor)               #
    # ------------------------------------------------------------------ #

    def _try_acquire(self) -> dict | None:
        now = int(time.time())
        try:
            response = self._table.update_item(
                Key={"leader": _LEASE_PK},
                UpdateExpression=(
                    "SET worker_id = :wid, lease_expires_at = :exp "
                    "ADD lease_epoch :one"
                ),
                ConditionExpression=(
                    "attribute_not_exists(#l) OR lease_expires_at < :now"
                ),
                ExpressionAttributeNames={"#l": "leader"},
                ExpressionAttributeValues={
                    ":wid": self._worker_id,
                    ":exp": Decimal(now + self._lease_duration),
                    ":now": Decimal(now),
                    ":one": Decimal(1),
                },
                ReturnValues="ALL_NEW",
            )
            return response["Attributes"]
        except ClientError as exc:
            if exc.response["Error"]["Code"] == _CONDITIONAL_CHECK_FAILED:
                return None
            raise

    def _try_renew(self) -> dict | None:
        assert self._known_epoch is not None
        now = int(time.time())
        try:
            response = self._table.update_item(
                Key={"leader": _LEASE_PK},
                UpdateExpression="SET lease_expires_at = :exp ADD lease_epoch :one",
                ConditionExpression="worker_id = :wid AND lease_epoch = :epoch",
                ExpressionAttributeValues={
                    ":wid": self._worker_id,
                    ":exp": Decimal(now + self._lease_duration),
                    ":epoch": Decimal(self._known_epoch),
                    ":one": Decimal(1),
                },
                ReturnValues="ALL_NEW",
            )
            return response["Attributes"]
        except ClientError as exc:
            if exc.response["Error"]["Code"] == _CONDITIONAL_CHECK_FAILED:
                return None
            raise

    def _read_leader(self) -> str | None:
        response = self._table.get_item(
            Key={"leader": _LEASE_PK},
            ConsistentRead=True,
        )
        item = response.get("Item")
        if item:
            return str(item.get("worker_id", ""))
        return None
