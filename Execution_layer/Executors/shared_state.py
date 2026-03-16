from __future__ import annotations

import threading
from datetime import datetime
from typing import Optional

from Execution_layer.Executors.models import OpenPairRecord, PairLiveMetrics


class SharedExecutorState:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._open_pairs: dict[str, OpenPairRecord] = {}
        self._pair_metrics: dict[str, PairLiveMetrics] = {}
        self._ws_critical: bool = False
        self._ws_critical_since: Optional[datetime] = None
        self._ws_comment: Optional[str] = None

    def register_open_pair(self, record: OpenPairRecord) -> None:
        with self._lock:
            self._open_pairs[record.uuid] = record

    def remove_open_pair(self, uuid: str) -> None:
        with self._lock:
            self._open_pairs.pop(uuid, None)

    def get_open_pair(self, uuid: str) -> Optional[OpenPairRecord]:
        with self._lock:
            return self._open_pairs.get(uuid)

    def get_all_open_pairs(self) -> list[OpenPairRecord]:
        with self._lock:
            return list(self._open_pairs.values())

    def get_open_pairs_for_bot(self, bot_id: str) -> list[OpenPairRecord]:
        with self._lock:
            return [record for record in self._open_pairs.values() if record.bot_id == bot_id]

    def update_pair_metrics(self, metrics: PairLiveMetrics) -> None:
        with self._lock:
            self._pair_metrics[metrics.uuid] = metrics

    def get_pair_metrics(self, uuid: str) -> Optional[PairLiveMetrics]:
        with self._lock:
            return self._pair_metrics.get(uuid)

    def remove_pair_metrics(self, uuid: str) -> None:
        with self._lock:
            self._pair_metrics.pop(uuid, None)

    def set_ws_critical(self, is_critical: bool, comment: str | None = None) -> None:
        with self._lock:
            if is_critical:
                if not self._ws_critical:
                    self._ws_critical_since = datetime.now()
                self._ws_critical = True
                self._ws_comment = comment
            else:
                self._ws_critical = False
                self._ws_critical_since = None
                self._ws_comment = None

    def is_ws_critical(self) -> bool:
        with self._lock:
            return self._ws_critical

    def get_ws_critical_state(self) -> dict[str, object]:
        with self._lock:
            return {
                "is_critical": self._ws_critical,
                "since": self._ws_critical_since,
                "comment": self._ws_comment,
            }