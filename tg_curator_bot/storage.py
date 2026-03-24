import asyncio
import json
import os
from copy import deepcopy
from pathlib import Path
from typing import Any, Dict


DEFAULT_STATE: Dict[str, Any] = {
    "owner_id": None,
    "bot_token": None,
    "user_session": {
        "api_id": None,
        "api_hash": None,
        "session_string": None,
    },
    "groups": {},
    "owner_dm_message_ids": [],
}


class Storage:
    def __init__(self, path: str) -> None:
        self.path = Path(path)
        self.lock = asyncio.Lock()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if not self.path.exists():
            self._write_sync(deepcopy(DEFAULT_STATE))

    def _merge_defaults(self, data: Dict[str, Any]) -> Dict[str, Any]:
        merged = deepcopy(DEFAULT_STATE)
        merged.update(data or {})
        if "user_session" not in merged or not isinstance(merged["user_session"], dict):
            merged["user_session"] = deepcopy(DEFAULT_STATE["user_session"])
        else:
            us = deepcopy(DEFAULT_STATE["user_session"])
            us.update(merged["user_session"])
            merged["user_session"] = us
        if "groups" not in merged or not isinstance(merged["groups"], dict):
            merged["groups"] = {}
        if "owner_dm_message_ids" not in merged or not isinstance(merged["owner_dm_message_ids"], list):
            merged["owner_dm_message_ids"] = []
        return merged

    def _read_sync(self) -> Dict[str, Any]:
        try:
            with self.path.open("r", encoding="utf-8") as f:
                data = json.load(f)
                if not isinstance(data, dict):
                    return deepcopy(DEFAULT_STATE)
                return self._merge_defaults(data)
        except (json.JSONDecodeError, OSError):
            return deepcopy(DEFAULT_STATE)

    def _write_sync(self, data: Dict[str, Any]) -> None:
        tmp_path = self.path.with_suffix(self.path.suffix + ".tmp")
        with tmp_path.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=True, indent=2)
        os.replace(tmp_path, self.path)

    async def read(self) -> Dict[str, Any]:
        async with self.lock:
            return self._read_sync()

    async def write(self, data: Dict[str, Any]) -> None:
        async with self.lock:
            self._write_sync(data)

    async def update(self, updater):
        async with self.lock:
            data = self._read_sync()
            new_data = updater(data)
            if new_data is None:
                new_data = data
            self._write_sync(new_data)
            return new_data


class ForwardLogStorage:
    def __init__(self, path: str) -> None:
        self.path = Path(path)
        self.lock = asyncio.Lock()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if not self.path.exists():
            self._write_sync({})

    def _read_sync(self) -> Dict[str, Any]:
        try:
            with self.path.open("r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, dict):
                    return data
        except (json.JSONDecodeError, OSError):
            pass
        return {}

    def _write_sync(self, data: Dict[str, Any]) -> None:
        tmp_path = self.path.with_suffix(self.path.suffix + ".tmp")
        with tmp_path.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=True, indent=2)
        os.replace(tmp_path, self.path)

    async def read(self) -> Dict[str, Any]:
        async with self.lock:
            return self._read_sync()

    async def write(self, data: Dict[str, Any]) -> None:
        async with self.lock:
            self._write_sync(data)

    async def update(self, updater):
        async with self.lock:
            data = self._read_sync()
            new_data = updater(data)
            if new_data is None:
                new_data = data
            self._write_sync(new_data)
            return new_data
