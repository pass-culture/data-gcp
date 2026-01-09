import asyncio
import functools
import time
from abc import ABC, abstractmethod
from typing import Callable, Dict, Optional, Tuple


class BaseAuthManager(ABC):
    def __init__(self, refresh_buffer: Optional[int] = 300):
        self._token = None
        self._expires_at = 0
        self.refresh_buffer = refresh_buffer

    @abstractmethod
    def _fetch_token_data(self) -> Tuple[str, int]: ...

    async def _afetch_token_data(self) -> Tuple[str, int]:
        return self._fetch_token_data()

    def get_token(self, force: bool = False) -> str:
        if (
            force
            or not self._token
            or (
                self.refresh_buffer
                and time.time() > self._expires_at - self.refresh_buffer
            )
        ):
            token, expires_in = self._fetch_token_data()
            self._token, self._expires_at = token, time.time() + expires_in
        return self._token

    async def get_atoken(self, force: bool = False) -> str:
        if (
            force
            or not self._token
            or (
                self.refresh_buffer
                and time.time() > self._expires_at - self.refresh_buffer
            )
        ):
            token, expires_in = await self._afetch_token_data()
            self._token, self._expires_at = token, time.time() + expires_in
        return self._token

    @abstractmethod
    def get_headers(self, token: str) -> Dict[str, str]: ...


def retry_on_401(manager: BaseAuthManager):
    def decorator(func: Callable):
        if asyncio.iscoroutinefunction(func):

            @functools.wraps(func)
            async def wrapper(*args, **kwargs):
                res = await func(*args, **kwargs)
                if res is not None and getattr(res, "status_code", None) == 401:
                    token = await manager.get_atoken(force=True)
                    kwargs["headers"] = {
                        **kwargs.get("headers", {}),
                        **manager.get_headers(token),
                    }
                    return await func(*args, **kwargs)
                return res
        else:

            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                res = func(*args, **kwargs)
                if res is not None and getattr(res, "status_code", None) == 401:
                    token = manager.get_token(force=True)
                    kwargs["headers"] = {
                        **kwargs.get("headers", {}),
                        **manager.get_headers(token),
                    }
                    return func(*args, **kwargs)
                return res

        return wrapper

    return decorator
