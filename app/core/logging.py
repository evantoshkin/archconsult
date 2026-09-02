import logging
import sys
import time
import uuid
from contextvars import ContextVar

from fastapi import Request, Response

from app.core.config import settings

request_id_var: ContextVar[str] = ContextVar("request_id", default="")
access_logger = logging.getLogger("app.access")


class RequestIdFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.request_id = request_id_var.get("")
        return True


def setup_logging() -> None:
    log_format = "%(asctime)s | %(levelname)-8s | %(name)s | request_id=%(request_id)s | %(message)s"
    
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(log_format))
    handler.addFilter(RequestIdFilter())
    
    root_logger = logging.getLogger()
    root_logger.setLevel(settings.LOG_LEVEL)
    root_logger.handlers = [handler]

    uvicorn_access_logger = logging.getLogger("uvicorn.access")
    uvicorn_access_logger.handlers = []
    uvicorn_access_logger.propagate = False
    uvicorn_access_logger.disabled = True

    logging.getLogger("app.access").setLevel(settings.LOG_LEVEL)


EXCLUDED_PATHS = {"/openapi.json", "/docs", "/redoc"}


class RequestIdMiddleware:
    def __init__(self, app) -> None:
        self.app = app

    async def __call__(self, scope, receive, send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        path = scope.get("path", "")
        headers = dict(scope.get("headers", []))
        request_id = headers.get(b"x-request-id", b"").decode() or str(uuid.uuid4())
        request_id_var.set(request_id)

        start = time.perf_counter()
        status_code = 500

        async def send_with_header(message):
            nonlocal status_code
            if message["type"] == "http.response.start":
                status_code = message.get("status", 500)
                headers_list = list(message.get("headers", []))
                headers_list.append((b"x-request-id", request_id.encode()))
                message["headers"] = headers_list
            await send(message)

        try:
            await self.app(scope, receive, send_with_header)
        finally:
            if path not in EXCLUDED_PATHS:
                duration_ms = (time.perf_counter() - start) * 1000
                access_logger.log(
                    logging.INFO,
                    '{"request_id": "%s", "method": "%s", "path": "%s", "status": %d, "duration_ms": %.1f}',
                    request_id,
                    scope.get("method", ""),
                    path,
                    status_code,
                    duration_ms,
                )
