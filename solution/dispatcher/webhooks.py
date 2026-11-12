"""Webhook receiver: the provider sends call results here.

Each result is checked and passed into the workflow as the `call_result` signal.
Once Temporal accepts a signal it is stored on the Temporal server, so it is safe
even if our worker is down at that moment.

Responses:
  200  signature valid and the result was handed to Temporal
  401  signature missing or wrong (not from the provider)
  400  body is not a valid call result
  503  Temporal could not be reached; the provider will redeliver
"""
from __future__ import annotations

import hashlib
import hmac
import json
import logging
from dataclasses import asdict
from typing import Awaitable, Callable

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from .provider import CallResult

log = logging.getLogger(__name__)

SIGNATURE_HEADER = "X-AU-Signature"

SignalFn = Callable[[CallResult], Awaitable[None]]
StatusFn = Callable[[], Awaitable[dict]]


def valid_signature(secret: str, body: bytes, signature: str | None) -> bool:
    """HMAC-SHA256 of the raw body, compared in constant time."""
    if not signature:
        return False
    expected = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature.strip().lower())


def create_app(secret: str, send_signal: SignalFn, get_status: StatusFn | None = None) -> FastAPI:
    app = FastAPI(title="Campaign Dispatcher", docs_url=None, redoc_url=None)

    @app.post("/webhooks/telephony")
    async def telephony_webhook(request: Request) -> JSONResponse:
        body = await request.body()  # raw bytes: the signature is over these exact bytes
        if not valid_signature(secret, body, request.headers.get(SIGNATURE_HEADER)):
            log.warning("webhook with bad or missing signature rejected")
            return JSONResponse({"error": "invalid_signature"}, status_code=401)
        try:
            result = CallResult.from_dict(json.loads(body))
        except (ValueError, KeyError, TypeError) as exc:
            log.warning("webhook with unreadable body rejected: %s", exc)
            return JSONResponse({"error": "invalid_body"}, status_code=400)
        try:
            await send_signal(result)
        except Exception as exc:  # Temporal down or unreachable: let the provider retry
            log.error("could not hand result %s to Temporal: %s", result.call_sid, exc)
            return JSONResponse({"error": "temporarily_unavailable"}, status_code=503)
        return JSONResponse({"ok": True})

    @app.get("/status")
    async def status() -> JSONResponse:
        if get_status is None:
            return JSONResponse({"error": "not_running"}, status_code=503)
        try:
            return JSONResponse(await get_status())
        except Exception as exc:
            return JSONResponse({"error": str(exc)}, status_code=503)

    @app.get("/health")
    async def health() -> dict:
        return {"ok": True}

    return app


def status_to_dict(status) -> dict:
    """The workflow's status query result as plain JSON."""
    return asdict(status) if not isinstance(status, dict) else status
