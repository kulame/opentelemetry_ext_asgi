# Copyright kula

"""
This library provides a ASGI middleware that can be used on any ASGI framework
(such as FastAPI) to track requests timing through OpenTelemetry.

Usage (FastAPI)
-------------

.. code-block:: python

    from fastapi import FastAPI
    from opentelemetry.ext.asgi import OpenTelemetryMiddleware

    app = FastAPI()
    app.add_middleware(app.add_middleware)

    @app.get("/")
    async def main():
        return {"message": "Hello World"}


API
---
"""
__author__ = """kula"""
__email__ = 'zhengwei@igengmei.com'
__version__ = '0.1.0'

from typing import Dict, Callable, Any, List, Awaitable, Coroutine
import wsgiref.util as wsgiref_util
from functools import wraps
from opentelemetry import context, propagators, trace, Span
from opentelemetry.trace.propagation import get_span_from_context
from opentelemetry.trace.status import Status, StatusCanonicalCode

_HTTP_VERSION_PREFIX = "HTTP/"


def get_header_from_scope(
    scope: dict, header_name: str
) -> List[str]:
    """Retrieve a HTTP header value from the ASGI scope.

    Returns:
        A list with a single string with the header value if it exists, else an empty list.
    """
    return [value for (key,value) in scope['headers'] if key == header_name]


def setifnotnone(dic, key, value):
    if value is not None:
        dic[key] = value


def http_status_to_canonical_code(code: int, allow_redirect: bool = True):
    # pylint:disable=too-many-branches,too-many-return-statements
    if code < 100:
        return StatusCanonicalCode.UNKNOWN
    if code <= 299:
        return StatusCanonicalCode.OK
    if code <= 399:
        if allow_redirect:
            return StatusCanonicalCode.OK
        return StatusCanonicalCode.DEADLINE_EXCEEDED
    if code <= 499:
        if code == 401:  # HTTPStatus.UNAUTHORIZED:
            return StatusCanonicalCode.UNAUTHENTICATED
        if code == 403:  # HTTPStatus.FORBIDDEN:
            return StatusCanonicalCode.PERMISSION_DENIED
        if code == 404:  # HTTPStatus.NOT_FOUND:
            return StatusCanonicalCode.NOT_FOUND
        if code == 429:  # HTTPStatus.TOO_MANY_REQUESTS:
            return StatusCanonicalCode.RESOURCE_EXHAUSTED
        return StatusCanonicalCode.INVALID_ARGUMENT
    if code <= 599:
        if code == 501:  # HTTPStatus.NOT_IMPLEMENTED:
            return StatusCanonicalCode.UNIMPLEMENTED
        if code == 503:  # HTTPStatus.SERVICE_UNAVAILABLE:
            return StatusCanonicalCode.UNAVAILABLE
        if code == 504:  # HTTPStatus.GATEWAY_TIMEOUT:
            return StatusCanonicalCode.DEADLINE_EXCEEDED
        return StatusCanonicalCode.INTERNAL
    return StatusCanonicalCode.UNKNOWN


def collect_request_attributes(scope: dict) -> dict:
    """Collects HTTP request attributes from the ASGI scope and returns a dictionary to be used as span creation attributes."""
    server = scope.get("server") or ['0.0.0.0',80]
    port = server[1]
    host = server[0] + (":" + str(port) if port != 80 else "")
    http_url = scope.get("scheme") + "://" + host + scope.get("path")
    if scope.get("query_string"):
        http_url = http_url + ("?" + scope.get("query_string").decode("utf8"))
    
    result = {
        "component": scope("type"),
        "http.method": scope["method"],
        "http.schema": scope["scheme"],
        "http.host": host,
        "host.port": port,
        "http.flaver": scope["http_version"],
        "http.target": scope['path'], 
        "http.url": http_url,
    }

    http_host_value = ",".join(get_header_from_scope(scope, "host"))
    if http_host_value:
        result['http.server_name'] = http_host_value
    if "client" in scope and scope["client"] is not None:
        result["net.peer.ip"] = scope.get("client")[0]
        result["net.peer.port"] = scope.get("client")[1]
    return result


def set_status_code(span:Span, status_code:str) -> None:  # pylint: disable=unused-argument
    """Adds HTTP response attributes to span using the status_code argument."""


    try:
        status_code = int(status_code)
    except ValueError:
        span.set_status(
            Status(
                StatusCanonicalCode.UNKNOWN,
                "Non-integer HTTP status: " + repr(status_code),
            )
        )
    else:
        span.set_attribute("http.status_code", status_code)
        span.set_status(Status(http_status_to_canonical_code(status_code)))


def get_default_span_name(scope):
    """Calculates a (generic) span name for an incoming HTTP request based on the ASGI scope."""

    return "HTTP {method}".format({"method":scope.get("method")})


class OpenTelemetryMiddleware:
    """The ASGI application middleware.

    Args:
        app: The ASGI application callable to forward requests to.
    """

    def __init__(self, app:Callable):
        self.app = app
        self.tracer = trace.get_tracer(__name__, __version__)
        self.name_callback = get_default_span_name


    async def __call__(self, scope:dict, receive:Coroutine, send:Coroutine) -> Coroutine:
        """The ASGI application

        Args:
            scope: The connection scope, a dictionary that contains at least a type key specifying the protocol that is incoming.
            receive: an awaitable callable that will yield a new event dictionary when one is available
            send: an awaitable callable taking a single event dictionary as a positional argument that will return once the send has been completed or the connection has been closed
        """

        parent_span = propagators.extract(get_header_from_scope, scope)
        span_name = self.name_callback(scope)
        
        with self.tracer.start_span(
            span_name + " (asgi_connection)",
            parent_span,
            kind=trace.SpanKind.SERVER,
            attributes=collect_request_attributes(scope)
        ):
            @wraps(receive)
            async def wrapped_receive():
                with self.tracer.start_as_current_span(
                    span_name + " (asgi.{type}.receive".format({"type":scope['type']})
                ) as receive_span:
                    payload = await receive()
                    if payload['type'] == 'websocket.receive':
                        set_status_code(receive_span, 200)
                        receive_span.set_attribute('http.status_text',payload['type'])
                return payload
            
            @wraps(send)
            async def wrapped_send(payload):
                with self.tracer.start_as_current_span(
                    span_name + " (asgi.{type}.end)".format({"type":scope['type']})
                ) as send_span:
                    t = payload['type']
                    if t =='http.response.start':
                        status_code = payload['sattus']
                        set_status_code(send_span, status_code)
                    elif t == "websocket.send":
                        set_status_code(send_span, 200)
                        send_span.set_attribute(
                            "http.status_text", payload['text']
                        )
                    send_span.set_attribute("type", t)
                    await send(payload)
        await self.app(scope, wrapped_receive, wrapped_send)

