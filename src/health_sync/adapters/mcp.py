"""MCP Streamable HTTP client helpers."""

from __future__ import annotations

import json
from typing import Any

from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client


def parse_text_payload(text: str) -> Any:
    stripped = text.strip()
    if not stripped:
        return None
    try:
        return json.loads(stripped)
    except json.JSONDecodeError:
        pass

    decoder = json.JSONDecoder()
    for index, char in enumerate(stripped):
        if char not in "{[":
            continue
        try:
            value, _ = decoder.raw_decode(stripped[index:])
            return value
        except json.JSONDecodeError:
            continue
    return text


def parse_tool_result(result: Any) -> Any:
    if getattr(result, "isError", False):
        raise RuntimeError(f"MCP tool call failed: {result}")

    content = getattr(result, "content", None) or []
    texts = [item.text for item in content if getattr(item, "type", None) == "text"]
    if not texts:
        return None
    if len(texts) == 1:
        return parse_text_payload(texts[0])
    return [parse_text_payload(text) for text in texts]


class McpToolClient:
    def __init__(self, url: str, *, timeout: float = 60, sse_read_timeout: float = 300):
        self.url = url
        self.timeout = timeout
        self.sse_read_timeout = sse_read_timeout

    async def call_tool(self, name: str, arguments: dict[str, Any] | None = None) -> Any:
        async with streamablehttp_client(
            self.url,
            timeout=self.timeout,
            sse_read_timeout=self.sse_read_timeout,
        ) as (read_stream, write_stream, _get_session_id), ClientSession(
            read_stream,
            write_stream,
        ) as session:
            await session.initialize()
            result = await session.call_tool(name, arguments or {})
            return parse_tool_result(result)
