"""Yazio MCP adapter."""

from __future__ import annotations

from typing import Any

from health_sync.adapters.mcp import McpToolClient


class YazioMcpClient:
    def __init__(self, url: str):
        self.client = McpToolClient(url)

    async def get_daily_summary(self, target_date: str) -> dict[str, Any]:
        response = await self.client.call_tool("get_user_daily_summary", {"date": target_date})
        if not isinstance(response, dict):
            raise ValueError(f"Unexpected Yazio daily summary response: {response!r}")
        return response
