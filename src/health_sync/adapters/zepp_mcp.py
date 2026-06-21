"""Zepp Life MCP adapter."""

from __future__ import annotations

from typing import Any

from health_sync.adapters.mcp import McpToolClient


class ZeppMcpClient:
    def __init__(self, url: str):
        self.client = McpToolClient(url)

    async def sync_body_measurements(self, start_date: str, end_date: str) -> Any:
        return await self.client.call_tool(
            "sync_data",
            {
                "data_types": ["body_measurements"],
                "start_date": start_date,
                "end_date": end_date,
                "force_full_sync": False,
            },
        )

    async def query_body_measurements(
        self,
        start_date: str,
        end_date: str,
        latest_only: bool = False,
    ) -> list[dict[str, Any]]:
        response = await self.client.call_tool(
            "query_body_measurements",
            {
                "start_date": start_date,
                "end_date": end_date,
                "latest_only": latest_only,
            },
        )
        data = (response or {}).get("data") or {}
        measurements = data.get("measurements") or []
        return list(measurements)
