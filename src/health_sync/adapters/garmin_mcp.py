"""Garmin MCP adapter."""

from __future__ import annotations

from typing import Any

from health_sync.adapters.mcp import McpToolClient


class GarminMcpClient:
    def __init__(self, url: str):
        self.client = McpToolClient(url)

    async def get_body_composition(self, start_date: str, end_date: str | None = None) -> Any:
        args: dict[str, Any] = {"start_date": start_date}
        if end_date:
            args["end_date"] = end_date
        return await self.client.call_tool("get_body_composition", args)

    async def add_body_composition(self, payload: dict[str, Any]) -> Any:
        return await self.client.call_tool("add_body_composition", payload)

    async def add_weigh_in_with_timestamps(
        self,
        *,
        weight: float,
        date_timestamp: str,
        gmt_timestamp: str | None = None,
        unit_key: str = "kg",
    ) -> Any:
        args = {
            "weight": weight,
            "unit_key": unit_key,
            "date_timestamp": date_timestamp,
        }
        if gmt_timestamp:
            args["gmt_timestamp"] = gmt_timestamp
        return await self.client.call_tool("add_weigh_in_with_timestamps", args)

    async def log_food(
        self,
        *,
        meal_date: str,
        meal_time: str,
        name: str,
        calories: float,
        carbs: float,
        protein: float,
        fat: float,
    ) -> Any:
        return await self.client.call_tool(
            "log_food",
            {
                "meal_date": meal_date,
                "meal_time": meal_time,
                "name": name,
                "calories": calories,
                "carbs": carbs,
                "protein": protein,
                "fat": fat,
            },
        )
