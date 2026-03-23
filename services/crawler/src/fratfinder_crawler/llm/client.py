from __future__ import annotations

import json
from typing import Any

from fratfinder_crawler.config import Settings, get_settings

try:  # pragma: no cover - exercised via monkeypatch in tests when package is absent
    from openai import OpenAI
except ImportError:  # pragma: no cover - local test env may not have the SDK installed
    OpenAI = None


class LLMUnavailableError(RuntimeError):
    pass


class LLMClient:
    def __init__(
        self,
        settings: Settings | None = None,
        client_factory: Any | None = None,
    ) -> None:
        self._settings = settings or get_settings()
        if not self._settings.crawler_llm_enabled:
            raise LLMUnavailableError("LLM support is disabled")
        if not self._settings.openai_api_key:
            raise LLMUnavailableError("OPENAI_API_KEY is not configured")

        if client_factory is None:
            if OpenAI is None:
                raise LLMUnavailableError("openai package is not installed")
            client_factory = OpenAI

        self._client = client_factory(api_key=self._settings.openai_api_key)

    def create_json_completion(
        self,
        *,
        system_prompt: str,
        user_prompt: str,
        schema_name: str,
        schema: dict[str, Any],
        max_tokens: int | None = None,
    ) -> dict[str, Any]:
        response = self._client.chat.completions.create(
            model=self._settings.crawler_llm_model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            response_format={
                "type": "json_schema",
                "json_schema": {
                    "name": schema_name,
                    "strict": True,
                    "schema": schema,
                },
            },
            max_tokens=max_tokens or self._settings.crawler_llm_max_tokens,
            temperature=0,
        )
        content = _extract_message_content(response)
        return json.loads(content)



def _extract_message_content(response: Any) -> str:
    try:
        content = response.choices[0].message.content
    except Exception as exc:  # pragma: no cover - defensive parsing
        raise ValueError("LLM response did not include message content") from exc

    if isinstance(content, str):
        return content

    if isinstance(content, list):
        text_parts: list[str] = []
        for part in content:
            if isinstance(part, dict):
                text = part.get("text")
            else:
                text = getattr(part, "text", None)
            if text:
                text_parts.append(str(text))
        if text_parts:
            return "".join(text_parts)

    raise ValueError("LLM response content was not a JSON string")
