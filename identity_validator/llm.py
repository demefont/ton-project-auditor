from __future__ import annotations

import asyncio
import json
import os
import urllib.request
from typing import Any, Dict

from .tracing import TraceStore


def _empty_template_llm_explainer_payload(metadata: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "project_name": str(metadata.get("project_name") or ""),
        "project_type": str(metadata.get("project_type") or "unknown"),
        "project_overview_en": "",
        "project_overview_ru": "",
        "explanation_text_en": "",
        "explanation_text_ru": "",
        "evidence_used": [],
    }


class BaseLLMClient:
    async def complete(
        self,
        block_id: str,
        model: str,
        prompt: str,
        trace_store: TraceStore,
        metadata: Dict[str, Any],
    ) -> str:
        raise NotImplementedError()


class TemplateLLMClient(BaseLLMClient):
    async def complete(
        self,
        block_id: str,
        model: str,
        prompt: str,
        trace_store: TraceStore,
        metadata: Dict[str, Any],
    ) -> str:
        if block_id == "project_discovery":
            response = json.dumps(
                {
                    "summary": "Template mode does not produce grounded candidate ranking.",
                    "selected_candidate_key": "",
                    "ranked_candidates": [],
                },
                ensure_ascii=False,
            )
            trace_store.save_llm_trace(block_id, model, prompt, response, metadata)
            return response
        if block_id == "telegram_semantics":
            response = ""
            trace_store.save_llm_trace(block_id, model, prompt, response, metadata)
            return response
        if block_id == "llm_explainer":
            response = json.dumps(_empty_template_llm_explainer_payload(metadata), ensure_ascii=False)
            trace_store.save_llm_trace(block_id, model, prompt, response, metadata)
            return response
        response = ""
        trace_store.save_llm_trace(block_id, model, prompt, response, metadata)
        return response


class HttpChatLLMClient(BaseLLMClient):
    def __init__(self) -> None:
        self._openai_sem = asyncio.Semaphore(4)
        self._sonar_sem = asyncio.Semaphore(2)

    async def complete(
        self,
        block_id: str,
        model: str,
        prompt: str,
        trace_store: TraceStore,
        metadata: Dict[str, Any],
    ) -> str:
        low_model = str(model or "").lower()
        is_sonar = "sonar" in low_model
        api_url = os.getenv("PERPLEXITY_API_URL", "https://api.perplexity.ai") if is_sonar else os.getenv("OPENAI_API_URL", "https://api.openai.com/v1")
        api_key = os.getenv("PERPLEXITY_API_KEY", "") if is_sonar else os.getenv("OPENAI_API_KEY", "")
        if not api_key:
            raise RuntimeError(f"API key is missing for model={model}")
        sem = self._sonar_sem if is_sonar else self._openai_sem
        payload = {
            "model": model,
            "messages": [{"role": "user", "content": prompt}],
        }
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }

        async with sem:
            raw = await asyncio.to_thread(self._post_json, f"{api_url}/chat/completions", headers, payload)
        content = (
            ((raw.get("choices") or [{}])[0].get("message") or {}).get("content")
            if isinstance(raw, dict)
            else None
        )
        if not content:
            raise RuntimeError(f"LLM response is empty for model={model}")
        trace_store.save_llm_trace(block_id, model, prompt, str(content), metadata)
        return str(content)

    @staticmethod
    def _post_json(url: str, headers: Dict[str, str], payload: Dict[str, Any]) -> Dict[str, Any]:
        request = urllib.request.Request(
            url,
            headers=headers,
            data=json.dumps(payload).encode("utf-8"),
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=120) as response:
            return json.loads(response.read().decode("utf-8", errors="replace"))


def build_llm_client(mode: str) -> BaseLLMClient:
    if mode == "live":
        return HttpChatLLMClient()
    return TemplateLLMClient()
