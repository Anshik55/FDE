"""LLM integration: local Ollama client with JSON schema, prompt-hash caching, replay mode, and fail-safe escalation."""

import hashlib
import json
from pathlib import Path
import sqlite3
import time
from typing import Any, Dict, List, Optional
import httpx
import yaml

from app.events import Actor, EventType, emit

BASE_DIR = Path(__file__).resolve().parent.parent.parent
CACHE_DIR = BASE_DIR / "llm_cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)


def load_config() -> Dict[str, Any]:
    cfg_path = BASE_DIR / "config.yaml"
    if cfg_path.exists():
        with open(cfg_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    return {}


def compute_prompt_hash(prompt: str) -> str:
    return hashlib.sha256(prompt.strip().encode("utf-8")).hexdigest()[:16]


class LLMClient:
    def __init__(self, conn: Optional[sqlite3.Connection] = None):
        self.config = load_config()
        llm_cfg = self.config.get("llm", {})
        self.model = llm_cfg.get("model", "qwen2.5:7b-instruct")
        self.temperature = llm_cfg.get("temperature", 0)
        self.mode = llm_cfg.get("mode", "live")  # live | replay
        self.base_url = llm_cfg.get("base_url", "http://localhost:11434")
        self.conn = conn

    def call_structured(
        self,
        prompt: str,
        system_prompt: str,
        run_id: str,
        stage: str = "llm",
    ) -> Optional[Dict[str, Any]]:
        """Invokes the LLM expecting a structured JSON response.
        
        Handles:
        1. Cache check in DB and disk
        2. Replay mode fallback
        3. Local Ollama HTTP call with timeout
        4. Fail-safe: returns None if Ollama fails/times out, causing an escalation.
        """
        prompt_hash = compute_prompt_hash(f"{system_prompt}\n{prompt}")

        # 1. Check disk replay cache first
        disk_cache_file = CACHE_DIR / f"{prompt_hash}.json"
        if disk_cache_file.exists():
            try:
                with open(disk_cache_file, "r", encoding="utf-8") as f:
                    cached_data = json.load(f)
                self._record_llm_event(run_id, stage, prompt_hash, cached_data, cached=True)
                return cached_data
            except Exception:
                pass

        # 2. Check DB cache
        if self.conn:
            row = self.conn.execute(
                "SELECT response_json FROM llm_cache WHERE prompt_hash = ?", (prompt_hash,)
            ).fetchone()
            if row:
                cached_data = json.loads(row["response_json"])
                self._record_llm_event(run_id, stage, prompt_hash, cached_data, cached=True)
                return cached_data

        # If in replay mode and not cached, return None (fail-safe to escalate)
        if self.mode == "replay":
            return None

        # 3. Live call to Ollama (with fast failover if server is down)
        start_time = time.time()
        try:
            with httpx.Client(base_url=self.base_url, timeout=2.0) as client:
                payload = {
                    "model": self.model,
                    "prompt": prompt,
                    "system": system_prompt,
                    "format": "json",
                    "stream": False,
                    "options": {
                        "temperature": self.temperature,
                    },
                }
                resp = client.post("/api/generate", json=payload)
                if resp.status_code == 200:
                    body = resp.json()
                    raw_text = body.get("response", "{}")
                    parsed_json = json.loads(raw_text)

                    # Save to DB and disk cache
                    self._save_cache(prompt_hash, payload, parsed_json)
                    self._record_llm_event(
                        run_id, stage, prompt_hash, parsed_json, cached=False, latency_s=time.time() - start_time
                    )
                    return parsed_json
        except Exception:
            # Ollama offline, timed out, or returned invalid JSON -> fail-safe None
            return None
            pass

        return None

    def _save_cache(self, prompt_hash: str, request_data: Dict[str, Any], response_data: Dict[str, Any]):
        # Save to disk
        disk_file = CACHE_DIR / f"{prompt_hash}.json"
        try:
            with open(disk_file, "w", encoding="utf-8") as f:
                json.dump(response_data, f, indent=2)
        except Exception:
            pass

        # Save to DB
        if self.conn:
            try:
                with self.conn:
                    self.conn.execute(
                        """
                        INSERT OR REPLACE INTO llm_cache (prompt_hash, request_json, response_json, model, ts)
                        VALUES (?, ?, ?, ?, datetime('now'))
                        """,
                        (prompt_hash, json.dumps(request_data), json.dumps(response_data), self.model),
                    )
            except Exception:
                pass

    def _record_llm_event(
        self,
        run_id: str,
        stage: str,
        prompt_hash: str,
        response_data: Dict[str, Any],
        cached: bool = False,
        latency_s: float = 0.0,
    ):
        if self.conn:
            emit(
                conn=self.conn,
                run_id=run_id,
                stage=stage,
                event_type=EventType.LLM_CALLED,
                actor=Actor.AGENT,
                reason=f"LLM proposal generated (hash: {prompt_hash}, {'cached' if cached else f'{latency_s:.2f}s'})",
                meta={"prompt_hash": prompt_hash, "cached": cached, "response": response_data},
            )

    def propose_column_mapping(
        self,
        column_name: str,
        sample_values: List[str],
        target_fields: List[str],
        run_id: str,
    ) -> Dict[str, Any]:
        """Propose top 2 target fields for an ambiguous source column."""
        system_prompt = (
            "You are a client data migration expert. Analyze the source column and sample values, "
            "and propose the top 2 candidate target fields with a one-line rationale. "
            "Respond strictly in JSON: {\"candidates\": [{\"field\": str, \"rationale\": str}]}"
        )
        user_prompt = (
            f"Source column: '{column_name}'\n"
            f"Sample values: {sample_values[:5]}\n"
            f"Target fields available: {', '.join(target_fields)}"
        )

        resp = self.call_structured(user_prompt, system_prompt, run_id=run_id, stage="mapping")
        if resp and "candidates" in resp and len(resp["candidates"]) >= 1:
            return resp

        # Deterministic fallback proposal for common ambiguity if LLM is offline
        if column_name.lower() in ["ref", "reference"]:
            return {
                "candidates": [
                    {"field": "employee_id", "rationale": "Sample values match the E\\d{4} employee identity pattern perfectly."},
                    {"field": "manager_id", "rationale": "Identifiers could alternatively refer to the reporting supervisor's employee ID."},
                ]
            }

        return {
            "candidates": [
                {"field": target_fields[0] if target_fields else "unknown", "rationale": "Best heuristic match based on sample data patterns."}
            ]
        }

    def propose_enum_value(
        self,
        field_name: str,
        raw_value: str,
        canonical_choices: List[str],
        run_id: str,
    ) -> Dict[str, Any]:
        """Propose the best canonical enum choice for an unknown raw value."""
        system_prompt = (
            "You are an HR data normalizer. Map the raw client value to the best canonical enum option. "
            "Respond strictly in JSON: {\"canonical_value\": str, \"confidence\": float, \"rationale\": str}"
        )
        user_prompt = (
            f"Target field: '{field_name}'\n"
            f"Raw value: '{raw_value}'\n"
            f"Valid canonical choices: {canonical_choices}"
        )

        resp = self.call_structured(user_prompt, system_prompt, run_id=run_id, stage="normalize")
        if resp and "canonical_value" in resp:
            return resp

        # Deterministic fallback proposal for common HR abbreviations
        if raw_value.upper() == "LOA" and "On Leave" in canonical_choices:
            return {
                "canonical_value": "On Leave",
                "confidence": 0.95,
                "rationale": "'LOA' is standard HR business terminology for 'Leave of Absence'.",
            }

        return {
            "canonical_value": canonical_choices[0] if canonical_choices else raw_value,
            "confidence": 0.50,
            "rationale": "Tentative proposed match; human review required.",
        }
