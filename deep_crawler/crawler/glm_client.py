"""
GLM-4-Flash Client for Direct API Extraction

Handles ≤10 page websites with optional splitting for large content.
Uses existing GLM API format from orchestrator.py.
"""

from pathlib import Path
from typing import Dict, Any, List
import json
import time
import random
import os
from zhipuai import ZhipuAI

from crawler.ai_client import AIClient, ExtractionError
from crawler.token_utils import estimate_tokens_with_overhead
from crawler.page_utils import concatenate_pages, split_pages_in_half
from crawler.merge_utils import merge_split_extractions
from crawler.gemini_prompts import get_whole_website_prompt

import logging
LOG = logging.getLogger('glm_client')


class GLMClient(AIClient):
    """GLM-4-Flash client for ≤10 page websites"""

    def __init__(self, api_keys: List[str] | None = None):
        """
        Initialize GLM client.

        Args:
            api_keys: List of API keys for rotation (defaults to env vars)
        """
        if api_keys is None:
            api_keys = [
                os.environ.get('ZHIPUAI_API_KEY1'),
                os.environ.get('ZHIPUAI_API_KEY2'),
                os.environ.get('ZHIPUAI_API_KEY')
            ]
            api_keys = [k for k in api_keys if k]

        if not api_keys:
            raise ValueError('No GLM API keys found')

        self.api_keys = api_keys
        self.clients = [ZhipuAI(api_key=k) for k in api_keys]
        self.current_client_idx = 0
        LOG.info(f'GLM client initialized with {len(api_keys)} API keys')

    def get_name(self) -> str:
        return 'GLM-4-Flash-250414'

    @property
    def token_limit(self) -> int:
        return 128000  # GLM-4-Flash context window

    @property
    def split_threshold(self) -> int:
        return 50000  # Split at 50K tokens per plan

    def _get_client(self) -> ZhipuAI:
        """Get next client (for load balancing across keys)"""
        client = self.clients[self.current_client_idx]
        self.current_client_idx = (self.current_client_idx + 1) % len(self.clients)
        return client

    def _make_request_with_retry(
        self,
        client: ZhipuAI,
        messages: List[Dict[str, str]],
        max_retries: int = 3
    ) -> str:
        """
        Make API request with retry logic.

        Args:
            client: ZhipuAI client instance
            messages: OpenAI-style messages [{"role": "system"/"user", "content": "..."}]
            max_retries: Maximum retry attempts

        Returns:
            Response content as string

        Raises:
            ExtractionError: If all retries fail
        """
        backoff = 0.5

        for attempt in range(max_retries):
            try:
                response = client.chat.completions.create(
                    model='glm-4-flash-250414',
                    messages=messages,
                    temperature=0.0,
                    max_tokens=4096,
                    response_format={"type": "json_object"}
                )
                return response.choices[0].message.content.strip()

            except Exception as e:
                error_str = str(e).lower()
                LOG.warning(f'GLM request failed (attempt {attempt+1}/{max_retries}): {e}')

                if attempt == max_retries - 1:
                    raise ExtractionError(f'GLM request failed after {max_retries} attempts: {e}')

                # Retry on rate limit or server errors
                if any(x in error_str for x in ['rate', '429', '503', '500']):
                    sleep_time = backoff * (2 ** attempt) + random.uniform(0, 0.4)
                    time.sleep(sleep_time)
                else:
                    # Client error, don't retry
                    raise ExtractionError(f'GLM client error: {e}')

    def _parse_json_response(self, raw_response: str) -> Dict[str, Any]:
        """Parse JSON from GLM response (handles markdown fences)"""
        raw = raw_response.strip()

        # Remove markdown code fences if present
        if raw.startswith('```'):
            lines = raw.split('\n')
            if lines[0].startswith('```'):
                lines = lines[1:]  # Remove first ```json line
            if lines and lines[-1].strip() == '```':
                lines = lines[:-1]  # Remove last ``` line
            raw = '\n'.join(lines)

        try:
            return json.loads(raw)
        except json.JSONDecodeError as e:
            LOG.error(f'Failed to parse JSON response: {e}')
            LOG.error(f'Raw response (first 500 chars): {raw[:500]}')
            raise ExtractionError(f'Invalid JSON response: {e}')

    def extract(
        self,
        pages: List[Dict[str, str]],
        company_id: int,
        domain: str,
        output_dir: Path
    ) -> Dict[str, Any]:
        """
        Extract data from pages using GLM-4-Flash.

        Flow:
        1. Get system prompt and schema
        2. Concatenate pages (with metadata if split)
        3. Check token count against 50K threshold
        4. If >50K: split pages in half, send 2 requests, merge results
        5. Else: send single request
        6. Save artifacts to output_dir/glm_extraction/
        """
        LOG.info(f'GLM extraction: company_id={company_id} pages={len(pages)}')

        # Create extraction directory
        extract_dir = output_dir / 'glm_extraction'
        extract_dir.mkdir(parents=True, exist_ok=True)

        # Get prompt (non-split version for token estimation)
        system, schema = get_whole_website_prompt(is_split=False)
        schema_str = json.dumps(schema, ensure_ascii=False)

        # Concatenate pages (no metadata yet)
        concatenated = concatenate_pages(pages)

        # Check token count
        total_tokens = estimate_tokens_with_overhead(concatenated, system, schema_str)
        LOG.info(f'Total tokens: {total_tokens} (threshold: {self.split_threshold})')

        # Decide: single or split
        if total_tokens <= self.split_threshold:
            result = self._extract_single(pages, system, extract_dir)
        else:
            result = self._extract_split(pages, system, extract_dir)

        # Save final output
        (extract_dir / 'output.json').write_text(
        json.dumps(result, ensure_ascii=False, indent=2),
        encoding='utf-8'
        )
        

        LOG.info(f'GLM extraction complete: company_id={company_id}')
        return result

    def _extract_single(
        self,
        pages: List[Dict[str, str]],
        system: str,
        extract_dir: Path
    ) -> Dict[str, Any]:
        """Single request extraction (<=50K tokens)"""
        LOG.info('Using single request (<=50K tokens)')

        # Concatenate without split metadata
        concatenated = concatenate_pages(pages)

        # Save concatenated input
        (extract_dir / 'input.txt').write_text(concatenated, encoding='utf-8')

        # Build OpenAI-style messages
        messages = [
            {'role': 'system', 'content': system},
            {'role': 'user', 'content': concatenated}
        ]

        # Make request
        client = self._get_client()
        raw_response = self._make_request_with_retry(client, messages)

        # Parse result
        result = self._parse_json_response(raw_response)
        return result

    def _extract_split(
        self,
        pages: List[Dict[str, str]],
        system: str,
        extract_dir: Path
    ) -> Dict[str, Any]:
        """Split request extraction (>50K tokens) - uses smart delimiter approach"""
        LOG.info('Using split requests (>50K tokens) - splitting pages')

        # Split pages in half
        pages_part1, pages_part2 = split_pages_in_half(pages)
        LOG.info(f'Split: part1={len(pages_part1)} pages, part2={len(pages_part2)} pages')

        # Concatenate with smart delimiter metadata
        concat_part1 = '[EXTRACTION_CONTEXT: SPLIT_PART=1/2]\n\n' + concatenate_pages(pages_part1)
        concat_part2 = '[EXTRACTION_CONTEXT: SPLIT_PART=2/2]\n\n' + concatenate_pages(pages_part2)

        full_input = concat_part1 + "\n\n" + concat_part2
        (extract_dir / 'input.txt').write_text(full_input, encoding='utf-8')

        # Extract part 1
        messages_part1 = [
            {'role': 'system', 'content': system},
            {'role': 'user', 'content': concat_part1}
        ]
        client = self._get_client()
        raw_response1 = self._make_request_with_retry(client, messages_part1)
        result1 = self._parse_json_response(raw_response1)

        # Extract part 2
        messages_part2 = [
            {'role': 'system', 'content': system},
            {'role': 'user', 'content': concat_part2}
        ]
        client = self._get_client()
        raw_response2 = self._make_request_with_retry(client, messages_part2)
        result2 = self._parse_json_response(raw_response2)

        # Merge results
        merged = merge_split_extractions(result1, result2)
        (extract_dir / 'merged_result.json').write_text(
            json.dumps(merged, ensure_ascii=False, indent=2),
            encoding='utf-8'
        )

        LOG.info('Split extraction complete - results merged')
        return merged
