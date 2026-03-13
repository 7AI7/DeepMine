Overview
This repository implements a two‑stage pipeline to crawl manufacturer websites, sort links, extract structured data per page, and upsert to Postgres in an idempotent, asynchronous manner.

Dependencies
Languages/SDKs: Python 3.11+, google‑genai SDK for Gemini API/Vertex, OpenRouter or xAI SDK for Grok‑4‑Fast, Pydantic/JSONSchema validators, and psycopg2/SQLAlchemy for DB.

Services: Gemini 2.0 Flash Batch API with structured outputs and explicit context caching; OpenRouter xAI Grok‑4‑Fast for triage; PostgreSQL 14+.

Environment variables
GEMINI keys and region for Batch and caching, with Vertex setting where applicable; OpenRouter or xAI API key for triage; Postgres DSN.

Configure cache TTL (e.g., 24–72 hours) to span batch windows, and batch size limits per job aligned to API quotas.

Directory layout
crawler/: robots‑aware fetcher and frontier manager for BFS under selected roots with depth cap ≤ 3.

triage/: Grok‑4‑Fast structured output classifier for homepage links to select root_paths deterministically.

extractor/: Gemini Batch job builder with responseSchema and context cache integration for per‑page extraction.

validator/: JSON Schema validators and repair pass for non‑conforming outputs, enforcing null vs empty rules.

merger/: merge and dedupe logic keyed by company_domain and composite keys, with provenance retention.

upsert/: SQL generation and bulk upserts with ON CONFLICT per table order specified.

ops/: batch orchestration, polling, retries, metrics, and cost accounting.

Crawl and triage
Homepage fetch: parse anchors, canonicalize href paths, build candidate list; drop obvious noise with a regex blacklist on path tokens.

Triage call: Grok‑4‑Fast structured outputs classify links and return root_paths; configure a paid fallback to avoid free‑tier caps or route changes.

BFS traversal
Only enqueue descendants whose canonicalized path starts with selected root_paths; set depth cap ≤ 3 and cap fan‑out per depth to stabilize IO.

Persist per‑page provenance for downstream merging and audits while avoiding non‑HTML content types.

Extraction requests (Gemini Batch)
Per page, add a system instruction, a compact responseSchema that mirrors the DB tables, and the cleaned content in a JSONL line with application/json response.

Keep decoding conservative (temperature ≤ 0.2, topK=1) and cap maxOutputTokens to stay well within the 8,192 output limit.

Context caching
Create a content cache with the shared instruction + schema and a TTL covering the batch window; reference it in every Batch request to discount repeated input tokens.

Monitor cache usage metadata and refresh TTL or rotate cache when schema changes to maintain a high cache hit‑rate.

Validation and repair
Validate every response against the responseSchema; when invalid, run a single repair prompt or deterministic coercion; quarantine records that still fail.

Enforce null vs empty strictly and split numeric capacity values from units, truncating extras to the size limits in the “rules and how” guide.

Merge and upsert
Merge page outputs per domain, deduping with composite keys and keeping the most complete record per item; store provenance URLs in extras for audit.

Upsert in order: companies → company_infra_blocks → company_machines → other child tables, using ON CONFLICT per documented keys and committing only on full success.

Operations and cost
Submit Batch jobs with JSONL; poll operations until success; partition by domains to avoid large single jobs; record job IDs and result file references.

Cost guardrails: Batch yields ~50% discount, and context caching reduces repeated input cost; token usage scales roughly with characters, so aggressive HTML trimming is required.

SRE considerations
Metrics: schema pass rate, repair rate, tokens/request, cached‑input share, cost per site, batch turnaround, and page failure rates; alarms on excessive quarantine.

Re‑crawl strategy: persistent frontier storage for revisit, change detection via hashing cleaned content, and targeted re‑batching of changed pages.

Unified Task List with Tags and Dependencies
Phase 1: Foundations

[CRAWL‑01] Implement robots‑aware homepage fetch, anchor parsing, canonicalization, and candidate list persistence; dep: none.

[TRIAGE‑01] Implement Grok‑4‑Fast structured output classifier on {anchor, href} for homepage links to select root_paths; dep: CRAWL‑01.

[TRIAGE‑02] Add paid fallback route and rate caps for Grok‑4‑Fast; dep: TRIAGE‑01.

Phase 2: Discovery

[CRAWL‑02] Implement BFS frontier traversal under root_paths with depth ≤ 3 and first‑party restriction; dep: TRIAGE‑01.

[CRAWL‑03] Implement MIME/type filter to skip non‑HTML and persist {page_url, root_path, depth}; dep: CRAWL‑02.

Phase 3: Extraction

[EXTRACT‑01] Implement HTML cleaner to strip nav/footer/scripts/styles and keep title/H1–H3 + relevant sections; dep: CRAWL‑03.

[EXTRACT‑02] Define responseSchema mirroring DB tables and conservative decoding defaults; dep: EXTRACT‑01.

[CACHE‑01] Create context cache for shared instruction+schema, set TTL=72h, and add request referencing to every JSONL line; dep: EXTRACT‑02.

[OPS‑01] Build JSONL generator for Gemini Batch and a batch submit/poll/download runner; dep: CACHE‑01.

Phase 4: Quality and Merge

[VALIDATE‑01] Implement client‑side JSON Schema validation and one‑shot repair path; dep: OPS‑01.

[MERGE‑01] Implement normalization, unit splitting, dedupe by composite keys, and provenance retention; dep: VALIDATE‑01.

[UPSERT‑01] Generate bulk SQL with ON CONFLICT per table order, set last_crawled, and transactional commit; dep: MERGE‑01.

Phase 5: Observability and Cost

[OBS‑01] Instrument metrics for schema pass rate, tokens, cached‑input share, cost per site, and batch turnaround; dep: OPS‑01.

[OBS‑02] Implement quarantine store and retry queues for failed validations and batch errors; dep: VALIDATE‑01.

Outstanding tasks (must complete before scale‑up)

[TRIAGE‑02], [CRAWL‑02], [EXTRACT‑02], [CACHE‑01], [OPS‑01], [VALIDATE‑01], [MERGE‑01], [UPSERT‑01], [OBS‑01], [OBS‑02]; prioritize in listed order to achieve first full, correct pipeline run before adding advanced telemetry.

Example request shapes
Triage (Grok‑4‑Fast via OpenRouter, structured outputs)

System: “Classify first‑party homepage links for an Indian manufacturer; output strictly JSON: {keep:boolean, category:enum[products,services,infrastructure,capabilities,about,contact,other], reason:string}.”

User: [{"anchor":"Infrastructure","href":"/infrastructure"},{"anchor":"Products","href":"/products"}] with response_format enforcing schema on the gateway to ensure parseable output.

Extraction (Gemini 2.0 Flash Batch JSONL line)

{"key":"extract_001","request":{"model":"gemini-2.0-flash","contents":[{"parts":[{"text":"SYSTEM: Emit only application/json matching the provided responseSchema; set null for missing; no prose."},{"text":"PAGE_META: url=https://mfg.example/infrastructure title=Infrastructure h1=Our Forging Division"},{"text":"HTML_TEXT: [cleaned content here]"}]}],"generation_config":{"response_mime_type":"application/json"},"response_schema":{…}} referencing the context cache for instruction/schema to reduce input cost.

Why this plan avoids prior pitfalls
No DOM‑section reliance: decisions are made at the homepage via classification plus path inheritance under selected roots, avoiding false drops like relevant footer links or generic child slugs.

No snippet or shallow‑fetch complexity: homepage classification plus constrained traversal reduces IO and tokens and still reaches deep product/infra subpages deterministically.

Page‑level extraction ensures isolation, easy retries, and predictable output sizes under Gemini 2.0 Flash’s response cap while enabling exact schema enforcement and validation.

This PRD, checklist, and README specify the models, APIs, cache usage, batch flow, schema contracts, and merge/upsert logic so each agent can implement its task without further clarification.

python -m orchestrator watch-batches --root data/ab
python -m orchestrator --triage-range 251 50 --triage-parallel 7