<div align="center">
  
# 🚀 DeepMine: B2B Lead Generation & Data Intelligence Engine

[![Python](https://img.shields.io/badge/Python-3.11+-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://python.org)
[![Playwright](https://img.shields.io/badge/Playwright-Async-2EAD33?style=for-the-badge&logo=playwright&logoColor=white)](https://playwright.dev/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Advanced-316192?style=for-the-badge&logo=postgresql&logoColor=white)](https://postgresql.org)
[![Google Gemini](https://img.shields.io/badge/Gemini_2.0_Flash-Batch_API-8E75B2?style=for-the-badge&logo=googlebard&logoColor=white)](https://ai.google.dev/)
[![Pandas](https://img.shields.io/badge/Pandas-Data_Processing-150458?style=for-the-badge&logo=pandas&logoColor=white)](https://pandas.pydata.org)

**A high-performance, AI-driven B2B lead generation architecture designed to process thousands of corporate domains, utilizing Large Language Models (LLMs) to construct highly structured, actionable sales intelligence and outreach databases.**

</div>

---

## 🌟 Executive Summary

**DeepMine** is not just a scraper; it is an autonomous **Lead Generation Engine**. It completely automates the pipeline from finding a raw company URL to building a deep, multi-relational database of its products, infrastructure, and direct contact management team. By combining **robot-aware Breadth-First Search (BFS) crawling**, **proxy-rotated headless browser automation**, and **cutting-edge LLM context caching**, DeepMine provides sales and marketing teams with enterprise-level, highly enriched lead lists at a fraction of traditional API costs.

### 📈 Scale & Lead Generation Metrics
In its latest production run, DeepMine autonomously processed and structured massive volumes of actionable B2B leads:
- **9,200+** Corporate Entities Enriched
- **364,000+** Unique Products & Services Catalogued
- **12,800+** C-Suite & Management Records Extracted (Direct Outbound Targets)
- **14,600+** Verified Geographical Addresses
- **7.8M+** web pages crawled and dynamically analyzed for lead potential

---

## 🏗️ Advanced System Architecture

The pipeline is heavily distributed, utilizing asynchronous workers and multi-stage triage to ensure only high-value lead data reaches the expensive extraction layers.

```mermaid
graph TD
    %% Core Inputs
    A[(Raw Excel Domain Master List)] -->|Domain Normalization| B[Deep Crawler Engine]
    
    %% Crawler Subsystem
    subgraph Crawler [1. Asynchronous Deep Crawl Subsystem]
        B -->|Async HTTP requests| C{Robots.txt Enforcer}
        C -->|Valid| D[MIME/Type Filtering]
        D -->|Valid HTML| E[Grok-4-Fast AI Lead Triage]
        E -->|High-Value Targets| F[(Cleaned Pages Buffer)]
        E -.->|Dropped Noise| Void[Void]
    end

    %% Extraction Subsystem
    subgraph Extraction [2. AI Extraction & Caching Subsystem]
        F --> G{Page Payload Size Routing}
        G -->|Payload > 10 Pages| H[Gemini Batch API]
        G -->|Payload <= 10 Pages| I[GLM-4 Flash API Realtime]
        
        H -->|Context Caching 72h TTL| J[JSON Schema Validation]
        I --> J
        J -->|Auto-Repair on fail| K[(Structured Lead JSON Sink)]
    end

    %% Enrichment Subsystem
    subgraph Enrichment [3. OSINT Playwright Enrichment]
        K --> L[Manager Orchestrator]
        L --> M[Google SERP Scraper]
        L --> N[LinkedIn Scraper]
        L --> O[TheCompanyCheck / Tofler APIs]
        
        %% Stealth features
        M -.->|Decodo ISP Proxies + Stealth| Web((External Web))
        N -.->|Rotating User-Agents| Web
        O -.->|10x Concurrent Contexts| Web
        Web --> P[Enrichment Payload]
    end

    %% Output
    P --> Q[Data Merger & Upsert Engine]
    K --> Q
    Q -->|Relational Mapping| R[(PostgreSQL Core Leads DB)]
    Q -->|Direct Export| S[final.xlsx Master Sales List]

    classDef default fill:#f9f9f9,stroke:#333,stroke-width:2px;
    classDef ai fill:#e1bee7,stroke:#8e24aa,stroke-width:2px;
    classDef db fill:#bbdefb,stroke:#1976d2,stroke-width:2px;
    
    class E,H,I ai;
    class A,F,K,R,S db;
```

---

## 🛠️ Core Engineering Features

### 1. Dual-Path LLM Extraction Engine
To massively reduce operational costs while generating leads, the system routes tasks dynamically:
- **Massive sites (10+ pages)**: Sent to **Gemini Batch API**. We utilize Gemini's Context Caching (72-hour TTL) to process thousands of tokens for pennies, enforcing strict JSON Schema compliance for our outbound lead format.
- **Micro-sites (<=10 pages)**: Sent directly to **GLM-4-Flash** for real-time extraction, bypassing the 24-hour SLA of batch processing.

### 2. AI-Driven Lead Triage (Grok-4)
Instead of blindly traversing domains and accumulating junk data, DeepMine feeds a domain's homepage link tree to **Grok-4-Fast**. The model acts as a highly intelligent heuristic agent, deterministically selecting only links related to products, services, contact info, and management (the core requirements for a qualified B2B lead), slashing subsequent crawl volume by 80%.

### 3. Asynchronous Playwright OSINT Suite
The enrichment suite handles extreme anti-bot environments (LinkedIn, Tofler, Google Maps) to pull decision-maker profiles using:
- **Decodo ISP Residential Proxies** with aggressive rotation logic.
- **Concurrent Contexts**: Utilizing up to 10 isolated `asyncio` browser contexts simultaneously.
- **Playwright Stealth**: Evading modern canvas fingerprinting, headless detection, and WebDriver flags to scrape pristine lead data.

### 4. Robust Database Upserts
The Data Merger assigns unique provenance hashes to extracted contact data, gracefully handling deduplication, merging LLM outputs with scraped JSON-LD schema data, and performing bulk PostgreSQL `UPSERT` operations to maintain a pristine CRM/Lead database.

---

## 💻 Tech Stack Deep Dive

| Component | Technology | Role |
|-----------|------------|------|
| **Language** | Python 3.11+ | Core engine, concurrency (`asyncio`) |
| **Parsing** | BeautifulSoup4, Playwright | DOM parsing, JavaScript rendering, Bot evasion |
| **Intelligence** | Gemini 2.0, Grok-4, GLM-4 | Semantic triage, highly structured JSON extraction |
| **Data Layer** | PostgreSQL, Pandas | Relational integrity, high-speed CSV/Excel serialization |
| **Infra** | Tqdm, RotatingFileHandler | Advanced CLI progress tracking, log rotation |

---

## 🚀 Installation & Usage

1. **Clone the repository:**
   ```bash
   git clone https://github.com/7AI7/DeepMine.git
   cd DeepMine
   ```

2. **Install Python requirements:**
   *(Ensure you are using a virtual environment)*
   ```bash
   pip install -r requirements.txt
   ```

3. **Install Browser Binaries:**
   Required for the Playwright enrichment pipelines.
   ```bash
   playwright install chromium
   ```

4. **Environment Configuration:**
   Copy the example environment file and populate it with your provider API keys and proxy credentials.
   ```bash
   cp .env.example .env
   ```

5. **Execution:**
   Initialize the orchestrator from either Excel lead seeds or the PostgreSQL connector.
   ```bash
   python deep_crawler/orchestrator.py --mode complete
   ```

---
*Architected and Deployed by JAI J — AI Applications & Automation Engineer*
