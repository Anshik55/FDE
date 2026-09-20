# AI Agent for Client Data Migration & Integration

An autonomous AI agent designed to ingest, map, clean, and validate legacy HR/CRM data into a standardized schema. Built to operate autonomously while surfacing ambiguous decisions to a human supervisor through a clean web UI.

## Overview
This project tackles the challenge of messy client data migrations. It uses an LLM solely for its strengths (fuzzy matching and semantic mapping) while relying on robust, deterministic Python code for the actual data pipeline, validation, and pushing to the target API. 

A central **Event Log** architecture ensures every decision is recorded, providing a complete audit trail out-of-the-box.

## Technology Stack
* **Backend**: Python 3.10+, FastAPI
* **Database**: SQLite (acting as the event store and state database)
* **Real-time Comms**: Server-Sent Events (SSE)
* **Frontend**: React with Tailwind CSS (for a modern, responsive interface)
* **AI Runtime**: Ollama (Running open-source models like Llama 3 or Qwen locally)
* **Data Processing**: Pandas

## Core Design Principles
1. **Deterministic Guardrails**: The LLM proposes; the code disposes. 
2. **Defensible Escalations**: Escalate at the highest level of abstraction (e.g., escalating an ambiguous column mapping fixes 1,000 rows at once).
3. **Volume Circuit Breakers**: If the escalation rate exceeds 15%, the mapping itself is deemed faulty and the run halts.
4. **Sensitive Field Carve-outs**: Fields like "Salary" or "National ID" are hard-coded to require human approval, regardless of the LLM's confidence score.

## Setup Instructions
*(To be completed during implementation)*

### Prerequisites
- Python 3.10+
- Ollama installed locally
- Node.js / npm 

### Installation
1. Clone the repository.
2. Install python dependencies: `pip install -r requirements.txt`
3. Start the Ollama server: `ollama run llama3`
4. Start the backend: `uvicorn main:app --reload`
5. Start the frontend development server: `npm run dev`

## Project Structure
*(To be populated as phases are completed)*
- `/docs`: Project documentation and decisions.
- `/backend`: FastAPI application, event log, and LLM integrations.
- `/frontend`: Human-in-the-loop web UI.
- `/data`: Sample input fixtures and target schema definitions.
