from contextlib import asynccontextmanager
import os
import json
from typing import Optional, List

from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
from sentence_transformers import SentenceTransformer
import uvicorn
from google import genai
from google.genai import types
from dotenv import load_dotenv

from app_logger import get_logger

load_dotenv()
logger = get_logger("ml-service")

api_key = os.getenv("GEMINI_API_KEY")
model_name = os.getenv("CLASSIFIER_MODEL", "gemini-2.5-flash")  # note: no "models/" prefix in new SDK

if api_key:
    client = genai.Client(api_key=api_key)
    logger.info(f"Gemini configured with model: {model_name}")
else:
    client = None
    logger.warning("GEMINI_API_KEY not found in environment")

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Loading SentenceTransformer model: all-MiniLM-L6-v2")
    try:
        app.state.embedder = SentenceTransformer("all-MiniLM-L6-v2")
        logger.info("ML models loaded successfully")
    except Exception as e:
        logger.error(f"Failed to load sentence-transformer: {str(e)}")
    yield
    logger.info("Shutting down ML service")

app = FastAPI(lifespan=lifespan)

class TextRequest(BaseModel):
    text: str

class ChatIntentRequest(BaseModel):
    text: str
    user_id: str

class ChatSummarizeRequest(BaseModel):
    user_query: str
    context_data: str
    user_id: str

@app.post("/embed")
async def get_embed(payload: TextRequest, request: Request):
    logger.debug(f"Embedding request received: {payload.text[:100]}")
    embedder = request.app.state.embedder
    try:
        embedding_vector = embedder.encode(payload.text)
        embedding_list = [float(val) for val in embedding_vector.tolist()]
        return {"embedding": embedding_list}
    except Exception as exc:
        logger.error(f"Embedding generation failed: {str(exc)}")
        raise HTTPException(status_code=500, detail=f"Embedding generation failed: {str(exc)}")

@app.post("/chat/intent")
async def get_intent(payload: ChatIntentRequest):
    if not client:
        raise HTTPException(status_code=500, detail="Gemini API not configured")

    prompt = f"""
    You are a financial intent classifier for LedgerBuddy.
    Analyze the user's query and categorize it into one of these intents:
    - SPENDING_SUMMARY: Asking for general spending totals or breakdown.
    - ANOMALY_DETECTION: Asking about spikes, unusual transactions, or "money leaking".
    - COMPARISON: Comparing different months or timeframes.
    - BUDGET_ADVICE: Asking if it's safe to spend or budget tips.
    - UI_HELP: Questions about how to use the LedgerAI platform (uploading, QC, dashboard navigation).
    - GENERAL: Greeting, polite talk, or queries that don't fit above.

    User Query: "{payload.text}"

    Return ONLY a JSON object: 
    {{
        "intent": "INTENT_NAME",
        "params": {{ "timeframe": "relative_time", "category": "optional", "topic": "optional" }}
    }}
    """

    try:
        response = client.models.generate_content(
            model=model_name,
            contents=prompt
        )
        raw_text = response.text.strip()
        if "```json" in raw_text:
            raw_text = raw_text.split("```json")[1].split("```")[0].strip()
        elif "```" in raw_text:
            raw_text = raw_text.split("```")[1].split("```")[0].strip()
        return json.loads(raw_text)
    except Exception as e:
        logger.error(f"Intent classification failed: {str(e)}")
        return {"intent": "GENERAL", "params": {}}

@app.post("/chat/summarize")
async def summarize_context(payload: ChatSummarizeRequest):
    if not client:
        raise HTTPException(status_code=500, detail="Gemini API not configured")

    system_prompt = f"""You are "LedgerBuddy", an expert AI financial assistant for an Indian expense tracking and banking platform called LedgerAI.

User Data Context (Financial Summary):
{payload.context_data}

Behavioral Rules:
1. ALWAYS use the "₹" symbol for money. Never use "$" or "USD".
2. Use the Indian numbering system (e.g., 1,00,000 for 1 Lakh) where appropriate.
3. If the user asks a financial question, use ONLY the numbers in the JSON context provided above.
4. If data is missing, say: "I don't have category-level details yet, but I can see your total spending is ₹..."
5. For platform questions, refer users to Parsing, Transactions, or Review pages.
6. Responses must be very concise (2-4 sentences max) and helpful.
7. Do not give legal or official investment advice.
"""

    try:
        response = client.models.generate_content(
            model=model_name,
            contents=f"Instruction: {system_prompt}\nUser Question: {payload.user_query}"
        )
        return {"text": response.text}
    except Exception as e:
        logger.error(f"Narrative generation failed: {str(e)}")
        return {"text": "I'm experiencing a bit of a brain-freeze! Could you please ask that again?"}

@app.get("/health")
async def health():
    return {"status": "ok", "model": model_name}

if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    logger.info(f"Starting ML service on port {port}")
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)