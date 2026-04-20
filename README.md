# LedgerAI — Intelligent Bank Statement Processor

**LedgerAI** is a multi-service platform designed to ingest, parse, and categorize bank statements with high precision. By combining **Rule-matching**, **Machine Learning**, and **LLMs (Google Gemini, Claude Sonnet 4.5)**, it transforms raw PDF data into organized financial insights.

## 🚀 Key Features

* **Automated PDF Extraction**: Automatically detects bank formats (e.g., SBI, HDFC, ICICI) and extracts transaction data.
* **AI Categorization**: Uses specialized ML models to classify transactions into categories like Food, Rent, and Shopping.
* **Interactive Review**: A dedicated UI to review AI-extracted data and manually correct categories if needed.
* **Financial Analytics**: Visual charts for spending trends, income vs. expense ratios, and monthly breakdowns.
* **Account Management**: Manually add or connect accounts to specific parsed documents to track balances.

---

## 🏗 Architecture Overview

The system is composed of four primary services that work in tandem:

| Component | Technology | Port | Purpose |
| :--- | :--- | :--- | :--- |
| **Main Backend** | Node.js / Express | 3000 | Orchestration, user accounts, and categorization workflow. |
| **ML Microservice** | Python / MiniLM | 5000 | Specialized transaction classification via Sentence-Transformers. |
| **Parser Backend** | Python / FastAPI | 8000 | PDF text extraction and LLM-based format identification. |
| **Web Frontend** | React / Vite | 5173 | Premium UI for document uploads and result reviewing. |

---

## 🛠 Installation & Setup

### 1. Prerequisites
* **Node.js**: v18+
* **Python**: 3.9+
* **Accounts**: Supabase (Database + Auth) and a OpenRouter API Key

### 2. Setup Commands

#### **A. Main Backend (Node.js)**
```bash
cd backend/
npm install
npm run dev
```

#### **B. ML Microservice (Python)**
```bash
cd ml-service/
python3.12 -m venv .venv

# Activate Virtual Env:
source .venv/bin/activate       # Linux/macOS
.venv\Scripts\activate          # Windows

pip install -r requirements.txt
python3 main.py
```

#### **C. Parser Backend (Python)**
```bash
cd parser_backend/
python3.12 -m venv .venv

# Activate Virtual Env:
source .venv/bin/activate       # Linux/macOS
.venv\Scripts\activate          # Windows

pip install -r requirements.txt
uvicorn main:app --reload --port 8000
```

#### **D. Web Frontend (React)**
```bash
cd frontend-web/
npm install
npm run dev
```
---

## 📂 Project Structure

```text
├── backend/            # Express.js API (Orchestration)
├── ml-service/         # Python ML (Classification)
├── parser_backend/    # Python FastAPI (LLM Extraction)
├── frontend-web/       # React + Vite UI
└── schema.sql          # Supabase Database Structure
```

---

## 💡 Troubleshooting & Security

* **Environment Variables**: Each service requires a `.env` file. Ensure `VITE_API_BASE_URL` in your frontend points to your active backend.
* **PDF Detection**: Ensure PDFs are text-based bank statements; scanned images will not be parsed correctly.
* **CORS Errors**: If connection fails, update `ALLOW_ORIGINS` in the backend to include your frontend URL.
* **Security**: Enable **Row Level Security (RLS)** in Supabase to protect user data.