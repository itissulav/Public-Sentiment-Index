# 🌐 Public Sentiment Index (PSI)

> A real-time sentiment measurement system that analyzes public perception across social media platforms — converting online discussions into a quantifiable index that reflects how society feels about any topic, product, or public figure.

---

## 📌 Overview

The **Public Sentiment Index (PSI)** bridges the gap between fragmented public opinion data and actionable insight. Using Transformer-based NLP models and an ELO-inspired rating system, PSI scrapes Reddit discussions, classifies sentiment at scale, and renders the results through an interactive analytics dashboard.

### The Problem
- No trusted, unified system to measure the public mood
- Inconsistent and fragmented data on citizens' reactions to topics
- Limited real-time access to sentiment trends
- Social media algorithms distorting the view of public opinion

### The Solution
PSI provides a **single, structured sentiment score** for any topic by:
- Aggregating social media comments from Reddit
- Running multi-class sentiment classification via Hugging Face Transformers
- Storing results in a relational database (Supabase / PostgreSQL)
- Visualizing trends through an interactive web dashboard

---

## 🎯 Who Is It For?

| Audience | Use Case |
|---|---|
| 🏦 Government | Understand citizens' emotions on national issues |
| ⚖️ Policy Makers | Evidence-based data for better representation |
| 🗳️ Voters | Aggregated opinions for informed decisions |
| 🌍 NGOs & INGOs | Identify focal problem areas for target audiences |
| 🎓 Students & Youth | Stay informed on brands, people, and controversies |

---

## 🛠️ Tech Stack

| Layer | Technology |
|---|---|
| **Backend** | Python, Flask |
| **Frontend** | HTML, Vanilla CSS, JavaScript, Chart.js |
| **Database** | Supabase (PostgreSQL) |
| **Auth** | Supabase Auth (JWT) |
| **NLP / AI** | Hugging Face Inference API (Transformer models) |
| **Data Source** | Reddit API (PRAW) |
| **Sentiment Scoring** | VADER + custom ELO-style rating |
| **Templating** | Jinja2 |
| **Env Management** | python-dotenv |

---

## 🗂️ Project Structure

```
Public-Sentiment-Index/
│
├── app/
│   ├── controllers/          # Flask route handlers (main, auth)
│   ├── models/               # OOP models (User, Topic)
│   ├── services/             # Supabase client, auth service, admin service
│   ├── utils/                # Reddit fetcher, HF analyzer, visualizer, scheduler
│   └── templates/            # Jinja2 HTML templates
│
├── static/
│   ├── css/                  # Per-page stylesheets
│   ├── images/               # Topic thumbnail images
│   └── analysed/             # CSV exports (gitignored)
│
├── app.py                    # Application entry point
├── config.py                 # Flask config
├── run_weekly_job.py         # Scheduled sentiment refresh job
├── requirements.txt          # Python dependencies
└── .env                      # Environment variables (gitignored)
```

---

## ⚙️ Setup & Installation

### 1. Clone the Repository
```bash
git clone https://github.com/itissulav/Public-Sentiment-Index.git
cd Public-Sentiment-Index
```

### 2. Create a Virtual Environment
```bash
python -m venv .venv
.venv\Scripts\activate        # Windows
source .venv/bin/activate     # macOS/Linux
```

### 3. Install Dependencies
```bash
pip install -r requirements.txt
```

### 4. Configure Environment Variables

Create a `.env` file in the root directory:
```env
SUPABASE_URL=your_supabase_project_url
SUPABASE_ANON_KEY=your_supabase_anon_key
SUPABASE_SERVICE_KEY=your_supabase_service_role_key
SECRET_KEY=your_flask_secret_key
HF_API_KEY=your_huggingface_api_key
REDDIT_CLIENT_ID=your_reddit_client_id
REDDIT_CLIENT_SECRET=your_reddit_client_secret
REDDIT_USER_AGENT=your_reddit_user_agent
```

### 5. Run the App
```bash
python app.py
```

Visit `http://localhost:5000` in your browser.

---

## 🚀 Key Features

- **🔍 Live Topic Search** — Search any topic; PSI scrapes Reddit in real-time, runs AI sentiment analysis, and stores results
- **📊 Trend Visualizations** — 15+ interactive Chart.js visualizations per topic (sentiment distribution, confidence scores, viral comments, etc.)
- **⭐ Trending Section** — ELO-style rating scores for top topics displayed on the home page
- **🛡️ Admin Dashboard** — Manage users and topics, trigger sentiment reruns, view platform-wide statistics
- **🔒 Auth System** — Secure login/register via Supabase Auth with role-based access (User / Admin)
- **⏱️ Weekly Job Scheduler** — Automated background refresh of tracked topics

---

## 📐 Database Schema

| Table | Purpose |
|---|---|
| `Users` | User profiles linked to Supabase Auth |
| `Topic` | Admin-managed tracked topics |
| `search_topics` | Auto-generated topic entries from live searches |
| `reddit_comments` | Scraped and analyzed Reddit comments |
| `topic_comments` | Analyzed comments linked to tracked topics |

---

## 🧠 How the Sentiment Pipeline Works

```
Reddit API  →  Raw Comments  →  Hugging Face (Transformer)
                                        ↓
                            Sentiment Label + Confidence Score
                                        ↓
                              Supabase (reddit_comments)
                                        ↓
                         ELO Rating Calculation (analyzer.py)
                                        ↓
                            Dashboard Visualizations
```

---

## 📄 License

This project is developed as a Final Year Project (FYP). All rights reserved.
