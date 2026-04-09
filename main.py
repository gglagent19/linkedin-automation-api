"""
FastAPI backend for the LinkedIn automation hybrid SaaS.
Handles task queue, user auth, and activity log.
"""

import json
import os
from typing import Optional

from fastapi import FastAPI, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from models import get_db, init_db, generate_id, generate_api_key, now_iso
from auth import (
    hash_password,
    verify_password,
    create_access_token,
    get_current_user,
)

app = FastAPI(title="LinkedIn Automation API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
def startup():
    init_db()


# ── Schemas ──────────────────────────────────────────────────────

class RegisterRequest(BaseModel):
    email: str
    password: str

class LoginRequest(BaseModel):
    email: str
    password: str

class TaskCreateRequest(BaseModel):
    action: str
    payload: dict = {}

class TaskUpdateRequest(BaseModel):
    status: str
    result: str = ""

class ChatRequest(BaseModel):
    message: str

class OnboardingRequest(BaseModel):
    goal: str
    product_name: str = ""
    product_description: str = ""
    target_titles: list[str] = []
    target_industries: list[str] = []
    company_size_min: int = 50
    company_size_max: int = 500
    revenue_min: str = "5000000"
    revenue_max: str = "100000000"

class ResearchRequest(BaseModel):
    website: str
    goal: str

class ApolloKeyRequest(BaseModel):
    apollo_api_key: str

class InstantlyKeyRequest(BaseModel):
    instantly_api_key: str
    instantly_campaign_id: str = ""

class ConfigUpdateRequest(BaseModel):
    goal: str | None = None
    product_name: str | None = None
    product_description: str | None = None
    target_titles: list[str] | None = None
    target_industries: list[str] | None = None
    company_size_min: int | None = None
    company_size_max: int | None = None
    revenue_min: str | None = None
    revenue_max: str | None = None
    daily_schedule: list[dict] | None = None
    automations: list[dict] | None = None
    safety_rules: list[str] | None = None
    phase: int | None = None


# ── Auth Routes ──────────────────────────────────────────────────

@app.post("/auth/register")
def register(req: RegisterRequest):
    conn = get_db()
    existing = conn.execute("SELECT id FROM users WHERE email = ?", (req.email,)).fetchone()
    if existing:
        conn.close()
        raise HTTPException(status_code=400, detail="Email already registered")

    user_id = generate_id()
    api_key = generate_api_key()
    conn.execute(
        "INSERT INTO users (id, email, password_hash, api_key, created_at) VALUES (?, ?, ?, ?, ?)",
        (user_id, req.email, hash_password(req.password), api_key, now_iso()),
    )
    conn.commit()
    conn.close()

    return {
        "user_id": user_id,
        "api_key": api_key,
        "token": create_access_token(user_id),
    }


@app.post("/auth/login")
def login(req: LoginRequest):
    conn = get_db()
    row = conn.execute("SELECT * FROM users WHERE email = ?", (req.email,)).fetchone()
    conn.close()

    if not row or not verify_password(req.password, row["password_hash"]):
        raise HTTPException(status_code=401, detail="Invalid credentials")

    return {
        "user_id": row["id"],
        "api_key": row["api_key"],
        "token": create_access_token(row["id"]),
    }


# ── Task Routes ──────────────────────────────────────────────────

@app.post("/tasks")
def create_task(req: TaskCreateRequest, user=Depends(get_current_user)):
    conn = get_db()
    task_id = generate_id()
    now = now_iso()
    conn.execute(
        "INSERT INTO tasks (id, user_id, action, payload, status, created_at, updated_at) VALUES (?, ?, ?, ?, 'pending', ?, ?)",
        (task_id, user["id"], req.action, json.dumps(req.payload), now, now),
    )
    conn.commit()
    conn.close()
    return {"task_id": task_id, "status": "pending"}


@app.get("/tasks")
def list_tasks(
    status: Optional[str] = None,
    agent_id: Optional[str] = None,
    user=Depends(get_current_user),
):
    """List tasks. Agents poll this with status=pending to pick up work."""
    conn = get_db()
    query = "SELECT * FROM tasks WHERE user_id = ?"
    params = [user["id"]]

    if status:
        query += " AND status = ?"
        params.append(status)
    if agent_id:
        query += " AND (agent_id = ? OR agent_id IS NULL)"
        params.append(agent_id)

    query += " ORDER BY created_at DESC LIMIT 100"
    rows = conn.execute(query, params).fetchall()
    conn.close()

    return {"tasks": [dict(r) for r in rows]}


@app.get("/tasks/{task_id}")
def get_task(task_id: str, user=Depends(get_current_user)):
    conn = get_db()
    row = conn.execute(
        "SELECT * FROM tasks WHERE id = ? AND user_id = ?", (task_id, user["id"])
    ).fetchone()
    conn.close()
    if not row:
        raise HTTPException(status_code=404, detail="Task not found")
    return dict(row)


@app.post("/tasks/{task_id}/claim")
def claim_task(task_id: str, agent_id: str, user=Depends(get_current_user)):
    """Agent claims a pending task so no other agent picks it up."""
    conn = get_db()
    result = conn.execute(
        "UPDATE tasks SET agent_id = ?, status = 'running', updated_at = ? WHERE id = ? AND user_id = ? AND status = 'pending'",
        (agent_id, now_iso(), task_id, user["id"]),
    )
    conn.commit()
    conn.close()
    if result.rowcount == 0:
        raise HTTPException(status_code=409, detail="Task already claimed or not found")
    return {"status": "running"}


@app.post("/tasks/{task_id}/complete")
def complete_task(task_id: str, req: TaskUpdateRequest, user=Depends(get_current_user)):
    conn = get_db()
    result = conn.execute(
        "UPDATE tasks SET status = ?, result = ?, updated_at = ? WHERE id = ? AND user_id = ?",
        (req.status, req.result, now_iso(), task_id, user["id"]),
    )
    conn.commit()
    conn.close()
    if result.rowcount == 0:
        raise HTTPException(status_code=404, detail="Task not found")
    return {"status": req.status}


# ── Activity Log ─────────────────────────────────────────────────

@app.get("/activity")
def activity_log(limit: int = 50, user=Depends(get_current_user)):
    """Returns recent tasks as an activity feed for the dashboard."""
    conn = get_db()
    rows = conn.execute(
        "SELECT id, action, status, result, created_at, updated_at FROM tasks WHERE user_id = ? ORDER BY updated_at DESC LIMIT ?",
        (user["id"], limit),
    ).fetchall()
    conn.close()
    return {"activity": [dict(r) for r in rows]}


# ── Chat ─────────────────────────────────────────────────────────

COMMAND_MAP = {
    "start the work": ["scrape_apollo", "target_leads", "run_daily_loop"],
    "start work": ["scrape_apollo", "target_leads", "run_daily_loop"],
    "start": ["run_daily_loop"],
    "scrape leads": ["scrape_apollo"],
    "scrape apollo": ["scrape_apollo"],
    "find leads": ["scrape_apollo"],
    "get leads": ["scrape_apollo"],
    "target leads": ["target_leads"],
    "target them": ["target_leads"],
    "send connections": ["send_connection_request"],
    "connect": ["send_connection_request"],
    "send emails": ["send_emails"],
    "email them": ["send_emails"],
    "write a post": ["generate_post"],
    "generate post": ["generate_post"],
    "create post": ["generate_post"],
    "post something": ["generate_post"],
    "reply comments": ["reply_comments"],
    "reply to comments": ["reply_comments"],
    "view profiles": ["view_profiles"],
    "warm up": ["view_profiles"],
    "warmup": ["view_profiles"],
    "book meetings": ["run_daily_loop"],
    "run daily": ["run_daily_loop"],
    "run everything": ["scrape_apollo", "target_leads", "run_daily_loop"],
    "do everything": ["scrape_apollo", "target_leads", "run_daily_loop"],
    "collect leads": ["collect_leads"],
}


def parse_commands(message: str) -> list[str]:
    """Parse natural language message into automation actions."""
    msg = message.lower().strip()

    # Exact match first
    if msg in COMMAND_MAP:
        return COMMAND_MAP[msg]

    # Fuzzy match -- check if any command phrase is contained in the message
    matched = []
    for phrase, actions in COMMAND_MAP.items():
        if phrase in msg:
            for a in actions:
                if a not in matched:
                    matched.append(a)

    return matched


@app.post("/chat")
def chat(req: ChatRequest, user=Depends(get_current_user)):
    conn = get_db()
    now = now_iso()

    # Save user message
    user_msg_id = generate_id()
    conn.execute(
        "INSERT INTO chat_messages (id, user_id, role, content, created_at) VALUES (?, ?, 'user', ?, ?)",
        (user_msg_id, user["id"], req.message, now),
    )

    # Parse commands from the message
    actions = parse_commands(req.message)

    if actions:
        # Create tasks for each action
        task_ids = []
        for action in actions:
            task_id = generate_id()
            conn.execute(
                "INSERT INTO tasks (id, user_id, action, payload, status, created_at, updated_at) VALUES (?, ?, ?, '{}', 'pending', ?, ?)",
                (task_id, user["id"], action, now, now),
            )
            task_ids.append(task_id)

        action_names = [a.replace("_", " ") for a in actions]
        reply = f"Got it! I'm starting: {', '.join(action_names)}. {len(task_ids)} task(s) queued. Your agent will pick them up shortly."
    else:
        reply = (
            "I can help you with these commands:\n"
            "- \"start the work\" - runs the full daily pipeline\n"
            "- \"scrape leads\" - find new leads from Apollo\n"
            "- \"target leads\" - run targeting on scraped leads\n"
            "- \"send connections\" - send LinkedIn connection requests\n"
            "- \"send emails\" - send cold emails\n"
            "- \"write a post\" - generate a LinkedIn post\n"
            "- \"reply comments\" - reply to post comments\n"
            "- \"view profiles\" - warm up by viewing profiles\n"
            "- \"do everything\" - scrape, target, and run daily loop\n"
            "\nTry saying something like \"start the work\" or \"scrape leads and send connections\"."
        )

    # Save assistant reply
    reply_id = generate_id()
    conn.execute(
        "INSERT INTO chat_messages (id, user_id, role, content, created_at) VALUES (?, ?, 'assistant', ?, ?)",
        (reply_id, user["id"], reply, now),
    )

    conn.commit()
    conn.close()

    return {
        "reply": reply,
        "actions_triggered": actions,
        "task_count": len(actions),
    }


@app.get("/chat/history")
def chat_history(limit: int = 50, user=Depends(get_current_user)):
    conn = get_db()
    rows = conn.execute(
        "SELECT id, role, content, created_at FROM chat_messages WHERE user_id = ? ORDER BY created_at ASC LIMIT ?",
        (user["id"], limit),
    ).fetchall()
    conn.close()
    return {"messages": [dict(r) for r in rows]}


# ── Research Agent ────────────────────────────────────────────────

def scrape_website(url: str) -> dict:
    """Scrape a website and extract product/company info."""
    import requests as http_requests
    import re

    if not url.startswith("http"):
        url = "https://" + url

    try:
        resp = http_requests.get(url, timeout=15, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        })
        html = resp.text[:50000]  # limit to 50k chars

        # Extract title
        title_match = re.search(r"<title[^>]*>(.*?)</title>", html, re.IGNORECASE | re.DOTALL)
        title = title_match.group(1).strip() if title_match else ""

        # Extract meta description
        desc_match = re.search(r'<meta[^>]*name=["\']description["\'][^>]*content=["\'](.*?)["\']', html, re.IGNORECASE)
        if not desc_match:
            desc_match = re.search(r'<meta[^>]*content=["\'](.*?)["\'][^>]*name=["\']description["\']', html, re.IGNORECASE)
        description = desc_match.group(1).strip() if desc_match else ""

        # Extract h1s and h2s for understanding the product
        h1s = re.findall(r"<h1[^>]*>(.*?)</h1>", html, re.IGNORECASE | re.DOTALL)
        h2s = re.findall(r"<h2[^>]*>(.*?)</h2>", html, re.IGNORECASE | re.DOTALL)

        # Clean HTML tags from headings
        def clean(text):
            return re.sub(r"<[^>]+>", "", text).strip()

        h1s = [clean(h) for h in h1s[:5] if clean(h)]
        h2s = [clean(h) for h in h2s[:10] if clean(h)]

        # Extract visible text snippets (paragraphs)
        paragraphs = re.findall(r"<p[^>]*>(.*?)</p>", html, re.IGNORECASE | re.DOTALL)
        paragraphs = [clean(p) for p in paragraphs if len(clean(p)) > 30][:10]

        # Try to find company name from domain
        from urllib.parse import urlparse
        domain = urlparse(url).netloc.replace("www.", "")
        company_name = domain.split(".")[0].capitalize()

        return {
            "url": url,
            "company_name": company_name,
            "title": title,
            "description": description,
            "headings": h1s + h2s,
            "key_paragraphs": paragraphs,
            "scraped": True,
        }
    except Exception as e:
        return {"url": url, "scraped": False, "error": str(e)}


def generate_strategy(website_data: dict, goal: str) -> dict:
    """Generate ICP, strategy, automations based on website research and goal."""

    company = website_data.get("company_name", "")
    description = website_data.get("description", "")
    headings = website_data.get("headings", [])
    paragraphs = website_data.get("key_paragraphs", [])

    # Combine all text for analysis
    all_text = f"{description} {' '.join(headings)} {' '.join(paragraphs)}".lower()

    # Detect product category
    product_category = "B2B Software"
    category_signals = {
        "partner": "Partner/Channel Management",
        "channel": "Partner/Channel Management",
        "alliance": "Partner/Channel Management",
        "crm": "CRM/Sales",
        "sales": "Sales Enablement",
        "marketing": "Marketing Platform",
        "security": "Cybersecurity",
        "cyber": "Cybersecurity",
        "cloud": "Cloud Infrastructure",
        "hr": "HR Technology",
        "recruit": "HR Technology",
        "finance": "Financial Technology",
        "payment": "Financial Technology",
        "analytics": "Data & Analytics",
        "data": "Data & Analytics",
        "ai": "AI/ML Platform",
        "automat": "Automation Platform",
        "ecommerce": "E-commerce",
        "health": "HealthTech",
        "educ": "EdTech",
        "learn": "EdTech",
    }
    for signal, category in category_signals.items():
        if signal in all_text:
            product_category = category
            break

    # Generate ICP titles based on category and goal
    title_map = {
        "Partner/Channel Management": ["VP of Partnerships", "VP of Channel Sales", "VP of Alliances", "Director of Partner Programs", "Head of Channel", "Director of Business Development", "Chief Revenue Officer", "SVP Partnerships"],
        "CRM/Sales": ["VP of Sales", "Head of Sales Operations", "Director of Revenue Operations", "Chief Revenue Officer", "VP of Business Development", "Sales Director"],
        "Sales Enablement": ["VP of Sales", "VP of Sales Enablement", "Head of Revenue Operations", "Director of Sales", "Chief Revenue Officer"],
        "Marketing Platform": ["VP of Marketing", "CMO", "Head of Digital Marketing", "Director of Demand Gen", "Head of Growth", "Director of Marketing"],
        "Cybersecurity": ["CISO", "VP of Security", "Head of IT Security", "Director of InfoSec", "CTO", "VP of Engineering"],
        "Cloud Infrastructure": ["CTO", "VP of Engineering", "Head of DevOps", "Director of Infrastructure", "VP of IT"],
        "HR Technology": ["VP of HR", "CHRO", "Head of People Operations", "Director of Talent", "VP of People"],
        "Financial Technology": ["CFO", "VP of Finance", "Head of Treasury", "Director of Financial Operations"],
        "Data & Analytics": ["Chief Data Officer", "VP of Analytics", "Head of Data", "Director of BI", "VP of Engineering"],
        "AI/ML Platform": ["CTO", "VP of Engineering", "Head of AI/ML", "Chief Data Officer", "VP of Product"],
        "Automation Platform": ["COO", "VP of Operations", "Head of Process Automation", "Director of Digital Transformation"],
        "E-commerce": ["VP of E-commerce", "Head of Digital", "Director of Online Sales", "CMO"],
        "HealthTech": ["CTO", "VP of Product", "Chief Medical Officer", "Head of Digital Health"],
        "EdTech": ["VP of Product", "Head of Curriculum", "Director of EdTech", "CTO"],
    }
    target_titles = title_map.get(product_category, ["VP of Sales", "CTO", "Director of Business Development", "Head of Growth", "CMO", "COO"])

    # Generate industries based on product
    industry_map = {
        "Partner/Channel Management": ["SaaS", "Cybersecurity", "Cloud Infrastructure", "IT Services", "MarTech", "HR Tech", "FinTech"],
        "Cybersecurity": ["Financial Services", "Healthcare", "Government", "Technology", "Retail", "Manufacturing"],
        "Marketing Platform": ["SaaS", "E-commerce", "Retail", "Media", "Technology", "Consumer Brands"],
        "HR Technology": ["Technology", "Financial Services", "Healthcare", "Retail", "Manufacturing", "Professional Services"],
    }
    target_industries = industry_map.get(product_category, ["SaaS", "Technology", "Financial Services", "Healthcare", "Professional Services", "Manufacturing", "Retail"])

    # Generate strategy based on goal
    goal_lower = goal.lower()
    if "demo" in goal_lower or "book" in goal_lower:
        strategy_focus = "demo bookings"
        outreach_tone = "Value-first, offer a quick 15-min demo"
    elif "client" in goal_lower or "customer" in goal_lower:
        strategy_focus = "client acquisition"
        outreach_tone = "Consultative, focus on solving their pain points"
    elif "partner" in goal_lower:
        strategy_focus = "partnership development"
        outreach_tone = "Collaborative, mutual value proposition"
    elif "sale" in goal_lower or "revenue" in goal_lower:
        strategy_focus = "revenue generation"
        outreach_tone = "ROI-focused, data-driven value prop"
    else:
        strategy_focus = goal
        outreach_tone = "Professional, personalized, value-first"

    # Build automations customized to their product
    automations = [
        {"name": "Scrape Leads from Apollo", "role": "Lead Scraper", "description": f"Searches Apollo for {product_category} buyers matching your ICP. Pulls LinkedIn URLs and work emails for {', '.join(target_titles[:3])} and similar roles.", "frequency": "75-400/day", "action": "scrape_apollo", "enabled": True},
        {"name": "Target Scraped Leads", "role": "Lead Targeter", "description": f"Qualifies leads, enriches data, generates personalized messages about {company} for {strategy_focus}.", "frequency": "After scrape", "action": "target_leads", "enabled": True},
        {"name": f"Generate LinkedIn Post", "role": "Content Creator", "description": f"Writes thought-leadership posts about {product_category} trends to attract {', '.join(target_titles[:2])} and build authority.", "frequency": "1-2x/day", "action": "generate_post", "enabled": True},
        {"name": "Send Connection Requests", "role": "Network Builder", "description": f"Sends personalized connection notes to {product_category} buyers. Each note mentions their role, company, and a {strategy_focus} angle.", "frequency": "7-22/day", "action": "send_connection_request", "enabled": True},
        {"name": "Reply to Comments", "role": "Engagement Manager", "description": "Monitors your posts and replies to comments to boost visibility and engagement.", "frequency": "As needed", "action": "reply_comments", "enabled": True},
        {"name": "View Profiles", "role": "Presence Builder", "description": "Visits target profiles so they see your name before you reach out. Increases acceptance rates.", "frequency": "20-70/day", "action": "view_profiles", "enabled": True},
        {"name": "Send Cold Emails", "role": "Email Outreach", "description": f"Personalized cold emails about {company} focused on {strategy_focus}. Tone: {outreach_tone}.", "frequency": "25-120/day", "action": "send_emails", "enabled": True},
        {"name": "Manage Conversations", "role": "Conversation Manager", "description": "Handles LinkedIn and email replies. Categorizes as interested/not now/not interested.", "frequency": "2-3x/day", "action": "manage_conversations", "enabled": True},
        {"name": f"Book {strategy_focus.title()}", "role": "Meeting Scheduler", "description": f"Converts interested prospects into {strategy_focus}. Sends calendar links and follows up.", "frequency": "As opportunities arise", "action": "book_meetings", "enabled": True},
        {"name": "Analyze Performance", "role": "Analytics Agent", "description": "Tracks daily metrics: connections, acceptance rate, replies, emails opened, meetings booked.", "frequency": "End of day", "action": "analyze_performance", "enabled": True},
        {"name": "Post Engagement", "role": "Social Warmer", "description": "Likes and comments on prospects' posts before sending connection requests.", "frequency": "10-20/day", "action": "post_engagement", "enabled": True},
    ]

    schedule = [
        {"time": "9:00 AM", "step": f"Review {strategy_focus} strategy & daily plan"},
        {"time": "9:15 AM", "step": f"Scrape new {product_category} leads from Apollo"},
        {"time": "9:30 AM", "step": "Qualify and score leads against ICP"},
        {"time": "9:45 AM", "step": "Enrich lead data (company, revenue, tech stack)"},
        {"time": "10:00 AM", "step": "View target profiles (warm-up)"},
        {"time": "10:30 AM", "step": "Like & comment on prospects' posts"},
        {"time": "11:00 AM", "step": "Send personalized connection requests"},
        {"time": "12:00 PM", "step": f"Send cold emails about {company}"},
        {"time": "1:00 PM", "step": "Check & reply to LinkedIn messages"},
        {"time": "2:00 PM", "step": "Check & reply to email responses"},
        {"time": "3:00 PM", "step": f"Publish LinkedIn post about {product_category}"},
        {"time": "3:30 PM", "step": "Reply to comments on your posts"},
        {"time": "4:00 PM", "step": f"Follow up hot prospects / book {strategy_focus}"},
        {"time": "4:30 PM", "step": "Daily performance analysis & report"},
        {"time": "5:00 PM", "step": "Save insights, end day"},
    ]

    safety_rules = [
        "No weekends in Phase 1 (warm-up period)",
        "No duplicate messages ever",
        "Pause immediately on LinkedIn warning",
        "Minimum 30% connection acceptance rate",
        "All messages must be personalized",
        f"Tone: {outreach_tone}",
        f"Goal: {goal}",
    ]

    return {
        "product_name": company,
        "product_description": description or f"{company} - {product_category}",
        "product_category": product_category,
        "target_titles": target_titles,
        "target_industries": target_industries,
        "company_size_min": 50,
        "company_size_max": 500,
        "revenue_min": "5000000",
        "revenue_max": "100000000",
        "strategy_focus": strategy_focus,
        "outreach_tone": outreach_tone,
        "automations": automations,
        "daily_schedule": schedule,
        "safety_rules": safety_rules,
    }


@app.post("/research")
def research_website(req: ResearchRequest, user=Depends(get_current_user)):
    """Research a website and generate a complete automation strategy.

    The user provides their website URL and goal.
    The agent scrapes the site, understands the product, and generates:
    - ICP (titles, industries, company size)
    - Automations customized to the product
    - Daily schedule
    - Safety rules
    - Outreach strategy and tone

    Returns everything for user approval before saving.
    """
    # Step 1: Scrape the website
    website_data = scrape_website(req.website)

    if not website_data.get("scraped"):
        return {
            "success": False,
            "error": f"Could not reach {req.website}: {website_data.get('error', 'Unknown error')}",
        }

    # Step 2: Generate strategy
    strategy = generate_strategy(website_data, req.goal)

    return {
        "success": True,
        "website_data": {
            "url": website_data["url"],
            "company_name": website_data["company_name"],
            "title": website_data["title"],
            "description": website_data["description"],
            "headings": website_data["headings"][:5],
        },
        "proposed_strategy": strategy,
        "message": f"I've researched {website_data['company_name']} and created a {strategy['strategy_focus']} strategy targeting {len(strategy['target_titles'])} roles across {len(strategy['target_industries'])} industries. Review below and approve to start.",
    }


# ── Onboarding & Config ──────────────────────────────────────────

@app.get("/config")
def get_config(user=Depends(get_current_user)):
    """Get user's config. If not onboarded, returns onboarded=false."""
    conn = get_db()
    row = conn.execute("SELECT * FROM user_config WHERE user_id = ?", (user["id"],)).fetchone()
    conn.close()

    if not row:
        return {"onboarded": False}

    data = dict(row)
    # Parse JSON fields
    for field in ["target_titles", "target_industries", "daily_schedule", "automations", "safety_rules"]:
        try:
            data[field] = json.loads(data[field]) if data[field] else []
        except (json.JSONDecodeError, TypeError):
            data[field] = []
    data["onboarded"] = bool(data.get("onboarded", 0))
    return data


@app.post("/config/onboard")
def onboard(req: OnboardingRequest, user=Depends(get_current_user)):
    """First-time onboarding -- saves the user's goal, ICP, and creates default automations."""
    conn = get_db()
    now = now_iso()

    # Generate default automations based on their goal
    default_automations = [
        {"name": "Scrape Leads from Apollo", "role": "Lead Scraper", "description": f"Searches Apollo for {req.goal} prospects matching your ICP. Pulls LinkedIn URLs and work emails.", "frequency": "75-400/day", "action": "scrape_apollo", "enabled": True},
        {"name": "Target Scraped Leads", "role": "Lead Targeter", "description": "Qualifies, enriches, and generates personalized messages for scraped leads.", "frequency": "After scrape", "action": "target_leads", "enabled": True},
        {"name": "Generate LinkedIn Post", "role": "Content Creator", "description": f"Writes thought-leadership posts about {req.product_name or 'your product'} to attract inbound leads.", "frequency": "1-2x/day", "action": "generate_post", "enabled": True},
        {"name": "Send Connection Requests", "role": "Network Builder", "description": "Sends personalized connection notes to target leads. Each under 300 chars.", "frequency": "7-22/day", "action": "send_connection_request", "enabled": True},
        {"name": "Reply to Comments", "role": "Engagement Manager", "description": "Monitors posts and replies to comments to boost visibility.", "frequency": "As needed", "action": "reply_comments", "enabled": True},
        {"name": "View Profiles", "role": "Presence Builder", "description": "Visits target profiles so they see your name before outreach.", "frequency": "20-70/day", "action": "view_profiles", "enabled": True},
        {"name": "Send Cold Emails", "role": "Email Outreach", "description": f"Personalized emails about {req.product_name or 'your product'} via Instantly.", "frequency": "25-120/day", "action": "send_emails", "enabled": True},
        {"name": "Manage Conversations", "role": "Conversation Manager", "description": "Handles LinkedIn and email replies from prospects.", "frequency": "2-3x/day", "action": "manage_conversations", "enabled": True},
        {"name": "Book Meetings", "role": "Meeting Scheduler", "description": f"Converts interested prospects into {req.goal} demos/calls.", "frequency": "As needed", "action": "book_meetings", "enabled": True},
        {"name": "Analyze Performance", "role": "Analytics Agent", "description": "Tracks connections, acceptance rate, replies, meetings booked.", "frequency": "End of day", "action": "analyze_performance", "enabled": True},
        {"name": "Post Engagement", "role": "Social Warmer", "description": "Likes and comments on prospects' posts before outreach.", "frequency": "10-20/day", "action": "post_engagement", "enabled": True},
    ]

    default_schedule = [
        {"time": "9:00 AM", "step": "Review strategy & confirm daily plan"},
        {"time": "9:15 AM", "step": "Scrape new leads from Apollo"},
        {"time": "9:30 AM", "step": "Qualify and score leads against ICP"},
        {"time": "9:45 AM", "step": "Enrich lead data"},
        {"time": "10:00 AM", "step": "View target profiles (warm-up)"},
        {"time": "10:30 AM", "step": "Like & comment on prospects' posts"},
        {"time": "11:00 AM", "step": "Send personalized connection requests"},
        {"time": "12:00 PM", "step": "Send cold emails"},
        {"time": "1:00 PM", "step": "Check & reply to LinkedIn messages"},
        {"time": "2:00 PM", "step": "Check & reply to email responses"},
        {"time": "3:00 PM", "step": "Generate and publish LinkedIn post"},
        {"time": "3:30 PM", "step": "Reply to comments on your posts"},
        {"time": "4:00 PM", "step": f"Follow up hot prospects / book {req.goal}"},
        {"time": "4:30 PM", "step": "Daily performance analysis"},
        {"time": "5:00 PM", "step": "Save insights, end day"},
    ]

    default_rules = [
        "No weekends in Phase 1",
        "No duplicate messages ever",
        "Pause immediately on LinkedIn warning",
        "Minimum 30% connection acceptance rate",
        "All messages must be personalized",
        f"Goal: {req.goal}",
    ]

    # Check if already exists
    existing = conn.execute("SELECT user_id FROM user_config WHERE user_id = ?", (user["id"],)).fetchone()

    if existing:
        conn.execute("""
            UPDATE user_config SET
                onboarded = 1, goal = ?, product_name = ?, product_description = ?,
                target_titles = ?, target_industries = ?,
                company_size_min = ?, company_size_max = ?,
                revenue_min = ?, revenue_max = ?,
                daily_schedule = ?, automations = ?, safety_rules = ?,
                updated_at = ?
            WHERE user_id = ?
        """, (
            req.goal, req.product_name, req.product_description,
            json.dumps(req.target_titles), json.dumps(req.target_industries),
            req.company_size_min, req.company_size_max,
            req.revenue_min, req.revenue_max,
            json.dumps(default_schedule), json.dumps(default_automations), json.dumps(default_rules),
            now, user["id"],
        ))
    else:
        conn.execute("""
            INSERT INTO user_config (
                user_id, onboarded, goal, product_name, product_description,
                target_titles, target_industries,
                company_size_min, company_size_max,
                revenue_min, revenue_max,
                daily_schedule, automations, safety_rules, updated_at
            ) VALUES (?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            user["id"], req.goal, req.product_name, req.product_description,
            json.dumps(req.target_titles), json.dumps(req.target_industries),
            req.company_size_min, req.company_size_max,
            req.revenue_min, req.revenue_max,
            json.dumps(default_schedule), json.dumps(default_automations), json.dumps(default_rules),
            now,
        ))

    conn.commit()
    conn.close()

    return {
        "onboarded": True,
        "automations_count": len(default_automations),
        "schedule_steps": len(default_schedule),
        "rules_count": len(default_rules),
    }


@app.put("/config")
def update_config(req: ConfigUpdateRequest, user=Depends(get_current_user)):
    """Update specific config fields."""
    conn = get_db()
    now = now_iso()

    updates = []
    params = []
    for field, value in req.dict(exclude_none=True).items():
        if isinstance(value, (list, dict)):
            updates.append(f"{field} = ?")
            params.append(json.dumps(value))
        else:
            updates.append(f"{field} = ?")
            params.append(value)

    if not updates:
        conn.close()
        return {"updated": False}

    updates.append("updated_at = ?")
    params.append(now)
    params.append(user["id"])

    conn.execute(f"UPDATE user_config SET {', '.join(updates)} WHERE user_id = ?", params)
    conn.commit()
    conn.close()
    return {"updated": True}


# ── Integrations (API Keys) ──────────────────────────────────────

@app.post("/integrations/apollo")
def connect_apollo(req: ApolloKeyRequest, user=Depends(get_current_user)):
    """Validate and save Apollo API key. Tests it by making a real API call."""
    import requests as http_requests

    # Test the key with Apollo's API
    try:
        resp = http_requests.post(
            "https://api.apollo.io/v1/mixed_people/search",
            json={
                "api_key": req.apollo_api_key,
                "per_page": 1,
                "person_titles": ["CEO"],
            },
            headers={"Content-Type": "application/json"},
            timeout=10,
        )

        if resp.status_code == 200 and resp.json().get("people") is not None:
            # Key is valid -- save it
            conn = get_db()
            now = now_iso()
            existing = conn.execute("SELECT user_id FROM user_config WHERE user_id = ?", (user["id"],)).fetchone()
            if existing:
                conn.execute(
                    "UPDATE user_config SET apollo_api_key = ?, apollo_connected = 1, updated_at = ? WHERE user_id = ?",
                    (req.apollo_api_key, now, user["id"]),
                )
            else:
                conn.execute(
                    "INSERT INTO user_config (user_id, apollo_api_key, apollo_connected, updated_at) VALUES (?, ?, 1, ?)",
                    (user["id"], req.apollo_api_key, now),
                )
            conn.commit()
            conn.close()
            return {"connected": True, "message": "Apollo connected successfully!"}
        else:
            return {"connected": False, "message": "Invalid API key. Check your Apollo dashboard for the correct key."}
    except http_requests.Timeout:
        return {"connected": False, "message": "Apollo API timed out. Try again."}
    except Exception as e:
        return {"connected": False, "message": f"Connection failed: {str(e)}"}


@app.post("/integrations/instantly")
def connect_instantly(req: InstantlyKeyRequest, user=Depends(get_current_user)):
    """Validate and save Instantly API key."""
    import requests as http_requests

    try:
        resp = http_requests.get(
            "https://api.instantly.ai/api/v1/campaign/list",
            params={"api_key": req.instantly_api_key},
            timeout=10,
        )

        if resp.status_code == 200:
            conn = get_db()
            now = now_iso()
            existing = conn.execute("SELECT user_id FROM user_config WHERE user_id = ?", (user["id"],)).fetchone()
            if existing:
                conn.execute(
                    "UPDATE user_config SET instantly_api_key = ?, instantly_campaign_id = ?, instantly_connected = 1, updated_at = ? WHERE user_id = ?",
                    (req.instantly_api_key, req.instantly_campaign_id, now, user["id"]),
                )
            else:
                conn.execute(
                    "INSERT INTO user_config (user_id, instantly_api_key, instantly_campaign_id, instantly_connected, updated_at) VALUES (?, ?, ?, 1, ?)",
                    (user["id"], req.instantly_api_key, req.instantly_campaign_id, now),
                )
            conn.commit()
            conn.close()
            return {"connected": True, "message": "Instantly connected successfully!"}
        else:
            return {"connected": False, "message": "Invalid API key. Check your Instantly dashboard."}
    except Exception as e:
        return {"connected": False, "message": f"Connection failed: {str(e)}"}


@app.get("/integrations")
def get_integrations(user=Depends(get_current_user)):
    """Get connection status of all integrations (never returns actual keys)."""
    conn = get_db()
    row = conn.execute(
        "SELECT apollo_connected, instantly_connected, instantly_campaign_id FROM user_config WHERE user_id = ?",
        (user["id"],),
    ).fetchone()
    conn.close()

    if not row:
        return {"apollo": {"connected": False}, "instantly": {"connected": False}}

    return {
        "apollo": {"connected": bool(row["apollo_connected"])},
        "instantly": {
            "connected": bool(row["instantly_connected"]),
            "campaign_id": row["instantly_campaign_id"] or "",
        },
    }


# ── Health ───────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok"}
