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
