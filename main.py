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
    allow_origins=[
        "http://localhost:3000",
        os.getenv("DASHBOARD_URL", ""),
    ],
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


# ── Health ───────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok"}
