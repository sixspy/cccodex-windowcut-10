"""审计与互证旁路 + 争用登记册（写路径：登记口径，当天禁止改口）。"""
from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from auth import AuthHeader, require_user
from db import connect, write_audit

router = APIRouter(tags=["audit"])

VALID_PATHS = ("甲", "乙")


class RegistryIn(BaseModel):
    path: str


@router.get("/api/audit")
def list_audit(authorization: AuthHeader = None, limit: int = 100):
    require_user(authorization)
    limit = max(1, min(limit, 500))
    with connect() as conn:
        rows = conn.execute(
            "SELECT * FROM audit_trail ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
        return {"items": [dict(r) for r in rows]}


@router.get("/api/audit/actions")
def audit_actions(authorization: AuthHeader = None):
    require_user(authorization)
    with connect() as conn:
        rows = conn.execute(
            """SELECT action, COUNT(*) c FROM audit_trail
               GROUP BY action ORDER BY c DESC"""
        ).fetchall()
        return {"items": [dict(r) for r in rows]}


@router.get("/api/contention/registry")
def get_contention_registry(authorization: AuthHeader = None):
    require_user(authorization)
    with connect() as conn:
        rows = conn.execute(
            "SELECT * FROM contention_registry ORDER BY id DESC"
        ).fetchall()
        items = [dict(r) for r in rows]
        return {"items": items, "current": items[0] if items else None}


@router.post("/api/contention/registry")
def register_contention_path(body: RegistryIn, authorization: AuthHeader = None):
    """选定争用口径写入登记册；当天禁止改口，改口即拒写并记争用偏了。"""
    user = require_user(authorization)
    if body.path not in VALID_PATHS:
        raise HTTPException(400, "口径须为甲或乙")
    today = datetime.now().date().isoformat()
    now = datetime.now().isoformat(timespec="seconds")
    with connect() as conn:
        row = conn.execute(
            "SELECT * FROM contention_registry WHERE decided_on=?", (today,)
        ).fetchone()
        if row and row["path"] != body.path:
            conn.execute(
                """INSERT INTO contention_log(lock_key,actor,action,detail,created_at)
                   VALUES (?,?,?,?,?)""",
                (
                    "contention_registry",
                    user,
                    "争用偏了",
                    f"当日已登记{row['path']}路径，拒改{body.path}路径",
                    now,
                ),
            )
            write_audit(
                conn,
                user,
                "contention_deviated",
                "contention_registry",
                {"registered": row["path"], "attempted": body.path},
            )
            conn.commit()
            raise HTTPException(409, f"当天禁止改口：已登记{row['path']}路径")
        if row:
            return {
                "ok": True,
                "already": True,
                "path": row["path"],
                "decided_on": today,
            }
        conn.execute(
            """INSERT INTO contention_registry(decided_on,path,decided_by,created_at)
               VALUES (?,?,?,?)""",
            (today, body.path, user, now),
        )
        write_audit(
            conn,
            user,
            "contention_registry",
            "contention_registry",
            {"path": body.path, "decided_on": today},
        )
        conn.commit()
        return {"ok": True, "path": body.path, "decided_on": today}


@router.get("/api/dashboard")
def dashboard(authorization: AuthHeader = None):
    require_user(authorization)
    with connect() as conn:
        open_n = conn.execute(
            "SELECT COUNT(*) c FROM windows WHERE status='开启'"
        ).fetchone()["c"]
        closed_n = conn.execute(
            "SELECT COUNT(*) c FROM windows WHERE status='已关闭'"
        ).fetchone()["c"]
        late_n = conn.execute("SELECT COUNT(*) c FROM late_bucket").fetchone()["c"]
        reject_n = conn.execute("SELECT COUNT(*) c FROM reject_slips").fetchone()["c"]
        hold_n = conn.execute(
            "SELECT COUNT(*) c FROM hold_staging WHERE status='暂存'"
        ).fetchone()["c"]
        mats = conn.execute("SELECT COUNT(*) c FROM materializations").fetchone()["c"]
        contention = conn.execute("SELECT COUNT(*) c FROM contention_log").fetchone()["c"]
        deviated = conn.execute(
            "SELECT COUNT(*) c FROM contention_log WHERE action='争用偏了'"
        ).fetchone()["c"]
        reg = conn.execute(
            "SELECT path FROM contention_registry ORDER BY id DESC LIMIT 1"
        ).fetchone()
        return {
            "open_windows": open_n,
            "closed_windows": closed_n,
            "late_bucket": late_n,
            "rejects": reject_n,
            "holds": hold_n,
            "materializations": mats,
            "contention": contention,
            "contention_deviated": deviated,
            "registry_path": reg["path"] if reg else None,
        }
