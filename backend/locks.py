"""窗口锁与争用登记（公共并发面，题面争用句应对得上）。

争用口径以 contention_registry 登记册为准，当天禁止改口，两路径不得混用。
本仓实现甲路径：try_acquire 失败时与业务拒绝同进程同事务写 contention_log，
缺日志则整笔视为未拦下并回滚提示；偏离登记册拒写并记争用偏了。
"""
from __future__ import annotations

import sqlite3
from datetime import datetime

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from auth import AuthHeader, require_user
from db import connect, write_audit

router = APIRouter(tags=["locks"])

# 本仓实现的争用落证路径（须与 contention_registry 登记口径一致）
IMPLEMENTED_PATH = "甲"


class LockIn(BaseModel):
    lock_key: str
    action: str = "acquire"


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _insert_contention(
    conn: sqlite3.Connection, lock_key: str, actor: str, action: str, detail: str
) -> int:
    cur = conn.execute(
        """INSERT INTO contention_log(lock_key,actor,action,detail,created_at)
           VALUES (?,?,?,?,?)""",
        (lock_key, actor, action, detail, _now()),
    )
    return int(cur.lastrowid)


def registered_path(conn: sqlite3.Connection) -> str | None:
    row = conn.execute(
        "SELECT path FROM contention_registry ORDER BY id DESC LIMIT 1"
    ).fetchone()
    return row["path"] if row else None


def try_acquire(conn: sqlite3.Connection, lock_key: str, holder: str) -> tuple[bool, int | None]:
    """占锁。返回 (是否持锁, 争用行 id)；失败时争用行已在本事务内写好待落证。"""
    row = conn.execute(
        "SELECT holder FROM window_locks WHERE lock_key=?", (lock_key,)
    ).fetchone()
    if row is None:
        try:
            conn.execute(
                "INSERT INTO window_locks(lock_key,holder,acquired_at) VALUES (?,?,?)",
                (lock_key, holder, _now()),
            )
            return True, None
        except sqlite3.IntegrityError:
            # 两人同时抢同一 lock_key：另一连接已抢先落锁，后到按争用处理
            row = conn.execute(
                "SELECT holder FROM window_locks WHERE lock_key=?", (lock_key,)
            ).fetchone()
            if row is None or row["holder"] == holder:
                raise
    elif row["holder"] == holder:
        return True, None
    cid = _insert_contention(conn, lock_key, holder, "争用", f"持有者={row['holder']}")
    return False, cid


def contend_or_raise(
    conn: sqlite3.Connection, lock_key: str, holder: str, contention_id: int | None
) -> None:
    """甲路径：占锁失败同步记争用，与业务拒绝同进程同事务落证后拒写。"""
    registered = registered_path(conn)
    if registered != IMPLEMENTED_PATH:
        # 偏离争用登记册：撤掉本事务争用草稿，拒写并记争用偏了
        conn.rollback()
        cid = _insert_contention(
            conn,
            lock_key,
            holder,
            "争用偏了",
            f"登记口径={registered}，实现口径={IMPLEMENTED_PATH}，拒写",
        )
        write_audit(
            conn,
            holder,
            "contention_deviated",
            lock_key,
            {
                "registered": registered,
                "implemented": IMPLEMENTED_PATH,
                "contention_id": cid,
            },
        )
        conn.commit()
        raise HTTPException(
            409,
            detail={
                "message": "争用偏了：登记口径与实现不符，拒写",
                "lock_key": lock_key,
                "registered": registered,
            },
        )
    row = (
        conn.execute(
            "SELECT id FROM contention_log WHERE id=?", (contention_id,)
        ).fetchone()
        if contention_id is not None
        else None
    )
    if row is None:
        # 缺日志：整笔视为未拦下，回滚并提示
        conn.rollback()
        raise HTTPException(
            500,
            detail={
                "message": "争用日志缺失：整笔视为未拦下，已回滚",
                "lock_key": lock_key,
            },
        )
    write_audit(
        conn, holder, "lock_contention", lock_key, {"contention_id": contention_id}
    )
    conn.commit()
    raise HTTPException(
        409,
        detail={
            "message": "争用：后到只记争用，不写业务",
            "lock_key": lock_key,
            "contention_id": contention_id,
        },
    )


def require_lock(conn: sqlite3.Connection, lock_key: str, holder: str) -> None:
    ok, cid = try_acquire(conn, lock_key, holder)
    if not ok:
        contend_or_raise(conn, lock_key, holder, cid)


def release(conn: sqlite3.Connection, lock_key: str, holder: str) -> None:
    conn.execute(
        "DELETE FROM window_locks WHERE lock_key=? AND holder=?",
        (lock_key, holder),
    )


@router.get("/api/locks")
def list_locks(authorization: AuthHeader = None):
    require_user(authorization)
    with connect() as conn:
        rows = conn.execute(
            "SELECT * FROM window_locks ORDER BY acquired_at DESC"
        ).fetchall()
        return {"items": [dict(r) for r in rows]}


@router.get("/api/contention")
def list_contention(authorization: AuthHeader = None):
    require_user(authorization)
    with connect() as conn:
        rows = conn.execute(
            "SELECT * FROM contention_log ORDER BY id DESC LIMIT 100"
        ).fetchall()
        return {"items": [dict(r) for r in rows]}


@router.post("/api/locks/acquire")
def acquire_lock(body: LockIn, authorization: AuthHeader = None):
    user = require_user(authorization)
    with connect() as conn:
        ok, cid = try_acquire(conn, body.lock_key, user)
        if not ok:
            contend_or_raise(conn, body.lock_key, user, cid)
        write_audit(conn, user, "lock_acquire", body.lock_key, {"ok": True})
        conn.commit()
        return {"ok": True, "lock_key": body.lock_key, "holder": user}


@router.post("/api/locks/release")
def release_lock(body: LockIn, authorization: AuthHeader = None):
    user = require_user(authorization)
    with connect() as conn:
        release(conn, body.lock_key, user)
        write_audit(conn, user, "lock_release", body.lock_key, {})
        conn.commit()
    return {"ok": True}
