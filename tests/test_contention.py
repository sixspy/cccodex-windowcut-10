"""争用落库夹具：双人抢同一 lock_key，后到必留争用行。

题面口径（选定后写入争用登记册，当天禁止改口，两路径不得混用）：
  甲路径 —— try_acquire 失败时与业务拒绝同进程同事务写 contention_log；
            缺日志则整笔视为未拦下并回滚提示。
"""
import threading

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from app import app, init_db


@pytest.fixture(autouse=True)
def _db(tmp_path, monkeypatch):
    monkeypatch.setattr("db.DB_PATH", tmp_path / "contention.db")
    init_db()
    yield


def _login(c: TestClient, username: str, password: str) -> dict:
    tok = c.post(
        "/api/login", json={"username": username, "password": password}
    ).json()["token"]
    return {"Authorization": "Bearer " + tok}


@pytest.fixture
def two_users():
    """双人抢锁夹具：admin 与 closer 两个已登录会话抢同一 lock_key。"""
    c = TestClient(app)
    return c, _login(c, "admin", "123456"), _login(c, "closer", "close123")


def test_blocked_business_write_must_leave_contention_row(two_users):
    """争用空白复现：admin 持锁，closer 业务写被挡，contention_log 不得为空。"""
    c, ha, hb = two_users
    assert (
        c.post(
            "/api/locks/acquire", headers=ha, json={"lock_key": "window:2"}
        ).status_code
        == 200
    )
    r = c.post("/api/windows/2/close", headers=hb)
    assert r.status_code == 409
    items = c.get("/api/contention", headers=ha).json()["items"]
    mine = [x for x in items if x["lock_key"] == "window:2" and x["actor"] == "closer"]
    assert len(mine) == 1, "后到被挡必须落一条争用行（一胜一记）"


def test_same_key_repeated_grabs_each_leave_contention_row(two_users):
    """同键连抢：每次被挡都要落争用行，锁始终归先到者。"""
    c, ha, hb = two_users
    assert (
        c.post(
            "/api/locks/acquire", headers=ha, json={"lock_key": "window:3"}
        ).status_code
        == 200
    )
    for _ in range(2):
        assert c.post("/api/windows/3/close", headers=hb).status_code == 409
    items = c.get("/api/contention", headers=ha).json()["items"]
    mine = [x for x in items if x["lock_key"] == "window:3" and x["actor"] == "closer"]
    assert len(mine) == 2, "同键连抢两次，争用行须为两条"
    locks = c.get("/api/locks", headers=ha).json()["items"]
    holders = [x["holder"] for x in locks if x["lock_key"] == "window:3"]
    assert holders == ["admin"], "锁仍归先到者"


def test_five_views_tell_same_story(two_users):
    """五处同态：争用列表/持锁表/审计流水/业务回执/值班页互相印证。"""
    c, ha, hb = two_users
    c.post("/api/locks/acquire", headers=ha, json={"lock_key": "window:2"})
    r = c.post("/api/windows/2/close", headers=hb)
    assert r.status_code == 409
    # 1) 业务回执：带争用行 id，可回查
    receipt = r.json()["detail"]
    cid = receipt["contention_id"]
    assert receipt["lock_key"] == "window:2"
    # 2) 争用列表：同一行
    cont = c.get("/api/contention", headers=ha).json()["items"]
    row = [x for x in cont if x["id"] == cid]
    assert len(row) == 1
    assert row[0]["actor"] == "closer" and row[0]["action"] == "争用"
    # 3) 持锁表：锁仍归先到者 admin
    locks = c.get("/api/locks", headers=ha).json()["items"]
    assert [x["holder"] for x in locks if x["lock_key"] == "window:2"] == ["admin"]
    # 4) 审计流水：lock_contention 与同一人同一键
    audits = c.get("/api/audit", headers=ha).json()["items"]
    assert any(
        a["action"] == "lock_contention"
        and a["actor"] == "closer"
        and a["target"] == "window:2"
        for a in audits
    )
    # 5) 值班页：计数与争用列表一致，登记册口径为甲
    dash = c.get("/api/dashboard", headers=ha).json()
    assert dash["contention"] == len(cont)
    assert dash["registry_path"] == "甲"
    assert dash["contention_deviated"] == 0


def test_registry_seeded_and_same_day_flip_refused(two_users):
    """登记册：选定甲路径已落册；当天改口拒写并记争用偏了。"""
    c, ha, _ = two_users
    reg = c.get("/api/contention/registry", headers=ha).json()
    assert reg["current"]["path"] == "甲"
    # 同口径重复登记幂等
    ok = c.post("/api/contention/registry", headers=ha, json={"path": "甲"})
    assert ok.status_code == 200
    # 当天改口：拒写
    r = c.post("/api/contention/registry", headers=ha, json={"path": "乙"})
    assert r.status_code == 409
    cont = c.get("/api/contention", headers=ha).json()["items"]
    assert any(x["action"] == "争用偏了" for x in cont)
    audits = c.get("/api/audit", headers=ha).json()["items"]
    assert any(a["action"] == "contention_deviated" for a in audits)
    # 登记册未被改动
    assert c.get("/api/contention/registry", headers=ha).json()["current"]["path"] == "甲"
    # 非法口径
    assert (
        c.post("/api/contention/registry", headers=ha, json={"path": "丙"}).status_code
        == 400
    )


def test_deviation_from_registry_refuses_business_write(two_users):
    """偏离争用登记册（登记被改成乙）：业务写拒写并记争用偏了，不留正常争用行。"""
    c, ha, hb = two_users
    import db as dbmod

    with dbmod.connect() as conn:
        conn.execute("UPDATE contention_registry SET path='乙'")
        conn.commit()
    c.post("/api/locks/acquire", headers=ha, json={"lock_key": "window:2"})
    r = c.post("/api/windows/2/close", headers=hb)
    assert r.status_code == 409
    assert "争用偏了" in str(r.json()["detail"])
    cont = c.get("/api/contention", headers=ha).json()["items"]
    assert any(
        x["action"] == "争用偏了" and x["lock_key"] == "window:2" for x in cont
    )
    assert not any(
        x["action"] == "争用" and x["lock_key"] == "window:2" and x["actor"] == "closer"
        for x in cont
    )


def test_simultaneous_grab_one_winner_one_contention(monkeypatch):
    """两人同时抢同一 lock_key：可复现一胜一记争用（抢锁竞态转争用）。"""
    import db as dbmod
    import locks as locksmod

    barrier = threading.Barrier(2)
    real_connect = dbmod.connect

    class SyncedConn:
        """让两个连接的 INSERT 在 SELECT 之后对齐，复现同时抢锁。"""

        def __init__(self, raw):
            self._raw = raw

        def execute(self, sql, params=()):
            if "INSERT INTO window_locks" in sql:
                barrier.wait(timeout=10)
            return self._raw.execute(sql, params)

        def __getattr__(self, name):
            return getattr(self._raw, name)

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return self._raw.__exit__(*exc)

    monkeypatch.setattr(locksmod, "connect", lambda: SyncedConn(real_connect()))

    results = {}
    key = "race:simultaneous"

    def grab(user):
        with locksmod.connect() as conn:
            try:
                locksmod.require_lock(conn, key, user)
                conn.commit()
                results[user] = "win"
            except HTTPException as exc:
                results[user] = exc.status_code

    threads = [
        threading.Thread(target=grab, args=("admin",)),
        threading.Thread(target=grab, args=("closer",)),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(15)
    assert all(not t.is_alive() for t in threads), "抢锁线程不得卡死"
    assert sorted(results.values(), key=str) == [409, "win"], "须一胜一被挡"

    with real_connect() as conn:
        holders = conn.execute(
            "SELECT holder FROM window_locks WHERE lock_key=?", (key,)
        ).fetchall()
        assert len(holders) == 1, "锁只归一胜"
        rows = conn.execute(
            "SELECT * FROM contention_log WHERE lock_key=?", (key,)
        ).fetchall()
        assert len(rows) == 1, "一记争用"
        assert rows[0]["actor"] != holders[0]["holder"], "争用行记的是后到者"
