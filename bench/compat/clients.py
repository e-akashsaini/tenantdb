"""Drive the same small multi-tenant workload through whatever endpoint is given,
with each client library, and print one line per check. Used to compare the
proxy against direct Postgres."""
import sys, json, asyncio

host, port, user, db = sys.argv[1], int(sys.argv[2]), sys.argv[3], sys.argv[4]
results = []

def check(name, ok, detail=""):
    results.append((name, bool(ok), detail))

def workload_checks(run):
    """run(sql, params) -> list of rows; same assertions for every driver."""
    run("DELETE FROM compat_orders WHERE id >= 9000", ())
    run("INSERT INTO compat_orders (id, tenant_id, amount) VALUES (9001, 'acme', 10), (9002, 'globex', 20)", ())
    rows = run("SELECT id, tenant_id FROM compat_orders WHERE tenant_id = %s ORDER BY id", ("acme",)) if "%s" in "x" else None
    return run

try:
    import psycopg2
    c = psycopg2.connect(host=host, port=port, user=user, dbname=db, password="pw")
    c.autocommit = True
    cur = c.cursor()
    cur.execute("DELETE FROM compat_orders WHERE id >= 9000")
    cur.execute("INSERT INTO compat_orders (id, tenant_id, amount) VALUES (9001, 'acme', 10), (9002, 'globex', 20)")
    cur.execute("SELECT id, tenant_id FROM compat_orders WHERE tenant_id = %s ORDER BY id", ("acme",))
    check("psycopg2 select with parameter", cur.fetchall() == [(9001, "acme")])
    cur.execute("UPDATE compat_orders SET amount = amount + 1 WHERE id = %s RETURNING amount", (9001,))
    check("psycopg2 update returning", cur.fetchall() == [(11,)])
    c.autocommit = False
    cur.execute("INSERT INTO compat_orders (id, tenant_id, amount) VALUES (9003, 'acme', 1)")
    c.rollback()
    cur.execute("SELECT count(*) FROM compat_orders WHERE id = 9003")
    check("psycopg2 rollback", cur.fetchone() == (0,))
    with c.cursor() as cur2:
        cur2.copy_expert("COPY compat_orders (id, tenant_id, amount) FROM STDIN WITH (FORMAT csv)", __import__("io").StringIO("9004,acme,5\n9005,acme,6\n"))
    c.commit()
    cur.execute("SELECT count(*) FROM compat_orders WHERE id IN (9004, 9005)")
    check("psycopg2 COPY FROM STDIN", cur.fetchone() == (2,))
    c.close()
except Exception as e:
    check("psycopg2", False, repr(e))

try:
    import psycopg
    with psycopg.connect(host=host, port=port, user=user, dbname=db, password="pw", autocommit=True) as c:
        for i in range(8):  # psycopg3 switches to a server-side prepared statement after 5 uses
            r = c.execute("SELECT id FROM compat_orders WHERE tenant_id = %s AND id = %s", ("acme", 9001)).fetchall()
        check("psycopg3 repeated query (auto-prepared)", r == [(9001,)])
        r = c.execute("SELECT %s::int + 1", (41,)).fetchone()
        check("psycopg3 typed parameter", r == (42,))
        with c.pipeline():
            cur1 = c.execute("SELECT 1")
            cur2 = c.execute("SELECT id FROM compat_orders WHERE tenant_id = %s ORDER BY id", ("acme",))
            c.execute("SELECT 3")
        check("psycopg3 pipeline mode", cur1.fetchone() == (1,) and [x[0] for x in cur2.fetchall()][:1] == [9001])
        with c.transaction():
            with c.cursor(name="srv") as cur:  # server-side cursor: DECLARE / FETCH
                cur.execute("SELECT id FROM compat_orders WHERE tenant_id = %s", ("acme",))
                check("psycopg3 server-side cursor", len(cur.fetchmany(1)) == 1)
        with c.cursor() as cur:
            with cur.copy("COPY compat_orders (id, tenant_id, amount) TO STDOUT") as cp:
                n = sum(1 for _ in cp)
            check("psycopg3 COPY TO STDOUT", n >= 2)
        with c.transaction():  # one transaction: temp tables do not survive a pooler's server-connection switch
            c.execute("CREATE TEMP TABLE tmp_compat ON COMMIT DROP AS SELECT * FROM compat_orders WHERE tenant_id = 'acme'")
            check("psycopg3 temp table", c.execute("SELECT count(*) FROM tmp_compat").fetchone()[0] >= 1)
except Exception as e:
    check("psycopg3", False, repr(e))

try:
    import asyncpg
    async def go():
        conn = await asyncpg.connect(host=host, port=port, user=user, database=db, password="pw")
        rows = await conn.fetch("SELECT id FROM compat_orders WHERE tenant_id = $1 ORDER BY id", "acme")
        check("asyncpg fetch (binary protocol)", rows and rows[0]["id"] == 9001)
        st = await conn.prepare("SELECT amount FROM compat_orders WHERE id = $1")
        for _ in range(3):
            v = await st.fetchval(9001)
        check("asyncpg prepared statement", v == 11)
        async with conn.transaction():
            await conn.execute("UPDATE compat_orders SET amount = 99 WHERE id = $1", 9002)
        check("asyncpg transaction", await conn.fetchval("SELECT amount FROM compat_orders WHERE id = 9002") == 99)
        res = await conn.copy_records_to_table("compat_orders", records=[(9006, "acme", 7)], columns=["id", "tenant_id", "amount"])
        check("asyncpg copy_records_to_table (binary COPY)", res.startswith("COPY"))
        await conn.close()
    asyncio.run(go())
except Exception as e:
    check("asyncpg", False, repr(e))

try:
    import sqlalchemy as sa
    eng = sa.create_engine(f"postgresql+psycopg://{user}:pw@{host}:{port}/{db}")
    with eng.begin() as cx:
        n = cx.execute(sa.text("SELECT count(*) FROM compat_orders WHERE tenant_id = :t"), {"t": "acme"}).scalar()
        check("sqlalchemy (psycopg3) query in a transaction", n >= 1)
    eng.dispose()
except Exception as e:
    check("sqlalchemy", False, repr(e))

for name, ok, detail in results:
    print(("PASS " if ok else "FAIL ") + name + ("  " + detail if detail and not ok else ""))
sys.exit(0 if all(ok for _, ok, _ in results) else 1)
