// node-postgres through the endpoint given on the command line.
const { Client, Pool } = require('pg');
const [host, port, user, database] = process.argv.slice(2);
const out = [];
const check = (name, ok, detail = '') => out.push([name, !!ok, detail]);

(async () => {
  const c = new Client({ host, port: +port, user, database, password: 'pw' });
  try {
    await c.connect();
    await c.query("DELETE FROM compat_orders WHERE id >= 9100");
    await c.query("INSERT INTO compat_orders (id, tenant_id, amount) VALUES (9101, 'acme', 1)");
    let r = await c.query("SELECT id FROM compat_orders WHERE tenant_id = $1 AND id = $2", ['acme', 9101]);
    check('node-pg parameterised query (extended protocol)', r.rows.length === 1 && Number(r.rows[0].id) === 9101);
    r = await c.query({ name: 'by-id', text: 'SELECT amount FROM compat_orders WHERE id = $1', values: [9101] });
    r = await c.query({ name: 'by-id', text: 'SELECT amount FROM compat_orders WHERE id = $1', values: [9101] });
    check('node-pg named prepared statement reused', r.rows[0].amount === 1);
    await c.query('BEGIN');
    await c.query("UPDATE compat_orders SET amount = 5 WHERE id = 9101");
    await c.query('ROLLBACK');
    r = await c.query("SELECT amount FROM compat_orders WHERE id = 9101");
    check('node-pg rollback', r.rows[0].amount === 1);
    const results = await Promise.all([1, 2, 3, 4, 5].map(i => c.query('SELECT $1::int AS n', [i])));
    check('node-pg pipelined queries on one connection', results.every((x, i) => x.rows[0].n === i + 1));
    await c.end();
  } catch (e) { check('node-pg', false, String(e)); }
  try {
    const pool = new Pool({ host, port: +port, user, database, password: 'pw', max: 6 });
    const rs = await Promise.all(Array.from({ length: 30 }, (_, i) => pool.query('SELECT $1::int AS n', [i])));
    check('node-pg pool, 30 concurrent queries', rs.every((x, i) => x.rows[0].n === i));
    await pool.end();
  } catch (e) { check('node-pg pool', false, String(e)); }
  for (const [n, ok, d] of out) console.log((ok ? 'PASS ' : 'FAIL ') + n + (d && !ok ? '  ' + d : ''));
  process.exit(out.every(x => x[1]) ? 0 : 1);
})();
