require('dotenv').config();
const express = require('express');
const mysql = require('mysql2/promise');
const bodyParser = require('body-parser');

const app = express();
app.use(bodyParser.json({ limit: '10mb' }));

const PORT = process.env.PORT || 3000;
const API_KEY = process.env.API_KEY;

// Allowed tables (protect against arbitrary writes)
const ALLOWED_TABLES = new Set([
  'branch','bank','supplier','staff','expgroup','incgroup','purchase','customer','sales',
  'bankcash','expense','income','cheque','salary','customerpayment','itemmaster','bill','genbill','shop'
]);

// Primary key column mapping
const PRIMARY_KEYS = {
  customer: "cusid",
  staff: "staffid",
  supplier: "supid",
  branch: "brid",
  bank: "bankid",
  purchase: "purid",
  sales: "saleid",
  bankcash: "bpid",
  expense: "expid",
  income: "incomeid",
  cheque: "chqid",
  salary: "salid",
  customerpayment: "cuspayid",
  itemmaster: "itemid",
  bill: "billid",
  genbill: "genbillno",
  shop: "shopid",
  expgroup: "expgpid",
  incgroup: "incgpid"
};

// Create a pool once
const pool = mysql.createPool({
  host: process.env.DB_HOST,
  user: process.env.DB_USER,
  password: process.env.DB_PASS,
  database: process.env.DB_NAME,
  port: process.env.DB_PORT ? Number(process.env.DB_PORT) : 3306,
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0
});

// API key middleware
app.use((req, res, next) => {
  const key = req.headers['x-api-key'] || req.query.api_key;
  if (!API_KEY || key !== API_KEY) return res.status(403).json({ error: 'Invalid or missing API key' });
  next();
});

// Column cache helper
const columnsCache = new Map();
async function getColumnsForTable(conn, table) {
  if (columnsCache.has(table)) return columnsCache.get(table);
  const [rows] = await conn.query('SHOW COLUMNS FROM ??', [table]);
  const cols = rows.map(r => r.Field);
  columnsCache.set(table, cols);
  return cols;
}

/**
 * Main sync endpoint
 * Body: { tenant_id: <int>, data: [ { client_uuid, client_id(opt), ...fields, sync_status, local_updated_at, local_version }, ... ] }
 */
app.post('/api/sync/:table', async (req, res) => {
  const table = req.params.table;
  if (!ALLOWED_TABLES.has(table)) return res.status(400).json({ error: 'table not allowed' });

  const tenantId = req.body.tenant_id;
  const records = req.body.data;
  if (!tenantId || !Array.isArray(records)) return res.status(400).json({ error: 'tenant_id and data[] required' });

  const conn = await pool.getConnection();
  try {
    const serverCols = await getColumnsForTable(conn, table);

    if (!serverCols.includes('tenant_id')) {
      return res.status(500).json({ error: 'Server table missing tenant_id column' });
    }

    const hasClientUuid = serverCols.includes('client_uuid');
    const mappings = [];
    await conn.beginTransaction();

    for (const rec of records) {
      try {
        const clientUuid = rec.client_uuid || null;
        const clientId = rec.client_id !== undefined ? rec.client_id : null;
        const syncStatus = rec.sync_status !== undefined ? Number(rec.sync_status) : 0;

        if (syncStatus === 3) {
          if (clientUuid && hasClientUuid) {
            const [delRes] = await conn.query(`DELETE FROM \`${table}\` WHERE tenant_id = ? AND client_uuid = ?`, [tenantId, clientUuid]);
            mappings.push({ client_uuid: clientUuid, client_id: clientId, server_id: delRes.affectedRows ? delRes.affectedRows : null, status: 'deleted' });
            continue;
          } else if (clientId && PRIMARY_KEYS[table]) {
            const pk = PRIMARY_KEYS[table];
            const [delRes] = await conn.query(`DELETE FROM \`${table}\` WHERE tenant_id = ? AND \`${pk}\` = ?`, [tenantId, clientId]);
            mappings.push({ client_uuid: clientUuid, client_id: clientId, server_id: delRes.affectedRows ? clientId : null, status: 'deleted' });
            continue;
          } else {
            mappings.push({ client_uuid: clientUuid, client_id: clientId, server_id: null, status: 'skipped' });
            continue;
          }
        }

        // Build allowed keys
        const allowedKeys = [];
        for (const k of Object.keys(rec)) {
          if (serverCols.includes(k) && !['server_id','local_version','local_updated_at'].includes(k)) {
            allowedKeys.push(k);
          }
        }

        if (!allowedKeys.includes('tenant_id')) allowedKeys.push('tenant_id');
        if (clientUuid && hasClientUuid && !allowedKeys.includes('client_uuid')) allowedKeys.push('client_uuid');

        const values = allowedKeys.map(col => {
          if (col === 'tenant_id') return tenantId;
          return rec[col] !== undefined ? rec[col] : null;
        });

        // Find existing by tenant+client_uuid or by pk
        let existingServerId = null;
        if (clientUuid && hasClientUuid) {
          const pkName = PRIMARY_KEYS[table] || 'id';
          const [rows] = await conn.query(`SELECT \`${pkName}\` as id FROM \`${table}\` WHERE tenant_id = ? AND client_uuid = ? LIMIT 1`, [tenantId, clientUuid]);
          if (rows.length > 0) existingServerId = rows[0].id;
        }

        if (!existingServerId && clientId && PRIMARY_KEYS[table]) {
          const pk = PRIMARY_KEYS[table];
          const [rows] = await conn.query(`SELECT \`${pk}\` as id FROM \`${table}\` WHERE tenant_id = ? AND \`${pk}\` = ? LIMIT 1`, [tenantId, clientId]);
          if (rows.length > 0) existingServerId = rows[0].id;
        }

        if (existingServerId) {
          const setCols = allowedKeys.filter(c => c !== 'tenant_id');
          if (setCols.length === 0) {
            mappings.push({ client_uuid: clientUuid, client_id: clientId, server_id: existingServerId, status: 'noop' });
            continue;
          }
          const setSql = setCols.map(c => `\`${c}\` = ?`).join(', ');
          const setVals = setCols.map(c => (c === 'client_uuid' ? (clientUuid || rec[c] || null) : (rec[c] !== undefined ? rec[c] : null)));
          const pk = PRIMARY_KEYS[table] || 'id';
          const sql = `UPDATE \`${table}\` SET ${setSql} WHERE tenant_id = ? AND \`${pk}\` = ?`;
          await conn.query(sql, [...setVals, tenantId, existingServerId]);
          mappings.push({ client_uuid: clientUuid, client_id: clientId, server_id: existingServerId, status: 'updated', server_version: rec.server_version || null });
        } else {
          const insertCols = allowedKeys;
          const insertPlaceholders = insertCols.map(_ => '?').join(',');
          const insertSql = `INSERT INTO \`${table}\` (${insertCols.map(c => `\`${c}\``).join(',')}) VALUES (${insertPlaceholders})`;
          const [insRes] = await conn.query(insertSql, values);
          const newServerId = insRes.insertId;
          mappings.push({ client_uuid: clientUuid, client_id: clientId, server_id: newServerId, status: 'inserted', server_version: 1 });
        }

      } catch (rowErr) {
        console.error('Row sync error', rowErr);
        mappings.push({ client_uuid: rec.client_uuid || null, client_id: rec.client_id || null, server_id: null, status: 'error', message: rowErr.message });
      }
    }

    await conn.commit();
    return res.json(mappings);

  } catch (err) {
    console.error('Sync error', err);
    try { await conn.rollback(); } catch (e) { /* ignore */ }
    return res.status(500).json({ error: err.message });
  } finally {
    conn.release();
  }
});

// health
app.get('/health', (req, res) => res.json({ ok: true }));

app.listen(PORT, () => console.log(`Sync API listening on ${PORT}`));
