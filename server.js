const http = require('http');
const fs = require('fs');
const path = require('path');
const { MongoClient } = require('mongodb');

// Load .env file variables (simple parser)
const ENV_PATH = path.resolve(__dirname, '.env');
(function loadEnvFile() {
  try {
    const buf = fs.readFileSync(ENV_PATH, 'utf8');
    buf.split(/\r?\n/).forEach(function (line) {
      const trimmed = line.trim();
      if (!trimmed || trimmed.startsWith('#')) return;
      const m = trimmed.match(/^([^=]+)=(.*)$/);
      if (!m) return;
      const key = m[1].trim();
      let val = m[2];
      if ((val.startsWith('"') && val.endsWith('"')) || (val.startsWith("'") && val.endsWith("'"))) {
        val = val.slice(1, -1);
      }
      if (!process.env[key]) process.env[key] = val;
    });
  } catch (e) { /* ignore if .env not present */ }
})();

const ROOT = path.resolve(__dirname);
const PORT = process.env.PORT ? Number(process.env.PORT) : 8080;
const MONGO_URI = process.env.MONGODB_URI || process.env.MONGO_URI || '';
const MONGO_DB_NAME = process.env.MONGO_DB_NAME || 'track';
const CLIENT_REF_START = process.env.CLIENT_REF_START ? Number(process.env.CLIENT_REF_START) : 23000;
const CLIENT_REF_SEED = Number.isFinite(CLIENT_REF_START) ? (CLIENT_REF_START - 1) : 22999;
const CLIENT_REF_FORCE = String(process.env.CLIENT_REF_FORCE || '').trim() === '1';

const MIME = {
  '.html': 'text/html',
  '.css': 'text/css',
  '.js': 'application/javascript',
  '.json': 'application/json',
  '.png': 'image/png',
  '.jpg': 'image/jpeg',
  '.jpeg': 'image/jpeg',
  '.svg': 'image/svg+xml',
  '.ico': 'image/x-icon',
  '.txt': 'text/plain',
  '.map': 'application/json',
};

let mongoClient = null;
let db = null;

async function initMongo() {
  if (!MONGO_URI) {
    console.warn('[Mongo] MONGO_URI não definido. O backend funcionará sem persistência.');
    return;
  }
  mongoClient = new MongoClient(MONGO_URI, { maxPoolSize: 10 });
  await mongoClient.connect();
  db = mongoClient.db(MONGO_DB_NAME);
  await db.collection('sessions').createIndex({ event_id: 1 }, { unique: true }).catch(() => {});
  await db.collection('sessions').createIndex({ client_ref: 1 }).catch(() => {});
  await db.collection('sessions').createIndex({ createdAt: 1 }).catch(() => {});
  await db.collection('messages').createIndex({ from: 1, createdAt: 1 }).catch(() => {});
  await db.collection('payments').createIndex({ transaction_id: 1 }, { unique: true }).catch(() => {});
  // Seed do contador global de client_ref, evitando conflito de operadores
  try {
    const now = new Date();
    await db.collection('counters').updateOne(
      { _id: 'global:client_ref' },
      { $setOnInsert: { seq: CLIENT_REF_SEED, createdAt: now } },
      { upsert: true }
    );
    if (CLIENT_REF_FORCE) {
      await db.collection('counters').updateOne(
        { _id: 'global:client_ref' },
        { $set: { seq: CLIENT_REF_SEED } },
        { upsert: true }
      );
      console.log('[Mongo] FORCE global:client_ref =>', CLIENT_REF_SEED);
    } else {
      console.log('[Mongo] Seed global:client_ref =>', CLIENT_REF_SEED);
    }
  } catch (e) {
    console.warn('[Mongo] Seed global:client_ref falhou:', e && e.message ? e.message : e);
  }
  console.log('[Mongo] Conectado e índices criados');
}

function safeJoin(base, target) {
  const targetPath = path.posix.normalize(target).replace(/^\/+/, '');
  const resolved = path.resolve(base, targetPath);
  if (!resolved.startsWith(base)) return null; // evita path traversal
  return resolved;
}

function serveFile(res, filePath) {
  fs.readFile(filePath, (err, data) => {
    if (err) {
      res.writeHead(404, { 'Content-Type': 'text/plain' });
      res.end('Not Found');
      return;
    }
    const ext = path.extname(filePath).toLowerCase();
    const type = MIME[ext] || 'application/octet-stream';
    res.writeHead(200, { 'Content-Type': type });
    res.end(data);
  });
}

function getIpFromHeaders(req) {
  const xfwd = req.headers['x-forwarded-for'];
  if (xfwd) return String(xfwd).split(',')[0].trim();
  const xreal = req.headers['x-real-ip'];
  if (xreal) return String(xreal).trim();
  const remote = req.socket && req.socket.remoteAddress ? req.socket.remoteAddress : null;
  return remote || null;
}

function readJson(req) {
  return new Promise((resolve, reject) => {
    let data = '';
    req.on('data', chunk => { data += chunk; if (data.length > 1e6) req.destroy(); });
    req.on('end', () => {
      try { resolve(data ? JSON.parse(data) : {}); } catch (e) { reject(e); }
    });
    req.on('error', reject);
  });
}

function sendJson(res, code, obj) {
  res.writeHead(code, {
    'Content-Type': 'application/json',
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type'
  });
  res.end(JSON.stringify(obj));
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, `http://${req.headers.host}`);
  let pathname = url.pathname;
  if (pathname === '/') pathname = '/index.html';

  // CORS preflight
  if (req.method === 'OPTIONS') {
    res.writeHead(204, {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type'
    });
    res.end();
    return;
  }

  // API: gerar próximo client_ref sequencial (global)
  if (pathname === '/api/next-client-ref' && req.method === 'GET') {
    try {
      if (!db) return sendJson(res, 400, { ok: false, error: 'MongoDB não configurado (MONGO_URI ausente)' });
      const now = new Date();
      const seqRes = await db.collection('counters').findOneAndUpdate(
        { _id: 'global:client_ref' },
        { $inc: { seq: 1 }, $setOnInsert: { createdAt: now } },
        { upsert: true, returnDocument: 'after' }
      );
      const client_ref = (seqRes && seqRes.value && seqRes.value.seq) ? seqRes.value.seq : null;
      console.log('[next-client-ref] =>', client_ref);
      return sendJson(res, 200, { ok: true, client_ref });
    } catch (e) {
      console.error('[api/next-client-ref] error', e);
      return sendJson(res, 400, { ok: false, error: String(e.message || e) });
    }
  }

  // API: salvar sessão de clique (com número sequencial por cliente)
  if (pathname === '/api/track' && req.method === 'POST') {
    try {
      const body = await readJson(req);
      const server_ip = getIpFromHeaders(req);
      const now = new Date();
      const doc = Object.assign({}, body, {
        server_ip,
        createdAt: now,
      });

      let click_number = null;

      if (db) {
        const existing = await db.collection('sessions').findOne({ event_id: body.event_id });

        if (!doc.client_ref) {
          const seqClient = await db.collection('counters').findOneAndUpdate(
            { _id: 'global:client_ref' },
            { $inc: { seq: 1 }, $setOnInsert: { createdAt: now } },
            { upsert: true, returnDocument: 'after' }
          );
          doc.client_ref = seqClient && seqClient.value ? seqClient.value.seq : null;
        }

        if (!existing) {
          const counterKey = doc.client_ref ? `client:${doc.client_ref}` : 'global:sessions';
          const seqRes = await db.collection('counters').findOneAndUpdate(
            { _id: counterKey },
            { $inc: { seq: 1 }, $setOnInsert: { createdAt: now } },
            { upsert: true, returnDocument: 'after' }
          );
          click_number = (seqRes && seqRes.value && seqRes.value.seq) ? seqRes.value.seq : 1;
          doc.click_number = click_number;
        } else {
          click_number = existing.click_number || null;
        }

        await db.collection('sessions').updateOne(
          { event_id: body.event_id },
          { $set: doc, $setOnInsert: { insertedAt: now } },
          { upsert: true }
        );
      }

      console.log('[track] event_id=', body.event_id, 'client_ref=', doc.client_ref, 'click_number=', click_number);
      console.log('[track] message=', doc.message);
      sendJson(res, 200, { ok: true, click_number, client_ref: doc.client_ref || null });
    } catch (e) {
      console.error('[api/track] error', e);
      sendJson(res, 400, { ok: false, error: String(e.message || e) });
    }
    return;
  }

  // Webhook: BotConversa (mensagem recebida) → associa telefone ao client_ref
  if (pathname === '/webhook/botconversa' && req.method === 'POST') {
    try {
      const body = await readJson(req);
      const server_ip = getIpFromHeaders(req);
      const text = String(body.text || body.message || '');
      const from = String(body.from || body.phone || '').replace(/[^0-9+]/g, '');
      const mCliente = text.match(/cliente#([A-Za-z0-9_-]+)/i);
      const client_ref = mCliente ? mCliente[1] : (body.client_ref || null);
      const now = new Date();

      if (db) {
        await db.collection('messages').insertOne({
          text,
          from,
          client_ref,
          server_ip,
          createdAt: now
        });
        if (client_ref) {
          await db.collection('sessions').updateMany(
            { client_ref },
            { $set: { user_phone: from, whatsapp_received_at: now, last_message_text: text } }
          );
        }
      }
      sendJson(res, 200, { ok: true, client_ref, from });
    } catch (e) {
      console.error('[webhook/botconversa] error', e);
      sendJson(res, 400, { ok: false, error: String(e.message || e) });
    }
    return;
  }

  // Webhook: pagamento → salva compra e tenta vincular a sessão
  if (pathname === '/webhook/payment' && req.method === 'POST') {
    try {
      const body = await readJson(req);
      const server_ip = getIpFromHeaders(req);
      const now = new Date();

      const doc = {
        order_id: body.order_id,
        transaction_id: body.transaction_id,
        status: body.status,
        value: body.value != null ? Number(body.value) : undefined,
        currency: body.currency || 'BRL',
        email: body.email || null,
        phone: body.phone || null,
        client_ref: body.client_ref || null,
        event_id: body.event_id || null,
        session_id: body.session_id || null,
        server_ip,
        timestamp: body.timestamp || now.toISOString(),
        createdAt: now
      };

      if (db) {
        await db.collection('payments').updateOne(
          { transaction_id: doc.transaction_id || doc.order_id || `${Date.now()}-${Math.random()}` },
          { $set: doc, $setOnInsert: { insertedAt: now } },
          { upsert: true }
        );

        let sessionQuery = null;
        if (doc.event_id) sessionQuery = { event_id: doc.event_id };
        else if (doc.client_ref) sessionQuery = { client_ref: doc.client_ref };
        else if (doc.phone) sessionQuery = { user_phone: doc.phone };
        if (sessionQuery) {
          await db.collection('sessions').updateMany(
            sessionQuery,
            { $set: { last_purchase_at: now, last_purchase_status: doc.status } }
          );
        }
      }
      sendJson(res, 200, { ok: true });
    } catch (e) {
      console.error('[webhook/payment] error', e);
      sendJson(res, 400, { ok: false, error: String(e.message || e) });
    }
    return;
  }

  const fsPath = safeJoin(ROOT, pathname);
  if (!fsPath) {
    res.writeHead(400, { 'Content-Type': 'text/plain' });
    res.end('Bad Request');
    return;
  }

  fs.stat(fsPath, (err, stat) => {
    if (err) {
      const maybeIndex = path.join(fsPath, 'index.html');
      fs.stat(maybeIndex, (err2, stat2) => {
        if (!err2 && stat2.isFile()) return serveFile(res, maybeIndex);
        res.writeHead(404, { 'Content-Type': 'text/plain' });
        res.end('Not Found');
      });
      return;
    }
    if (stat.isDirectory()) {
      const indexPath = path.join(fsPath, 'index.html');
      return serveFile(res, indexPath);
    }
    serveFile(res, fsPath);
  });
});

initMongo().then(() => {
  server.listen(PORT, () => {
    console.log(`Server running at http://localhost:${PORT}/`);
  });
}).catch(err => {
  console.error('[Mongo] Falha ao conectar:', err);
  server.listen(PORT, () => {
    console.log(`Server running at http://localhost:${PORT}/ (sem MongoDB)`);
  });
});