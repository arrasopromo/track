const http = require('http');
const fs = require('fs');
const path = require('path');
const { MongoClient } = require('mongodb');
const https = require('https');

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

// Meta Conversions API
const PIXEL_ID = process.env.META_PIXEL_ID || process.env.PIXEL_ID || '';
const META_CAPI_TOKEN = process.env.META_CAPI_TOKEN || process.env.META_PIXEL_TOKEN || process.env.ACCESS_TOKEN || '';
const TEST_EVENT_CODE = process.env.TEST_EVENT_CODE || process.env.META_TEST_EVENT_CODE || '';

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
let LAST_CAPI = { pageview: null, contact: null };

async function initMongo() {
  if (!MONGO_URI) {
    console.warn('[Mongo] MONGO_URI não definido. O backend funcionará sem persistência.');
    return;
  }
  mongoClient = new MongoClient(MONGO_URI, { maxPoolSize: 10 });
  await mongoClient.connect();
  db = mongoClient.db(MONGO_DB_NAME);
  try { await db.collection('sessions').dropIndex('event_id_1'); } catch (_) {}
  await db.collection('sessions').createIndex(
    { event_id: 1 },
    { unique: true, partialFilterExpression: { event_id: { $exists: true, $ne: null } } }
  ).catch(() => {});
  await db.collection('sessions').createIndex({ client_ref: 1 }).catch(() => {});
  await db.collection('sessions').createIndex({ createdAt: 1 }).catch(() => {});
  await db.collection('messages').createIndex({ from: 1, createdAt: 1 }).catch(() => {});
  await db.collection('payments').createIndex({ transaction_id: 1 }, { unique: true }).catch(() => {});
  // Seed do contador global de client_ref, evitando conflito de operadores
  try {
    const now = new Date();
    // Cria seed apenas se não existir
    await db.collection('counters').updateOne(
      { _id: 'global:client_ref' },
      { $setOnInsert: { seq: CLIENT_REF_SEED, createdAt: now } },
      { upsert: true }
    );

    // Lê o valor atual do contador
    const current = await db.collection('counters').findOne({ _id: 'global:client_ref' });
    const currentSeq = current && typeof current.seq === 'number' ? current.seq : CLIENT_REF_SEED;

    // Descobre o maior client_ref já salvo nas sessões (apenas números)
    let maxSessionRef = null;
    try {
      const agg = await db.collection('sessions').aggregate([
        { $match: { client_ref: { $exists: true, $ne: null } } },
        { $project: { n: { $convert: { input: '$client_ref', to: 'int', onError: null, onNull: null } } } },
        { $match: { n: { $ne: null } } },
        { $group: { _id: null, max: { $max: '$n' } } }
      ]).toArray();
      if (agg && agg.length && typeof agg[0].max === 'number') {
        maxSessionRef = agg[0].max;
      }
    } catch (e) {
      console.warn('[Mongo] Falha ao calcular max client_ref em sessions:', e && e.message ? e.message : e);
    }

    const targetSeq = Math.max(CLIENT_REF_SEED, currentSeq, maxSessionRef || CLIENT_REF_SEED);

    if (CLIENT_REF_FORCE) {
      // Em modo FORCE, nunca reduzir: ajusta para o maior valor conhecido
      await db.collection('counters').updateOne(
        { _id: 'global:client_ref' },
        { $set: { seq: targetSeq } },
        { upsert: true }
      );
      console.log('[Mongo] FORCE global:client_ref =>', targetSeq);
    } else if (currentSeq < targetSeq) {
      // Sem FORCE, somente corrige para frente se necessário
      await db.collection('counters').updateOne(
        { _id: 'global:client_ref' },
        { $set: { seq: targetSeq } }
      );
      console.log('[Mongo] Ajuste global:client_ref =>', targetSeq, '(antes:', currentSeq, ')');
    } else {
      console.log('[Mongo] Seed global:client_ref =>', CLIENT_REF_SEED, '(atual:', currentSeq, ', maxSessions:', maxSessionRef, ')');
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
      try {
        if (!data) return resolve({});
        const ct = String(req.headers['content-type'] || '').toLowerCase();
        if (ct.includes('application/json')) {
          return resolve(JSON.parse(data));
        }
        if (ct.includes('application/x-www-form-urlencoded')) {
          const params = new URLSearchParams(data);
          const obj = {};
          for (const [k, v] of params.entries()) obj[k] = v;
          return resolve(obj);
        }
        // Fallback: tenta JSON, senão interpreta como urlencoded
        try { return resolve(JSON.parse(data)); } catch (_) {
          const params = new URLSearchParams(data);
          const obj = {};
          for (const [k, v] of params.entries()) obj[k] = v;
          return resolve(obj);
        }
      } catch (e) { reject(e); }
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

  if (pathname === '/api/client-ref-stats' && req.method === 'GET') {
    try {
      if (!db) return sendJson(res, 400, { ok: false, error: 'MongoDB não configurado (MONGO_URI ausente)' });
      const counter = await db.collection('counters').findOne({ _id: 'global:client_ref' });
      const currentSeq = counter && typeof counter.seq === 'number' ? counter.seq : null;
      let maxSessionRef = null;
      let sessionsCount = null;
      try {
        const agg = await db.collection('sessions').aggregate([
          { $match: { client_ref: { $exists: true, $ne: null } } },
          { $project: { n: { $convert: { input: '$client_ref', to: 'int', onError: null, onNull: null } } } },
          { $match: { n: { $ne: null } } },
          { $group: { _id: null, max: { $max: '$n' }, count: { $sum: 1 } } }
        ]).toArray();
        if (agg && agg.length) {
          maxSessionRef = typeof agg[0].max === 'number' ? agg[0].max : null;
          sessionsCount = typeof agg[0].count === 'number' ? agg[0].count : null;
        }
      } catch (e) {}
      return sendJson(res, 200, { ok: true, currentSeq, maxSessionRef, sessionsCount });
    } catch (e) {
      console.error('[api/client-ref-stats] error', e);
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

  // Debug: testar CAPI diretamente
  if (pathname === '/debug/capi-test' && req.method === 'GET') {
    try {
      if (!PIXEL_ID || !META_CAPI_TOKEN) return sendJson(res, 400, { ok: false, error: 'PIXEL_ID/META_CAPI_TOKEN ausentes' });
      const payload = {
        data: [
          {
            event_name: 'PageView',
            event_time: Math.floor(Date.now() / 1000),
            action_source: 'website',
            event_source_url: 'https://track.agenciaoppus.site/'
          }
        ]
      };
      if (TEST_EVENT_CODE) payload.test_event_code = TEST_EVENT_CODE;
      const resp = await postToMetaEvents(payload);
      return sendJson(res, 200, { ok: true, status: resp && resp.status || null, body: resp && resp.body || null });
    } catch (e) {
      return sendJson(res, 400, { ok: false, error: String(e.message || e) });
    }
  }

  if (pathname === '/debug/capi-status' && req.method === 'GET') {
    try {
      const status = {
        ok: true,
        hasPixelId: !!PIXEL_ID,
        hasToken: !!META_CAPI_TOKEN,
        hasDb: !!db,
        testEventCode: !!TEST_EVENT_CODE,
        last: LAST_CAPI
      };
      return sendJson(res, 200, status);
    } catch (e) {
      return sendJson(res, 400, { ok: false, error: String(e.message || e) });
    }
  }


  // Webhook: BotConversa (mensagem recebida) → associa telefone ao client_ref
  if (pathname === '/webhook/botconversa' && req.method === 'POST') {
    try {
      const body = await readJson(req);
      const server_ip = getIpFromHeaders(req);
      const text = String(body.text || body.message || '');
      const rawFrom = body.from || body.phone || body.tel || body.telefone || url.searchParams.get('from') || url.searchParams.get('phone') || url.searchParams.get('tel') || url.searchParams.get('telefone') || '';
      const from = String(rawFrom).replace(/[^0-9+]/g, '');
      const mCliente = text.match(/cliente#([A-Za-z0-9_-]+)/i);
      const rawClientRef = mCliente ? mCliente[1] : (body.client_ref || body.clientRef || body.idcliente || body.id || url.searchParams.get('client_ref') || url.searchParams.get('clientRef') || url.searchParams.get('idcliente') || url.searchParams.get('id') || null);
      const client_ref = (rawClientRef !== null && rawClientRef !== undefined) ? String(rawClientRef) : null;
      const now = new Date();

      let triggerSend = false;
      if (db) {
        await db.collection('messages').insertOne({
          text,
          from,
          client_ref,
          server_ip,
          createdAt: now
        });
        triggerSend = !!client_ref;
        if (client_ref) {
          const existing = await db.collection('sessions').findOne({ client_ref });
          if (existing) {
            await db.collection('sessions').updateMany(
              { client_ref },
              { $set: { user_phone: from, whatsapp_received_at: now, last_message_text: text } }
            );
          } else {
            await db.collection('sessions').insertOne({
              client_ref,
              user_phone: from,
              whatsapp_received_at: now,
              last_message_text: text,
              server_ip,
              createdAt: now
            });
          }
        }
      }
      try {
        if (triggerSend) {
          await sendPixelPageView({ client_ref, server_ip });
          let sess = null;
          if (client_ref && db) sess = await db.collection('sessions').findOne({ client_ref });
          if (!sess) {
            sess = { client_ref, event_source_url: 'https://track.agenciaoppus.site/', user_agent: null, fbc: null, fbp: null, session_id: null, event_id: null, timestamp: now.toISOString(), server_ip };
          }
          await sendMetaContactFromSession(sess, server_ip);
        }
      } catch (_) {}
      sendJson(res, 200, { ok: true, client_ref, from, capi: { pageview: LAST_CAPI.pageview, contact: LAST_CAPI.contact } });
    } catch (e) {
      console.error('[webhook/botconversa] error', e);
      sendJson(res, 400, { ok: false, error: String(e.message || e) });
    }
    return;
  }

  if (pathname === '/webhook/botconversa' && req.method === 'GET') {
    try {
      const server_ip = getIpFromHeaders(req);
      const now = new Date();
      const rawFrom = url.searchParams.get('from') || url.searchParams.get('phone') || url.searchParams.get('tel') || url.searchParams.get('telefone') || '';
      const from = String(rawFrom).replace(/[^0-9+]/g, '');
      const rawClientRef = url.searchParams.get('client_ref') || url.searchParams.get('clientRef') || url.searchParams.get('id') || url.searchParams.get('idcliente') || null;
      const client_ref = (rawClientRef !== null && rawClientRef !== undefined) ? String(rawClientRef) : null;

      if (db) {
        await db.collection('messages').insertOne({ type: 'botconversa-get', from, client_ref, server_ip, createdAt: now });
        if (client_ref) {
          const existing = await db.collection('sessions').findOne({ client_ref });
          if (existing) {
            await db.collection('sessions').updateMany(
              { client_ref },
              { $set: { user_phone: from, whatsapp_received_at: now } }
            );
          } else {
            await db.collection('sessions').insertOne({ client_ref, user_phone: from, whatsapp_received_at: now, server_ip, createdAt: now });
          }
        }
      }
      if (client_ref) {
        try {
          await sendPixelPageView({ client_ref, server_ip });
          let sess = null;
          if (client_ref && db) sess = await db.collection('sessions').findOne({ client_ref });
          if (!sess) sess = { client_ref, event_source_url: 'https://track.agenciaoppus.site/', timestamp: now.toISOString(), server_ip };
          await sendMetaContactFromSession(sess, server_ip);
        } catch (_) {}
      }
      sendJson(res, 200, { ok: true, client_ref, from, capi: { pageview: LAST_CAPI.pageview, contact: LAST_CAPI.contact } });
    } catch (e) {
      console.error('[webhook/botconversa GET] error', e);
      sendJson(res, 400, { ok: false, error: String(e.message || e) });
    }
    return;
  }

  // Webhook: track-cliente → captura client_ref/id e associa telefone
  if (pathname === '/webhook/track-cliente' && req.method === 'GET') {
    return sendJson(res, 200, {
      ok: true,
      hint: 'Use POST application/json',
      example: { id: '23057', from: '+5511999999999', text: 'cliente#23057' }
    });
  }

  if (pathname === '/webhook/track-cliente' && req.method === 'POST') {
    try {
      const body = await readJson(req);
      const server_ip = getIpFromHeaders(req);
      const now = new Date();

      const text = String(body.text || body.message || '');
      const from = String(body.from || body.phone || url.searchParams.get('from') || url.searchParams.get('phone') || '').replace(/[^0-9+]/g, '');
      let client_ref = null;

      if (body.id != null) client_ref = String(body.id);
      else if (body.client_ref != null) client_ref = String(body.client_ref);
      else if (body.clientRef != null) client_ref = String(body.clientRef);
      else if (url.searchParams.get('client_ref')) client_ref = String(url.searchParams.get('client_ref'));
      else if (url.searchParams.get('clientRef')) client_ref = String(url.searchParams.get('clientRef'));
      else if (url.searchParams.get('id')) client_ref = String(url.searchParams.get('id'));
      else if (url.searchParams.get('idcliente')) client_ref = String(url.searchParams.get('idcliente'));
      else {
        const m = text.match(/cliente#([A-Za-z0-9_-]+)/i);
        client_ref = m ? m[1] : null;
      }

      let triggerSend2 = false;
      if (db) {
        await db.collection('messages').insertOne({
          type: 'track-cliente',
          text,
          from,
          client_ref,
          server_ip,
          payload: body,
          createdAt: now
        });
        triggerSend2 = !!client_ref;
        if (client_ref) {
          const existing = await db.collection('sessions').findOne({ client_ref });
          if (existing) {
            await db.collection('sessions').updateMany(
              { client_ref },
              { $set: { user_phone: from, whatsapp_received_at: now, last_message_text: text } }
            );
          } else {
            await db.collection('sessions').insertOne({
              client_ref,
              user_phone: from,
              whatsapp_received_at: now,
              last_message_text: text,
              server_ip,
              createdAt: now
            });
          }
        }
      }
      try {
        if (triggerSend2) {
          await sendPixelPageView({ client_ref, server_ip });
          let sess = null;
          if (client_ref && db) sess = await db.collection('sessions').findOne({ client_ref });
          if (!sess) {
            sess = { client_ref, event_source_url: 'https://track.agenciaoppus.site/', user_agent: null, fbc: null, fbp: null, session_id: null, event_id: null, timestamp: now.toISOString(), server_ip };
          }
          await sendMetaContactFromSession(sess, server_ip);
        }
      } catch (_) {}
      sendJson(res, 200, { ok: true, client_ref, from, capi: { pageview: LAST_CAPI.pageview, contact: LAST_CAPI.contact } });
    } catch (e) {
      console.error('[webhook/track-cliente] error', e);
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


function postToMetaEvents(payload) {
  if (!PIXEL_ID || !META_CAPI_TOKEN) return Promise.resolve({ status: null, body: 'PIXEL_ID or META_CAPI_TOKEN missing' });
  const url = new URL(`https://graph.facebook.com/v19.0/${PIXEL_ID}/events?access_token=${encodeURIComponent(META_CAPI_TOKEN)}`);
  return new Promise((resolve, reject) => {
    const req = https.request({ hostname: url.hostname, path: url.pathname + url.search, method: 'POST', headers: { 'Content-Type': 'application/json' } }, (res) => {
      let data = '';
      res.on('data', (chunk) => { data += chunk; });
      res.on('end', () => { resolve({ status: res.statusCode, body: data }); });
    });
    req.on('error', (err) => reject(err));
    try { req.write(JSON.stringify(payload)); } catch (e) { /* noop */ }
    req.end();
  });
}

async function sendPixelPageView({ client_ref, server_ip }) {
  try {
    if (!PIXEL_ID) { console.warn('[meta] PageView skipped: PIXEL_ID missing'); LAST_CAPI.pageview = { status: null, body: 'PIXEL_ID missing' }; return; }
    if (!META_CAPI_TOKEN) { console.warn('[meta] PageView skipped: META_CAPI_TOKEN missing'); LAST_CAPI.pageview = { status: null, body: 'META_CAPI_TOKEN missing' }; return; }
    let sess = null;
    if (client_ref && db) sess = await db.collection('sessions').findOne({ client_ref });
    const event_source_url = (sess && (sess.event_source_url || sess.page_url)) || 'https://track.agenciaoppus.site/';
    const user_agent = (sess && sess.user_agent) || 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36';
    const fbp = (sess && sess.fbp) || null;
    const fbc = (sess && sess.fbc) || null;
    const payload = { data: [{ event_name: 'PageView', event_id: (sess && sess.event_id) || undefined, event_time: Math.floor(Date.now() / 1000), action_source: 'website', event_source_url, client_ip_address: server_ip || null, client_user_agent: user_agent || null, fbc: fbc || null, fbp: fbp || null, custom_data: { client_ref: client_ref || null } }] };
    if (TEST_EVENT_CODE) payload.test_event_code = TEST_EVENT_CODE;
    const resp = await postToMetaEvents(payload);
    LAST_CAPI.pageview = resp || null;
    if (resp) console.log('[meta] PageView sent', resp.status, resp.body);
  } catch (e) {
    const msg = e && e.message ? e.message : String(e);
    console.warn('[meta] PageView failed', msg);
    LAST_CAPI.pageview = { status: null, body: msg };
  }
}

async function sendMetaContactFromSession(sess, server_ip) {
  try {
    if (!PIXEL_ID) { console.warn('[meta] Contact skipped: PIXEL_ID missing'); LAST_CAPI.contact = { status: null, body: 'PIXEL_ID missing' }; return; }
    if (!META_CAPI_TOKEN) { console.warn('[meta] Contact skipped: META_CAPI_TOKEN missing'); LAST_CAPI.contact = { status: null, body: 'META_CAPI_TOKEN missing' }; return; }
    const event_time = Math.floor((Date.parse(sess.timestamp || new Date().toISOString())) / 1000) || Math.floor(Date.now() / 1000);
    const event_source_url = sess.event_source_url || sess.page_url || 'https://track.agenciaoppus.site/';
    const payload = {
      data: [
        {
          event_name: 'Contact',
          event_id: sess.event_id,
          event_time,
          action_source: 'website',
          event_source_url,
          client_ip_address: server_ip || sess.server_ip || null,
          client_user_agent: (sess.user_agent || 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36'),
          fbc: sess.fbc || null,
          fbp: sess.fbp || null,
          custom_data: {
            utm_source: sess.utm_source || null,
            utm_medium: sess.utm_medium || null,
            utm_campaign: sess.utm_campaign || null,
            utm_content: sess.utm_content || null,
            utm_term: sess.utm_term || null,
            whatsapp_destination: sess.whatsapp_destination || null,
            message: sess.message || null,
            referrer: sess.referrer || null,
            session_id: sess.session_id || null,
            client_ref: sess.client_ref || null
          }
        }
      ]
    };
    if (TEST_EVENT_CODE) payload.test_event_code = TEST_EVENT_CODE;
    const resp = await postToMetaEvents(payload);
    LAST_CAPI.contact = resp || null;
    if (resp) console.log('[meta] Contact sent', resp.status, resp.body);
  } catch (e) {
    const msg = e && e.message ? e.message : String(e);
    console.warn('[meta] Contact failed', msg);
    LAST_CAPI.contact = { status: null, body: msg };
  }
}