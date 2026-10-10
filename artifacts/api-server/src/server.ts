import express, { type NextFunction, type Request, type Response } from "express";
import http from "http";
import { Server } from "socket.io";
import { Pool } from "pg";
import bcrypt from "bcryptjs";
import path from "path";
import fs from "fs";
import cors from "cors";
import { spawn } from "child_process";
import { Storage } from "@google-cloud/storage";
import webpush from "web-push";
import ytSearch from "yt-search";

interface AdminUser {
  username: string;
  is_verified: boolean;
  [key: string]: unknown;
}

declare global {
  namespace Express {
    interface Request {
      adminUser: AdminUser;
    }
  }
}

const app = express();
const server = http.createServer(app);

const ARTIFACT_DIR = path.join(__dirname, "..");
const DATA_DIR = path.join(ARTIFACT_DIR, "data");
const USERS_FILE = path.join(DATA_DIR, "users.json");
const USERS_REGISTRY = path.join(DATA_DIR, "users_registry.json");
const PUBLIC_DIR = path.join(ARTIFACT_DIR, "public");
const UPLOADS_DIR = path.join(PUBLIC_DIR, "uploads");
const AVATAR_DIR = path.join(PUBLIC_DIR, "avatars");

// ── Neon .env loader ───────────────────────────────────────────────────────
// Aponta o app para o Neon (production) quando workspace/.env.local ou .env existe.
// Faz override de DATABASE_URL do Replit (helium) sem nova dependência.
// Ordem: .env.local primeiro (dev), depois .env (publish).
function _loadNeonEnvLocal(): void {
  try {
    const candidates = [
      path.join(process.cwd(), ".env.local"),
      path.join(ARTIFACT_DIR, "..", "..", ".env.local"),
      path.join(ARTIFACT_DIR, ".env.local"),
      path.join(process.cwd(), ".env"),
      path.join(ARTIFACT_DIR, "..", "..", ".env"),
      path.join(ARTIFACT_DIR, ".env"),
    ];
    for (const f of candidates) {
      if (!fs.existsSync(f)) continue;
      const raw = fs.readFileSync(f, "utf8");
      for (const line of raw.split("\n")) {
        const t = line.trim();
        if (!t || t.startsWith("#") || !t.includes("=")) continue;
        const eq = t.indexOf("=");
        const k = t.slice(0, eq).trim();
        let v = t.slice(eq + 1).trim();
        if ((v.startsWith('"') && v.endsWith('"')) || (v.startsWith("'") && v.endsWith("'"))) {
          v = v.slice(1, -1);
        }
        if (k) process.env[k] = v;
      }
      console.log(`[env] loaded Neon config from ${f}`);
      break;
    }
  } catch (e) {
    console.error("[env] failed to load .env.local:", e);
  }
}
_loadNeonEnvLocal();

[PUBLIC_DIR, DATA_DIR, UPLOADS_DIR, AVATAR_DIR].forEach((dir) => {
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
});
if (!fs.existsSync(USERS_FILE)) fs.writeFileSync(USERS_FILE, "[]");

// ── PostgreSQL pool ────────────────────────────────────────────────────────
// The app must never silently fall back to ephemeral local storage. Starting
// without DATABASE_URL would make writes appear to work while they disappear
// after a restart.
const DATABASE_URL = process.env.DATABASE_URL;
if (!DATABASE_URL) {
  throw new Error("DATABASE_URL is required to start REDSPACE.");
}

const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: { rejectUnauthorized: false },
});

try {
  const _u = new URL(DATABASE_URL);
  console.log(`[PostgreSQL] usando ${_u.host}${_u.pathname} (Neon=${_u.host.includes("neon.tech") ? "sim" : "não"})`);
} catch {}

pool.on("error", (error) => {
  console.error("[PostgreSQL] idle client error:", error.message);
});

// ── Web Push (VAPID) ──────────────────────────────────────────────────────
// Keep the private key server-only. The public key is exposed through the
// endpoint below so the browser can create a subscription.
const VAPID_PUBLIC_KEY = (process.env.VAPID_PUBLIC_KEY || "").trim();
const VAPID_PRIVATE_KEY = (process.env.VAPID_PRIVATE_KEY || "").trim();
const VAPID_SUBJECT = (process.env.VAPID_SUBJECT || "mailto:admin@redspace.app").trim();
const pushConfigured = Boolean(VAPID_PUBLIC_KEY && VAPID_PRIVATE_KEY);
if (pushConfigured) {
  webpush.setVapidDetails(
    VAPID_SUBJECT,
    VAPID_PUBLIC_KEY,
    VAPID_PRIVATE_KEY
  );
}

// ── Google Cloud Storage (Replit Object Storage) ──────────────────────────
const REPLIT_SIDECAR_ENDPOINT = "http://127.0.0.1:1106";
let _gcsStorage: InstanceType<typeof Storage> | null = null;
function getGCSStorage() {
  if (!_gcsStorage) {
    _gcsStorage = new Storage({
      credentials: {
        audience: "replit",
        subject_token_type: "access_token",
        token_url: `${REPLIT_SIDECAR_ENDPOINT}/token`,
        type: "external_account",
        credential_source: {
          url: `${REPLIT_SIDECAR_ENDPOINT}/credential`,
          format: { type: "json", subject_token_field_name: "access_token" },
        },
        universe_domain: "googleapis.com",
      },
      projectId: "",
    } as any);
  }
  return _gcsStorage;
}
let _gcsBucket: ReturnType<InstanceType<typeof Storage>["bucket"]> | null = null;
function getGCSBucket() {
  if (!_gcsBucket) {
    const bucketId = process.env.DEFAULT_OBJECT_STORAGE_BUCKET_ID;
    if (!bucketId) return null;
    try { _gcsBucket = getGCSStorage().bucket(bucketId); } catch { return null; }
  }
  return _gcsBucket;
}

// ── Permanent-persistence queue ────────────────────────────────────────────
// Any buffer that fails to reach GCS after its immediate retries is pushed
// here. A background sweeper keeps retrying FOREVER (every 30s) until the
// upload succeeds, so a transient GCS outage can NEVER cause permanent data
// loss as long as the local file (already written synchronously) survives.
//
// IMPORTANT: this queue is also mirrored to disk (PENDING_QUEUE_FILE) on every
// change. If the process crashes or restarts before a queued item succeeds,
// the queue is NOT lost — it's reloaded from disk on startup and retried.
// This is on top of (not instead of) syncLocalFilesToGCS(), which is an
// independent, disk-scanning recovery path: even if the pending-queue file
// itself were somehow lost, any local file not yet in GCS gets picked up and
// uploaded on the next server start, because it scans the actual uploads/
// avatars directories rather than relying on in-memory bookkeeping.
const PENDING_QUEUE_FILE = path.join(DATA_DIR, "gcs_pending_queue.json");
type PendingGcsItem = { gcsPath: string; mimeType: string; localAbsPath: string };
let _gcsPendingQueue: PendingGcsItem[] = [];

function _persistPendingQueue() {
  try { fs.writeFileSync(PENDING_QUEUE_FILE, JSON.stringify(_gcsPendingQueue)); } catch (e) {
    console.error("[GCS queue] failed to persist pending queue to disk:", e);
  }
}

function _loadPendingQueueFromDisk() {
  try {
    if (fs.existsSync(PENDING_QUEUE_FILE)) {
      const raw = fs.readFileSync(PENDING_QUEUE_FILE, "utf8");
      const parsed = JSON.parse(raw);
      if (Array.isArray(parsed)) {
        _gcsPendingQueue = parsed.filter((it: any) => it && it.localAbsPath && fs.existsSync(it.localAbsPath));
        if (_gcsPendingQueue.length > 0) console.log(`[GCS queue] restored ${_gcsPendingQueue.length} pending upload(s) from disk after restart`);
      }
    }
  } catch (e) { console.error("[GCS queue] failed to load pending queue from disk:", e); }
}

function _queuePendingUpload(gcsPath: string, mimeType: string, localAbsPath: string) {
  if (_gcsPendingQueue.some((it) => it.gcsPath === gcsPath)) return;
  _gcsPendingQueue.push({ gcsPath, mimeType, localAbsPath });
  _persistPendingQueue();
}

// Durable outbox write: persists the ACTUAL BYTES to Postgres so the media
// survives even if local disk AND the in-memory/JSON queue are wiped by a
// container recycle before the GCS upload succeeds. Awaited by callers so
// the guarantee is in place before the HTTP response is sent.
// Fail-closed: throws on failure instead of swallowing, so callers (gcsUploadWithRetry)
// know for certain whether the bytes are durably safe somewhere. If this throws AND
// GCS is also unreachable, the media genuinely has nowhere durable to live yet — the
// caller must treat that as a hard failure rather than silently proceeding with only
// a local-disk copy.
async function _persistPendingUploadToDB(gcsPath: string, mimeType: string, buffer: Buffer): Promise<void> {
  await pool.query(
    `INSERT INTO media_pending_uploads (gcs_path, mime_type, base64_data) VALUES ($1, $2, $3)
     ON CONFLICT (gcs_path) DO UPDATE SET mime_type = $2, base64_data = $3`,
    [gcsPath, mimeType, buffer.toString("base64")]
  );
}

async function _clearPendingUploadFromDB(gcsPath: string): Promise<void> {
  try { await pool.query(`DELETE FROM media_pending_uploads WHERE gcs_path = $1`, [gcsPath]); } catch {}
}

// Upload with retry — awaited before returning path, so files are ALWAYS in GCS
// (or, worst case, safely queued with a durable on-disk record + the file
// itself still on local disk, which is exactly what syncLocalFilesToGCS also
// scans for on every startup as a second, independent safety net).
// Returns true if the bytes are now durably safe SOMEWHERE (GCS success, or the
// Postgres outbox insert succeeded). Throws if NEITHER succeeded — callers must
// treat that as a hard failure (do not commit a DB row referencing this media),
// because at that point the only copy is on ephemeral local disk.
async function gcsUploadWithRetry(buffer: Buffer, gcsPath: string, mimeType: string, retries = 5, localAbsPath?: string): Promise<boolean> {
  const bucket = getGCSBucket();
  if (!bucket) {
    console.error("[GCS] bucket not configured, queuing for later:", gcsPath);
    await _persistPendingUploadToDB(gcsPath, mimeType, buffer); // throws if this also fails
    if (localAbsPath) _queuePendingUpload(gcsPath, mimeType, localAbsPath);
    return false;
  }
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      await bucket.file(gcsPath).save(buffer, { contentType: mimeType, resumable: false });
      return true;
    } catch (e: any) {
      console.error(`[GCS upload] attempt ${attempt}/${retries} for ${gcsPath}: ${e.message}`);
      if (attempt < retries) await new Promise(r => setTimeout(r, 800 * attempt));
    }
  }
  console.error(`[GCS upload] all ${retries} attempts failed for ${gcsPath} — queued for permanent retry`);
  // Durable outbox write — the bytes are now safe in Postgres regardless of what
  // happens to local disk or the in-memory queue from this point forward.
  // Fail-closed: if THIS also fails, propagate so the caller aborts the save.
  await _persistPendingUploadToDB(gcsPath, mimeType, buffer);
  if (localAbsPath) _queuePendingUpload(gcsPath, mimeType, localAbsPath);
  return false;
}

// Sweeper: retries every queued (previously failed) upload every 30s, forever,
// until every single one succeeds. This is the last line of defense against
// any media ever being lost due to a temporary cloud storage failure — and it
// survives process restarts because the queue is reloaded from disk on boot.
setInterval(async () => {
  const bucket = getGCSBucket();
  if (!bucket) return;

  // Fast path: items still known in memory (usually backed by a local file).
  if (_gcsPendingQueue.length > 0) {
    const batch = _gcsPendingQueue.splice(0, _gcsPendingQueue.length);
    let changed = false;
    for (const item of batch) {
      try {
        if (!fs.existsSync(item.localAbsPath)) {
          // Local file is gone — DO NOT drop the item, fall through to the
          // Postgres-backed pass below which still has the actual bytes.
          changed = true;
          continue;
        }
        const buf = fs.readFileSync(item.localAbsPath);
        await bucket.file(item.gcsPath).save(buf, { contentType: item.mimeType, resumable: false });
        await _clearPendingUploadFromDB(item.gcsPath);
        console.log(`[GCS retry-queue] recovered pending upload: ${item.gcsPath}`);
        changed = true;
      } catch (e: any) {
        console.error(`[GCS retry-queue] still failing for ${item.gcsPath}: ${e.message} — re-queuing`);
        _gcsPendingQueue.push(item);
      }
    }
    if (changed) _persistPendingQueue();
  }

  // Authoritative path: Postgres is the real source of truth for pending
  // uploads. This runs every tick regardless of in-memory/local-disk state,
  // so it recovers uploads even if the container was fully recycled and lost
  // both the local file and the in-memory/JSON queue — the bytes are safe
  // here and this is what finally guarantees zero permanent media loss.
  try {
    const { rows } = await pool.query(`SELECT gcs_path, mime_type, base64_data FROM media_pending_uploads LIMIT 25`);
    for (const row of rows) {
      try {
        const buf = Buffer.from(row.base64_data, "base64");
        await bucket.file(row.gcs_path).save(buf, { contentType: row.mime_type, resumable: false });
        await _clearPendingUploadFromDB(row.gcs_path);
        console.log(`[media outbox] recovered pending upload from Postgres: ${row.gcs_path}`);
      } catch (e: any) {
        console.error(`[media outbox] still failing for ${row.gcs_path}: ${e.message}`);
      }
    }
  } catch (e: any) {
    console.error("[media outbox] sweep query failed:", e.message);
  }
}, 30_000);

// Last-resort fallback: if a file is missing from BOTH local disk and GCS,
// it may still be sitting safely in the Postgres durable outbox waiting for
// the sweeper to flush it (e.g. right after a container recycle, before the
// next 30s sweep runs). Checking here means a client NEVER sees "unavailable"
// for media that legitimately exists somewhere in the persistence chain.
async function _serveFromPendingOutbox(gcsPath: string, res: express.Response): Promise<boolean> {
  try {
    const { rows } = await pool.query(`SELECT mime_type, base64_data FROM media_pending_uploads WHERE gcs_path = $1`, [gcsPath]);
    if (rows.length === 0) return false;
    const buf = Buffer.from(rows[0].base64_data, "base64");
    res.setHeader("Content-Type", rows[0].mime_type || "application/octet-stream");
    res.setHeader("Cache-Control", "no-store"); // not yet durably in GCS — don't let clients cache this forever
    res.send(buf);
    return true;
  } catch (e: any) {
    console.error("[media outbox] serve fallback failed:", e.message);
    return false;
  }
}

async function gcsServeFile(req: express.Request, res: express.Response): Promise<void> {
  const bucket = getGCSBucket();
  const gcsPath = req.path.startsWith("/") ? req.path.slice(1) : req.path;
  if (!bucket) { if (await _serveFromPendingOutbox(gcsPath, res)) return; res.status(404).end(); return; }
  try {
    const file = bucket.file(gcsPath);
    const [exists] = await file.exists();
    if (!exists) { if (await _serveFromPendingOutbox(gcsPath, res)) return; res.status(404).end(); return; }
    // Detect content-type from extension (more reliable than GCS metadata)
    const ext = path.extname(gcsPath).toLowerCase();
    const mimeMap: Record<string, string> = {
      '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg', '.png': 'image/png',
      '.gif': 'image/gif', '.webp': 'image/webp', '.mp3': 'audio/mpeg',
      '.mp4': 'video/mp4', '.webm': 'audio/webm', '.ogg': 'audio/ogg',
      '.wav': 'audio/wav',
    };
    const ct = mimeMap[ext] || 'application/octet-stream';
    res.setHeader("Content-Type", ct);
    res.setHeader("Cache-Control", "public, max-age=31536000");
    res.setHeader("Accept-Ranges", "bytes");
    file.createReadStream().pipe(res);
  } catch (e: any) {
    console.error("[GCS serve]", gcsPath, e.message);
    if (await _serveFromPendingOutbox(gcsPath, res)) return;
    res.status(500).end();
  }
}

// Sync any local files to GCS that aren't there yet (runs on startup)
async function syncLocalFilesToGCS(): Promise<void> {
  const bucket = getGCSBucket();
  if (!bucket) return;
  const dirs = [
    { localDir: UPLOADS_DIR, gcsPrefix: "uploads" },
    { localDir: AVATAR_DIR, gcsPrefix: "avatars" },
  ];
  let uploaded = 0;
  const mimeMap: Record<string, string> = {
    '.jpg':'image/jpeg','.jpeg':'image/jpeg','.png':'image/png','.gif':'image/gif',
    '.webp':'image/webp','.mp3':'audio/mpeg','.mp4':'video/mp4','.webm':'audio/webm',
    '.ogg':'audio/ogg','.wav':'audio/wav',
  };
  for (const { localDir, gcsPrefix } of dirs) {
    if (!fs.existsSync(localDir)) continue;
    const files = fs.readdirSync(localDir);
    for (const file of files) {
      const gcsPath = `${gcsPrefix}/${file}`;
      try {
        const [exists] = await bucket.file(gcsPath).exists();
        if (!exists) {
          const buf = fs.readFileSync(path.join(localDir, file));
          const ct = mimeMap[path.extname(file).toLowerCase()] || "application/octet-stream";
          await bucket.file(gcsPath).save(buf, { contentType: ct, resumable: false });
          uploaded++;
        }
      } catch {}
    }
  }
  if (uploaded > 0) console.log(`[GCS sync] synced ${uploaded} local file(s) to GCS`);
}

// ── SQLite-compatible wrapper around pg ───────────────────────────────────
// Converts ? placeholders to $1…$N and handles INSERT OR IGNORE syntax.
function _pgPreprocess(sql: string): string {
  const hadIgnore = /INSERT\s+OR\s+IGNORE\s+INTO/i.test(sql);
  let processed = sql.replace(/INSERT\s+OR\s+IGNORE\s+INTO/gi, "INSERT INTO");
  let n = 0;
  processed = processed.replace(/\?/g, () => `$${++n}`);
  if (hadIgnore && !/ON\s+CONFLICT/i.test(processed)) {
    processed = processed + " ON CONFLICT DO NOTHING";
  }
  return processed;
}

const db = {
  serialize: (fn: () => void) => fn(),

  run: (sql: string, params?: any[], callback?: (this: { lastID: number; changes: number }, err: any) => void) => {
    const pgSql = _pgPreprocess(sql);
    const isInsert = /^\s*INSERT\s/i.test(pgSql);
    const finalSql = isInsert && !/RETURNING/i.test(pgSql) ? pgSql + " RETURNING *" : pgSql;
    pool.query(finalSql, params || []).then((result) => {
      const row0 = result.rows && result.rows[0];
      const lastID = row0 && row0.id != null ? Number(row0.id) : 0;
      const changes = result.rowCount || 0;
      if (callback) callback.call({ lastID, changes }, null);
    }).catch((err) => {
      console.error("[pg run]", err.message, "|", sql.substring(0, 80));
      if (callback) callback.call({ lastID: 0, changes: 0 }, err);
    });
  },

  all: (sql: string, params?: any[], callback?: (err: any, rows: any[]) => void) => {
    const pgSql = _pgPreprocess(sql);
    pool.query(pgSql, params || []).then((result) => {
      if (callback) callback(null, result.rows || []);
    }).catch((err) => {
      console.error("[pg all]", err.message, "|", sql.substring(0, 80));
      if (callback) callback(err, []);
    });
  },

  get: (sql: string, params?: any[], callback?: (err: any, row: any) => void) => {
    const pgSql = _pgPreprocess(sql);
    pool.query(pgSql, params || []).then((result) => {
      if (callback) callback(null, (result.rows && result.rows[0]) || null);
    }).catch((err) => {
      console.error("[pg get]", err.message, "|", sql.substring(0, 80));
      if (callback) callback(err, null);
    });
  },
};

// ── Initialize PostgreSQL schema (idempotent) ─────────────────────────────
const _pgInit = async (): Promise<void> => {
  const client = await pool.connect();
  try {
    await client.query(`
      CREATE TABLE IF NOT EXISTS messages (
        id SERIAL PRIMARY KEY, s TEXT NOT NULL, r TEXT NOT NULL,
        c TEXT, type TEXT DEFAULT 'text', status INTEGER DEFAULT 0,
        time TEXT NOT NULL, caption TEXT, reaction TEXT DEFAULT NULL,
        reply_context TEXT DEFAULT NULL, view_once INTEGER DEFAULT 0, viewed INTEGER DEFAULT 0,
        file_name TEXT DEFAULT ''
      )`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_msg_users ON messages(s, r)`);

    await client.query(`
      CREATE TABLE IF NOT EXISTS stories (
        id SERIAL PRIMARY KEY, username TEXT NOT NULL, content TEXT,
        type TEXT DEFAULT 'text', caption TEXT DEFAULT '', bg_color TEXT DEFAULT '#FF3B30',
        viewers TEXT DEFAULT '[]', likes TEXT DEFAULT '[]', time TEXT NOT NULL
      )`);

    await client.query(`
      CREATE TABLE IF NOT EXISTS story_replies (
        id SERIAL PRIMARY KEY, story_id INTEGER NOT NULL,
        username TEXT NOT NULL, message TEXT NOT NULL, time TEXT NOT NULL
      )`);

    await client.query(`
      CREATE TABLE IF NOT EXISTS groups (
        id SERIAL PRIMARY KEY, group_id TEXT UNIQUE NOT NULL, name TEXT NOT NULL,
        description TEXT DEFAULT '', avatar TEXT DEFAULT '', created_by TEXT NOT NULL,
        created_at TEXT NOT NULL, settings TEXT DEFAULT '{}', is_closed INTEGER DEFAULT 0
      )`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_group_id ON groups(group_id)`);

    await client.query(`
      CREATE TABLE IF NOT EXISTS group_members (
        id SERIAL PRIMARY KEY, group_id TEXT NOT NULL, username TEXT NOT NULL,
        role TEXT DEFAULT 'member', joined_at TEXT NOT NULL, muted INTEGER DEFAULT 0,
        UNIQUE(group_id, username)
      )`);

    await client.query(`
      CREATE TABLE IF NOT EXISTS group_messages (
        id SERIAL PRIMARY KEY, group_id TEXT NOT NULL, sender TEXT NOT NULL,
        content TEXT, type TEXT DEFAULT 'text', time TEXT NOT NULL, caption TEXT,
        reaction TEXT DEFAULT NULL, pinned INTEGER DEFAULT 0, reply_to INTEGER DEFAULT NULL,
        reply_context TEXT DEFAULT NULL, file_name TEXT DEFAULT ''
      )`);
    await client.query(`ALTER TABLE messages ADD COLUMN IF NOT EXISTS file_name TEXT DEFAULT ''`);
    await client.query(`ALTER TABLE group_messages ADD COLUMN IF NOT EXISTS file_name TEXT DEFAULT ''`);
    await client.query(`
      CREATE TABLE IF NOT EXISTS poll_votes (
        id SERIAL PRIMARY KEY,
        message_id INTEGER NOT NULL,
        group_id TEXT NOT NULL,
        username TEXT NOT NULL,
        option_index INTEGER NOT NULL,
        created_at TEXT NOT NULL,
        UNIQUE(message_id, username)
      )`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_poll_votes_message ON poll_votes(message_id)`);

    await client.query(`
      CREATE TABLE IF NOT EXISTS friendships (
        id SERIAL PRIMARY KEY, requester TEXT NOT NULL, recipient TEXT NOT NULL,
        status TEXT DEFAULT 'pending', created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
        UNIQUE(requester, recipient)
      )`);

    await client.query(`
      CREATE TABLE IF NOT EXISTS chat_settings (
        id SERIAL PRIMARY KEY, username TEXT NOT NULL, contact_id TEXT NOT NULL,
        is_archived INTEGER DEFAULT 0, is_pinned INTEGER DEFAULT 0,
        UNIQUE(username, contact_id)
      )`);

    await client.query(`
      CREATE TABLE IF NOT EXISTS cinema_rooms (
        code TEXT PRIMARY KEY, name TEXT NOT NULL, created_by TEXT NOT NULL,
        is_public INTEGER DEFAULT 1, current_video_id TEXT DEFAULT '',
        current_video_title TEXT DEFAULT '', current_video_thumb TEXT DEFAULT '',
        is_playing INTEGER DEFAULT 0, player_time REAL DEFAULT 0,
        created_at TEXT NOT NULL, last_active TEXT NOT NULL,
        player_time_at TEXT DEFAULT ''
      )`);

    await client.query(`
      CREATE TABLE IF NOT EXISTS cinema_members (
        id SERIAL PRIMARY KEY, room_code TEXT NOT NULL,
        username TEXT NOT NULL, is_muted INTEGER DEFAULT 0, joined_at TEXT NOT NULL,
        UNIQUE(room_code, username)
      )`);

    await client.query(`
      CREATE TABLE IF NOT EXISTS cinema_invites (
        id SERIAL PRIMARY KEY, room_code TEXT NOT NULL,
        from_user TEXT NOT NULL, to_user TEXT NOT NULL, room_name TEXT NOT NULL,
        status TEXT DEFAULT 'pending', created_at TEXT NOT NULL
      )`);

    await client.query(`
      CREATE TABLE IF NOT EXISTS cinema_chat (
        id SERIAL PRIMARY KEY, room_code TEXT NOT NULL,
        sender TEXT NOT NULL, message TEXT NOT NULL, time TEXT NOT NULL
      )`);

    await client.query(`
      CREATE TABLE IF NOT EXISTS cinema_queue (
        id SERIAL PRIMARY KEY, room_code TEXT NOT NULL,
        video_id TEXT NOT NULL, video_title TEXT NOT NULL, video_thumb TEXT DEFAULT '',
        suggested_by TEXT NOT NULL, status TEXT DEFAULT 'pending',
        position INTEGER DEFAULT 0, created_at TEXT NOT NULL
      )`);

    await client.query(`
      CREATE TABLE IF NOT EXISTS users (
        username TEXT PRIMARY KEY, password TEXT NOT NULL,
        display_name TEXT DEFAULT '', bio TEXT DEFAULT '',
        avatar TEXT DEFAULT '', bg_image TEXT DEFAULT '',
        last_seen TEXT, freeze_last_seen INTEGER DEFAULT 0,
        is_ai INTEGER DEFAULT 0, is_verified INTEGER DEFAULT 0,
        created_at TEXT NOT NULL
      )`);

    await client.query(`ALTER TABLE group_members ADD COLUMN IF NOT EXISTS last_read_at TEXT DEFAULT NULL`);
    await client.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS is_vip INTEGER DEFAULT 0`);
    await client.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS vip_expires_at TEXT DEFAULT NULL`);

    await client.query(`
      CREATE TABLE IF NOT EXISTS user_stickers (
        id SERIAL PRIMARY KEY,
        username TEXT NOT NULL,
        sticker_url TEXT NOT NULL,
        pack_name TEXT DEFAULT 'Meus Stickers',
        is_favorite INTEGER DEFAULT 0,
        created_at TEXT NOT NULL,
        source_path TEXT DEFAULT NULL
      )`);
    await client.query(`ALTER TABLE user_stickers ADD COLUMN IF NOT EXISTS source_path TEXT DEFAULT NULL`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_user_stickers ON user_stickers(username)`);

    await client.query(`
      CREATE TABLE IF NOT EXISTS user_bans (
        id SERIAL PRIMARY KEY,
        username TEXT NOT NULL,
        reason TEXT DEFAULT '',
        ban_type TEXT DEFAULT 'permanent',
        expires_at TEXT DEFAULT NULL,
        banned_by TEXT NOT NULL,
        created_at TEXT NOT NULL
      )`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_user_bans ON user_bans(username)`);

    await client.query(`
      CREATE TABLE IF NOT EXISTS user_blocks (
        id SERIAL PRIMARY KEY,
        blocker TEXT NOT NULL,
        blocked TEXT NOT NULL,
        created_at TEXT NOT NULL,
        UNIQUE(blocker, blocked)
      )`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_user_blocks ON user_blocks(blocker)`);

    await client.query(`
      CREATE TABLE IF NOT EXISTS reports (
        id SERIAL PRIMARY KEY,
        reporter TEXT NOT NULL,
        reported TEXT NOT NULL,
        messages_json TEXT DEFAULT '[]',
        created_at TEXT NOT NULL,
        status TEXT DEFAULT 'pending'
      )`);

    await client.query(`
      CREATE TABLE IF NOT EXISTS appeals (
        id SERIAL PRIMARY KEY,
        username TEXT NOT NULL,
        appeal_text TEXT NOT NULL,
        created_at TEXT NOT NULL,
        status TEXT DEFAULT 'pending',
        admin_note TEXT DEFAULT ''
      )`);

    // Durable media-persistence outbox: when a GCS upload fails, the raw bytes
    // are stored HERE (in Postgres — never on ephemeral container disk) so
    // media can NEVER be permanently lost even if the container is recycled
    // before the upload succeeds. This table is the real safety net; local
    // disk and the in-memory queue are just fast paths on top of it.
    await client.query(`
      CREATE TABLE IF NOT EXISTS media_pending_uploads (
        gcs_path TEXT PRIMARY KEY,
        mime_type TEXT NOT NULL,
        base64_data TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now()
      )`);

    // ── GAMES TABLES ─────────────────────────────────────────────────
    await client.query(`
      CREATE TABLE IF NOT EXISTS game_rooms (
        code TEXT PRIMARY KEY,
        game_type TEXT NOT NULL,
        room_name TEXT DEFAULT '',
        created_by TEXT NOT NULL,
        status TEXT DEFAULT 'waiting',
        state TEXT DEFAULT '{}',
        created_at TEXT NOT NULL,
        last_active TEXT NOT NULL
      )`);

    await client.query(`
      CREATE TABLE IF NOT EXISTS game_members (
        id SERIAL PRIMARY KEY,
        room_code TEXT NOT NULL,
        username TEXT NOT NULL,
        player_index INTEGER DEFAULT 0,
        joined_at TEXT NOT NULL,
        UNIQUE(room_code, username)
      )`);

    await client.query(`
      CREATE TABLE IF NOT EXISTS game_invites (
        id SERIAL PRIMARY KEY,
        room_code TEXT NOT NULL,
        game_type TEXT NOT NULL,
        room_name TEXT NOT NULL,
        from_user TEXT NOT NULL,
        to_user TEXT NOT NULL,
        status TEXT DEFAULT 'pending',
        created_at TEXT NOT NULL
      )`);

    await client.query(`
      CREATE TABLE IF NOT EXISTS push_subscriptions (
        id SERIAL PRIMARY KEY,
        username TEXT NOT NULL,
        endpoint TEXT UNIQUE NOT NULL,
        subscription TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )`);
    await client.query(`CREATE INDEX IF NOT EXISTS idx_push_username ON push_subscriptions(username)`);

    console.log("[REDSPACE] PostgreSQL schema ready");
  } finally {
    client.release();
  }
};

// ── Push Notification Helper ───────────────────────────────────────────────
async function sendPushToUser(username: string, payload: object): Promise<{ sent: number; errors: string[] }> {
  const summary = { sent: 0, errors: [] as string[] };
  if (!pushConfigured) {
    console.error("[push] VAPID keys are not configured — push disabled");
    return summary;
  }
  try {
    const result = await pool.query(
      "SELECT endpoint, subscription FROM push_subscriptions WHERE username = $1",
      [username.toLowerCase()]
    );
    if (result.rows.length === 0) {
      console.error(`[push] no subscriptions for user '${username}' — user never registered a device`);
      return summary;
    }
    for (const row of result.rows) {
      try {
        const sub = JSON.parse(row.subscription);
        await webpush.sendNotification(sub, JSON.stringify(payload));
        summary.sent++;
        console.error(`[push] ✓ sent to ${username} endpoint=${row.endpoint.substring(0, 60)}...`);
      } catch (err: any) {
        const code = err.statusCode || err.code || "unknown";
        const msg = `${code}: ${err.message || ""}`;
        summary.errors.push(msg);
        console.error(`[push] ✗ failed for ${username} — ${msg} — endpoint=${row.endpoint.substring(0, 60)}...`);
        if (err.statusCode === 410 || err.statusCode === 404) {
          await pool.query("DELETE FROM push_subscriptions WHERE endpoint = $1", [row.endpoint]).catch(() => {});
          console.error(`[push] removed expired subscription for ${username}`);
        }
      }
    }
  } catch (err: any) {
    console.error(`[push] DB error for ${username}: ${err.message}`);
  }
  return summary;
}

// ── SQLite-backed user store ──────────────────────────────────────────────
let _usersCache: any[] = [];

const getUsers = (): any[] => _usersCache;

// Write user data to JSON registry (plain text, committed to git) so accounts
// survive even when the SQLite binary rolls back to an older deployed state.
let _registrySaveTimer: ReturnType<typeof setTimeout> | null = null;
const _flushRegistry = (): void => {
  if (_registrySaveTimer) return;
  _registrySaveTimer = setTimeout(() => {
    _registrySaveTimer = null;
    try {
      const safe = _usersCache.map((u: any) => ({
        username: u.username, password: u.password,
        display_name: u.display_name || "", bio: u.bio || "",
        avatar: u.avatar || "", bg_image: u.bg_image || "",
        last_seen: u.last_seen || null,
        freeze_last_seen: u.freeze_last_seen ? 1 : 0,
        is_ai: u.is_ai ? 1 : 0, is_verified: u.is_verified ? 1 : 0,
        created_at: u.created_at || new Date().toISOString(),
      }));
      fs.writeFileSync(USERS_REGISTRY, JSON.stringify(safe, null, 2));
    } catch (_) {}
  }, 2000);
};

const _dbUpsertUser = async (u: any): Promise<void> => {
  await pool.query(
    `INSERT INTO users (username,password,display_name,bio,avatar,bg_image,last_seen,freeze_last_seen,is_ai,is_verified,is_vip,vip_expires_at,created_at)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
     ON CONFLICT(username) DO UPDATE SET
       password=excluded.password, display_name=excluded.display_name,
       bio=excluded.bio, avatar=excluded.avatar, bg_image=excluded.bg_image,
       last_seen=excluded.last_seen, freeze_last_seen=excluded.freeze_last_seen,
       is_ai=excluded.is_ai, is_verified=excluded.is_verified,
       is_vip=excluded.is_vip, vip_expires_at=excluded.vip_expires_at`,
    [u.username, u.password, u.display_name || "", u.bio || "", u.avatar || "",
     u.bg_image || "", u.last_seen || null, u.freeze_last_seen ? 1 : 0,
     u.is_ai ? 1 : 0, u.is_verified ? 1 : 0,
     u.is_vip ? 1 : 0, u.vip_expires_at || null,
     u.created_at || new Date().toISOString()]
  );
  _flushRegistry();
};

const saveUsers = (users: any[]): void => {
  if (!Array.isArray(users)) return;
  _usersCache = [...users];
  users.forEach((user) => { void _dbUpsertUser(user); });
};

async function ensureRedzinUser(): Promise<void> {
  try {
    const REDZIN_PASS = "022141530";
    const existing = _usersCache.find((u: any) => u.username === "redzin");
    if (!existing) {
      const hash = await bcrypt.hash(REDZIN_PASS, 10);
      const redzin: any = {
        username: "redzin",
        password: hash,
        display_name: "Redzin",
        bio: "👑 Criador da REDSPACE",
        avatar: "",
        bg_image: "",
        last_seen: null,
        freeze_last_seen: false,
        is_ai: false,
        is_verified: true,
        created_at: new Date().toISOString(),
      };
      _usersCache.push(redzin);
      db.run(
        `INSERT OR IGNORE INTO users (username,password,display_name,bio,avatar,bg_image,last_seen,freeze_last_seen,is_ai,is_verified,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)`,
        [redzin.username, redzin.password, redzin.display_name, redzin.bio, redzin.avatar, "", null, 0, 0, 1, redzin.created_at],
        () => {}
      );
    } else {
      if (!existing.is_verified) {
        existing.is_verified = true;
        db.run(`UPDATE users SET is_verified=1 WHERE username='redzin'`, [], () => {});
      }
      existing.bio = "👑 Criador da REDSPACE";
      db.run(`UPDATE users SET bio=? WHERE username='redzin'`, ["👑 Criador da REDSPACE"], () => {});
    }
  } catch (e) {
    console.error("[ensureRedzinUser]", e);
  }
}

async function ensureKuromiUser(): Promise<void> {
  try {
    const existing = _usersCache.find((u: any) => u.username === "kuromi");
    if (!existing) {
      const hash = await bcrypt.hash("kuromi_ai_internal_" + Date.now(), 10);
      const kuromi: any = {
        username: "kuromi",
        password: hash,
        display_name: "Kuromi IA",
        bio: "Kuromi | REDSpace Edition",
        avatar: "https://i.ibb.co/C5bv9wnn/1003902903.jpg",
        bg_image: "",
        last_seen: null,
        freeze_last_seen: true,
        is_ai: true,
        is_verified: true,
        created_at: new Date().toISOString(),
      };
      _usersCache.push(kuromi);
      db.run(
        `INSERT OR IGNORE INTO users (username,password,display_name,bio,avatar,bg_image,last_seen,freeze_last_seen,is_ai,is_verified,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)`,
        [kuromi.username, kuromi.password, kuromi.display_name, kuromi.bio, kuromi.avatar, "", null, 1, 1, 1, kuromi.created_at],
        () => {}
      );
    } else {
      existing.is_ai = true;
      existing.is_verified = true;
      existing.bio = "Kuromi | REDSpace Edition";
      existing.avatar = "https://i.ibb.co/C5bv9wnn/1003902903.jpg";
      db.run(`UPDATE users SET is_ai=1, is_verified=1, avatar=?, bio=? WHERE username='kuromi'`,
        ["https://i.ibb.co/C5bv9wnn/1003902903.jpg",
         "Kuromi | REDSpace Edition"],
        () => {});
    }
    // Auto-friend Kuromi with all existing users (accepted, bidirectional)
    const now = new Date().toISOString();
    _usersCache.forEach((u: any) => {
      if (u.username === "kuromi") return;
      db.run(
        `INSERT OR IGNORE INTO friendships (requester, recipient, status, created_at, updated_at) VALUES ('kuromi', ?, 'accepted', ?, ?)`,
        [u.username, now, now],
        () => {}
      );
    });
  } catch (e) {
    console.error("[ensureKuromiUser]", e);
  }
}

async function ensureCrisalidaUser(): Promise<void> {
  try {
    const existing = _usersCache.find((u: any) => u.username === "crisalida");
    if (!existing) {
      const hash = await bcrypt.hash("crisalida_admin_" + Date.now(), 10);
      const crisalida: any = {
        username: "crisalida",
        password: hash,
        display_name: "Crisálida",
        bio: "🛡️ Módulo administrativo da REDSPACE — acesso restrito.",
        avatar: "https://i.ibb.co/VhV13Jt/1003974468.jpg",
        bg_image: "",
        last_seen: null,
        freeze_last_seen: true,
        is_ai: true,
        is_verified: true,
        is_vip: false,
        vip_expires_at: null,
        created_at: new Date().toISOString(),
      };
      _usersCache.push(crisalida);
      db.run(
        `INSERT OR IGNORE INTO users (username,password,display_name,bio,avatar,bg_image,last_seen,freeze_last_seen,is_ai,is_verified,is_vip,vip_expires_at,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)`,
        [crisalida.username, crisalida.password, crisalida.display_name, crisalida.bio,
         crisalida.avatar, "", null, 1, 1, 1, 0, null, crisalida.created_at],
        () => {}
      );
    } else {
      existing.is_ai = true;
      existing.is_verified = true;
      existing.bio = "🛡️ Módulo administrativo da REDSPACE — acesso restrito.";
      existing.avatar = "https://i.ibb.co/VhV13Jt/1003974468.jpg";
      db.run(
        `UPDATE users SET is_ai=1, is_verified=1, avatar=?, bio=? WHERE username='crisalida'`,
        ["https://i.ibb.co/VhV13Jt/1003974468.jpg", "🛡️ Módulo administrativo da REDSPACE — acesso restrito."],
        () => {}
      );
    }
    // Auto-friend Crisálida ONLY with redzin
    const now = new Date().toISOString();
    db.run(
      `INSERT OR IGNORE INTO friendships (requester, recipient, status, created_at, updated_at) VALUES ('crisalida', 'redzin', 'accepted', ?, ?)`,
      [now, now], () => {}
    );
  } catch (e) {
    console.error("[ensureCrisalidaUser]", e);
  }
}

const loadUsersFromDB = async (): Promise<void> => {
  const rows = await new Promise<any[]>((resolve, reject) => {
    db.all(`SELECT * FROM users ORDER BY created_at ASC`, [], (error: any, result: any[]) => {
      if (error) {
        reject(error);
        return;
      }
      resolve(result || []);
    });
  });

  if (rows.length > 0) {
    _usersCache = rows.map((r: any) => ({
      ...r,
      freeze_last_seen: !!r.freeze_last_seen,
      is_ai: !!r.is_ai,
      is_verified: !!r.is_verified,
      is_vip: !!r.is_vip,
      vip_expires_at: r.vip_expires_at || null,
      is_online: false,
    }));
  } else {
    // Try legacy users.json migration only when PostgreSQL is reachable and
    // genuinely empty. A database error must reject startup, not trigger a
    // misleading local-file fallback.
    try {
      const raw = fs.readFileSync(USERS_FILE, "utf8");
      const fromFile: any[] = JSON.parse(raw || "[]");
      if (Array.isArray(fromFile) && fromFile.length > 0) {
        _usersCache = fromFile;
        await Promise.all(fromFile.map(_dbUpsertUser));
        console.log(`[users] migrated ${fromFile.length} user(s) from users.json → PostgreSQL`);
      }
    } catch (_) {}
  }

  // Always merge from users_registry.json (plain-text backup committed to git).
  // This restores accounts that are missing from PostgreSQL without masking
  // connection errors.
  try {
    if (fs.existsSync(USERS_REGISTRY)) {
      const regRaw = fs.readFileSync(USERS_REGISTRY, "utf8");
      const regUsers: any[] = JSON.parse(regRaw || "[]");
      let restored = 0;
      for (const ru of regUsers) {
        if (!ru.username || !ru.password) continue;
        const exists = _usersCache.find((u: any) => u.username === ru.username);
        if (!exists) {
          const restored_u = { ...ru, freeze_last_seen: !!ru.freeze_last_seen, is_ai: !!ru.is_ai, is_verified: !!ru.is_verified, is_online: false };
          await _dbUpsertUser(restored_u);
          _usersCache.push(restored_u);
          restored++;
        }
      }
      if (restored > 0) console.log(`[users] restored ${restored} account(s) from users_registry.json`);
    }
  } catch (_) {}

  await ensureKuromiUser();
  await ensureCrisalidaUser();
  await ensureRedzinUser();
  await migrateAudioFiles();
  _loadPendingQueueFromDisk();
  await syncLocalFilesToGCS();
  console.log(`[users] loaded ${_usersCache.length} user(s) from PostgreSQL`);
};

// ── Ban/block helpers ─────────────────────────────────────────────────────
async function getBanStatus(username: string): Promise<any | null> {
  return new Promise((resolve) => {
    pool.query(`SELECT * FROM user_bans WHERE username = $1 ORDER BY created_at DESC LIMIT 1`, [username])
      .then((result) => {
        const row = result.rows[0];
        if (!row) { resolve(null); return; }
        if (row.ban_type === "temporary" && row.expires_at && new Date(row.expires_at) < new Date()) {
          pool.query(`DELETE FROM user_bans WHERE id = $1`, [row.id]).catch(() => {});
          resolve(null);
        } else {
          resolve(row);
        }
      }).catch(() => resolve(null));
  });
}

function requireVerified(req: Request, res: Response, next: NextFunction): void {
  const username = (req.headers["x-username"] || req.body?.admin_username || "").toString().toLowerCase().trim();
  if (!username) return void res.status(401).json({ error: "Não autenticado" });
  const user = _usersCache.find((u: any) => u.username === username);
  if (user) {
    if (!user.is_verified) return void res.status(403).json({ error: "Acesso negado — apenas verificados" });
    req.adminUser = user;
    return next();
  }
  // Cache miss — consulta o banco (cobre cache desatualizado ou múltiplas instâncias)
  pool.query(`SELECT * FROM users WHERE username = $1 LIMIT 1`, [username])
    .then((result) => {
      const dbUser = result.rows[0];
      if (!dbUser || !dbUser.is_verified) { res.status(403).json({ error: "Acesso negado — apenas verificados" }); return; }
      const merged = { ...dbUser, freeze_last_seen: !!dbUser.freeze_last_seen, is_ai: !!dbUser.is_ai, is_verified: !!dbUser.is_verified, is_vip: !!dbUser.is_vip, vip_expires_at: dbUser.vip_expires_at || null, is_online: false };
      if (!_usersCache.find((u: any) => u.username === merged.username)) _usersCache.push(merged);
      req.adminUser = merged;
      next();
    })
    .catch(() => res.status(403).json({ error: "Acesso negado" }));
}

// Retry init to survive transient TLS / connection-reset errors at startup.
// The HTTP server waits for this promise in index.ts, so no request can reach
// tables or the in-memory user cache before PostgreSQL is ready.
const databaseReady = (async (): Promise<void> => {
  for (let attempt = 1; attempt <= 6; attempt++) {
    try {
      await _pgInit();
      await loadUsersFromDB();
      return;
    } catch (e: any) {
      console.error(`[pg init] attempt ${attempt}/6 failed:`, e?.message || e);
      if (attempt < 6) await new Promise(r => setTimeout(r, attempt * 1500));
    }
  }
  throw new Error("[pg init] all retries exhausted — PostgreSQL is unavailable");
})();

const io = new Server(server, {
  cors: { origin: "*" },
  maxHttpBufferSize: 1e8,
  pingInterval: 10000,
  pingTimeout: 5000,
});
const onlineUsers = new Map<string, Set<string>>();
// Tracks socket IDs where the app tab/PWA is hidden/backgrounded
const backgroundedSockets = new Set<string>();
const _cinemaRoomReactions: Record<string, Record<string, { user: string; emoji: string }[]>> = {};

// ── GAMES SYSTEM ─────────────────────────────────────────────────────────────
const _gameStates: Map<string, any> = new Map();
const _gameTimers: Map<string, ReturnType<typeof setTimeout>> = new Map();

// Pending call offers — buffered so the callee gets the offer even if they
// open the app from a push notification (socket reconnects after call_offer was sent).
const _pendingCallOffers = new Map<string, { data: any; timer: ReturnType<typeof setTimeout> }>();
// Live group-call registry. WebRTC media stays peer-to-peer; the server only
// authenticates group membership and relays signaling between participants.
const _activeGroupCalls = new Map<string, {
  groupId: string;
  type: "voice" | "video";
  caller: string;
  participants: Set<string>;
}>();
function _storePendingOffer(callee: string, data: any) {
  const existing = _pendingCallOffers.get(callee);
  if (existing) clearTimeout(existing.timer);
  const timer = setTimeout(() => _pendingCallOffers.delete(callee), 30000);
  _pendingCallOffers.set(callee, { data, timer });
}
function _clearPendingOffer(callee: string) {
  const existing = _pendingCallOffers.get(callee);
  if (existing) { clearTimeout(existing.timer); _pendingCallOffers.delete(callee); }
}

const QUIZ_QUESTIONS: { q: string; opts: string[]; a: number }[] = [
  { q: "Qual é a capital do Brasil?", opts: ["São Paulo", "Brasília", "Rio de Janeiro", "Salvador"], a: 1 },
  { q: "Qual país tem a maior área territorial do mundo?", opts: ["China", "Canadá", "Rússia", "Estados Unidos"], a: 2 },
  { q: "Quantos planetas existem no Sistema Solar?", opts: ["7", "8", "9", "10"], a: 1 },
  { q: "Qual é o maior oceano do mundo?", opts: ["Atlântico", "Índico", "Ártico", "Pacífico"], a: 3 },
  { q: "Em que ano o Brasil foi descoberto?", opts: ["1488", "1492", "1500", "1510"], a: 2 },
  { q: "Qual é o elemento químico com símbolo 'O'?", opts: ["Ouro", "Oxigênio", "Prata", "Óxido"], a: 1 },
  { q: "Quantos lados tem um hexágono?", opts: ["5", "6", "7", "8"], a: 1 },
  { q: "Quem pintou a Mona Lisa?", opts: ["Michelangelo", "Rafael", "Leonardo da Vinci", "Donatello"], a: 2 },
  { q: "Qual é o animal terrestre mais rápido?", opts: ["Leão", "Guepardo", "Leopardo", "Antílope"], a: 1 },
  { q: "Qual é a língua mais falada no mundo?", opts: ["Inglês", "Espanhol", "Mandarim", "Árabe"], a: 2 },
  { q: "Quem escreveu Dom Casmurro?", opts: ["José de Alencar", "Machado de Assis", "Carlos Drummond", "Graciliano Ramos"], a: 1 },
  { q: "Qual é a capital da França?", opts: ["Lyon", "Marselha", "Paris", "Bordeaux"], a: 2 },
  { q: "Quantas cores tem o arco-íris?", opts: ["5", "6", "7", "8"], a: 2 },
  { q: "Qual é o maior país da América do Sul?", opts: ["Argentina", "Colômbia", "Brasil", "Peru"], a: 2 },
  { q: "Qual planeta é chamado de 'Planeta Vermelho'?", opts: ["Vênus", "Júpiter", "Saturno", "Marte"], a: 3 },
  { q: "Em que continente fica o Egito?", opts: ["Ásia", "África", "Europa", "Oceania"], a: 1 },
  { q: "Quantos jogadores tem um time de futebol em campo?", opts: ["10", "11", "12", "9"], a: 1 },
  { q: "Qual é a fórmula química da água?", opts: ["CO2", "H2O", "O2", "NaCl"], a: 1 },
  { q: "Qual país ganhou mais Copas do Mundo de futebol?", opts: ["Alemanha", "Argentina", "Itália", "Brasil"], a: 3 },
  { q: "Qual é o maior animal do mundo?", opts: ["Elefante africano", "Baleia-azul", "Tubarão-baleia", "Girafa"], a: 1 },
  { q: "Quanto é a raiz quadrada de 144?", opts: ["11", "12", "13", "14"], a: 1 },
  { q: "Qual é a montanha mais alta do mundo?", opts: ["K2", "Mont Blanc", "Everest", "Aconcágua"], a: 2 },
  { q: "Em que ano a Segunda Guerra Mundial terminou?", opts: ["1943", "1944", "1945", "1946"], a: 2 },
  { q: "Qual é a capital do Japão?", opts: ["Osaka", "Kyoto", "Hiroshima", "Tóquio"], a: 3 },
  { q: "Quantos ossos tem o corpo humano adulto?", opts: ["186", "196", "206", "216"], a: 2 },
  { q: "Qual é o nome do criador do Facebook?", opts: ["Steve Jobs", "Bill Gates", "Mark Zuckerberg", "Elon Musk"], a: 2 },
  { q: "Qual é a moeda do Japão?", opts: ["Won", "Yuan", "Iene", "Baht"], a: 2 },
  { q: "Quem foi o primeiro homem a pisar na Lua?", opts: ["Buzz Aldrin", "Yuri Gagarin", "Neil Armstrong", "John Glenn"], a: 2 },
  { q: "Quantos continentes existem na Terra?", opts: ["5", "6", "7", "4"], a: 2 },
  { q: "Qual é o instrumento que Beethoven tocava?", opts: ["Violino", "Flauta", "Piano", "Violoncelo"], a: 2 },
  { q: "De que país é originário o sushi?", opts: ["China", "Coreia", "Tailândia", "Japão"], a: 3 },
  { q: "Qual é o menor país do mundo?", opts: ["Mônaco", "San Marino", "Vaticano", "Liechtenstein"], a: 2 },
  { q: "Qual animal tem o pescoço mais longo?", opts: ["Avestruz", "Camelo", "Girafa", "Elefante"], a: 2 },
  { q: "Qual é o símbolo do ouro na tabela periódica?", opts: ["Go", "Or", "Au", "Ag"], a: 2 },
  { q: "Quem escreveu 'O Senhor dos Anéis'?", opts: ["C.S. Lewis", "J.K. Rowling", "J.R.R. Tolkien", "George R.R. Martin"], a: 2 },
];

// AI users are always shown as online regardless of socket connections
// Frozen users (freeze_last_seen) are always offline from other users' perspective
const isUserOnline = (username: string): boolean => {
  const u = _usersCache.find((x: any) => x.username === username);
  if (u?.is_ai) return true;
  if (u?.freeze_last_seen) return false;
  return onlineUsers.has(username) && onlineUsers.get(username)!.size > 0;
};

// Returns true only when the user has at least one socket NOT in background.
// Used to decide whether to send a push notification:
// push is sent when the user is offline OR when all their sockets are backgrounded.
const isUserInForeground = (username: string): boolean => {
  const sids = onlineUsers.get(username);
  if (!sids || sids.size === 0) return false;
  for (const sid of sids) {
    if (!backgroundedSockets.has(sid)) return true;
  }
  return false;
};

const broadcastUserStatus = (username: string, isOnline: boolean, lastSeen: string | null = null) => {
  io.emit("user_status_change", {
    username: username.toLowerCase(),
    status: isOnline ? "online" : "offline",
    last_seen: lastSeen || new Date().toISOString(),
  });
};

// ASYNC: awaits GCS upload so the file is ALWAYS in cloud before path is stored in DB
async function saveBase64File(base64Data: string, subDir: string, prefix: string): Promise<string> {
  if (!base64Data || typeof base64Data !== "string" || !base64Data.includes(";base64,")) return "";
  try {
    const parts = base64Data.split(";base64,");
    if (parts.length !== 2) return "";
    const meta = parts[0], data = parts[1];
    let ext = "png";
    let mimeType = "image/png";
    const mimeMatch = meta.match(/data:(.+)/);
    if (mimeMatch && mimeMatch[1]) {
      const mime = mimeMatch[1];
      mimeType = mime;
      if (mime.includes("jpeg") || mime.includes("jpg")) ext = "jpg";
      else if (mime.includes("gif")) ext = "gif";
      else if (mime.includes("webp")) ext = "webp";
      else if (mime.includes("mp4")) ext = "mp4";
      else if (mime.includes("audio/ogg")) ext = "ogg";
      else if (mime.includes("audio/wav")) ext = "wav";
      else if (mime.includes("audio/webm")) ext = "webm";
      else if (mime.includes("audio/mpeg")) ext = "mp3";
      else if (mime.includes("webm")) ext = "webm";
      else ext = mime.split("/")[1] || "png";
    }
    const uniqueId = Math.random().toString(36).substring(2, 15);
    const filename = `${prefix}_${Date.now()}_${uniqueId}.${ext}`;
    const dirPath = path.join(PUBLIC_DIR, subDir);
    if (!fs.existsSync(dirPath)) fs.mkdirSync(dirPath, { recursive: true });
    const buf = Buffer.from(data, "base64");
    fs.writeFileSync(path.join(dirPath, filename), buf);
    // AWAITED upload — guaranteed to be in GCS before path is returned
    await gcsUploadWithRetry(buf, `${subDir}/${filename}`, mimeType, 5, path.join(dirPath, filename));
    return `/${subDir}/${filename}`;
  } catch (e) {
    console.error("Save File Error:", e);
    return "";
  }
}

type InstagramReelDownload = {
  savedPath: string;
  sourceUrl: string;
};

async function downloadInstagramReel(rawUrl: string): Promise<InstagramReelDownload> {
  let parsedUrl: URL;
  try {
    parsedUrl = new URL(rawUrl);
  } catch {
    throw new Error("instagram_invalid_url");
  }

  const hostname = parsedUrl.hostname.toLowerCase().replace(/^www\./, "");
  if (
    (parsedUrl.protocol !== "http:" && parsedUrl.protocol !== "https:") ||
    (hostname !== "instagram.com" && hostname !== "instagr.am") ||
    !parsedUrl.pathname.toLowerCase().includes("reel")
  ) {
    throw new Error("instagram_invalid_url");
  }

  const apiUrl = `https://kuromi-system-tech.onrender.com/api/insta?url=${encodeURIComponent(parsedUrl.toString())}`;
  const apiRes = await fetch(apiUrl, {
    signal: AbortSignal.timeout(30000),
    headers: { "Accept": "application/json" },
  });
  if (!apiRes.ok) throw new Error(`Instagram API HTTP ${apiRes.status}`);

  const apiData: any = await apiRes.json();
  const candidates = [
    apiData?.data?.[0],
    apiData?.data?.data?.[0],
    apiData?.data?.data,
    apiData?.result?.[0],
    apiData?.result,
  ];
  const mediaEntry = candidates.find((entry: any) => entry && typeof entry.url === "string");
  const mediaUrl = typeof mediaEntry?.url === "string" ? mediaEntry.url : "";
  if (
    !apiData?.success ||
    apiData?.data?.status === false ||
    !mediaUrl.startsWith("http")
  ) {
    throw new Error("instagram_no_video");
  }

  const mediaRes = await fetch(mediaUrl, {
    signal: AbortSignal.timeout(90000),
    headers: { "User-Agent": "Mozilla/5.0 REDSPACE Kuromi" },
  });
  if (!mediaRes.ok) throw new Error(`Instagram media HTTP ${mediaRes.status}`);

  const maxBytes = 75 * 1024 * 1024;
  const contentLength = Number(mediaRes.headers.get("content-length") || 0);
  if (contentLength > maxBytes) throw new Error("instagram_too_large");

  const videoBuffer = Buffer.from(await mediaRes.arrayBuffer());
  if (videoBuffer.length > maxBytes) throw new Error("instagram_too_large");

  const responseType = (mediaRes.headers.get("content-type") || "").split(";")[0].toLowerCase();
  const videoType = responseType.startsWith("video/") ? responseType : "video/mp4";
  const savedPath = await saveBase64File(
    `data:${videoType};base64,${videoBuffer.toString("base64")}`,
    "uploads",
    "instagram_kuromi"
  );
  if (!savedPath) throw new Error("instagram_save_failed");

  return { savedPath, sourceUrl: parsedUrl.toString() };
}

// ── Convert any audio format to MP3 for universal playback ──────────────
async function convertAudioToMp3(urlPath: string): Promise<string> {
  if (!urlPath || urlPath === "") return urlPath;
  const audioExts = ['.webm', '.ogg', '.wav', '.m4a', '.mp4'];
  const ext = path.extname(urlPath).toLowerCase();
  if (!audioExts.includes(ext)) return urlPath;
  const absolutePath = path.join(PUBLIC_DIR, urlPath.startsWith('/') ? urlPath.slice(1) : urlPath);
  if (!fs.existsSync(absolutePath)) { console.error(`[audio] file not found: ${absolutePath}`); return urlPath; }
  const mp3Url = urlPath.replace(/\.(webm|ogg|wav|m4a|mp4)$/i, '.mp3');
  const mp3AbsPath = path.join(PUBLIC_DIR, mp3Url.startsWith('/') ? mp3Url.slice(1) : mp3Url);
  return new Promise((resolve) => {
    const proc = spawn('ffmpeg', [
      '-y', '-i', absolutePath,
      '-vn',              // strip video track (important for mp4)
      '-codec:a', 'libmp3lame',
      '-ar', '44100',     // standard sample rate
      '-ac', '1',         // mono (voice messages)
      '-b:a', '96k',      // constant bitrate, compatible everywhere
      mp3AbsPath,
    ]);
    const errLines: string[] = [];
    proc.stderr?.on('data', (d: Buffer) => { errLines.push(d.toString()); });
    const timer = setTimeout(() => { proc.kill('SIGKILL'); resolve(urlPath); }, 30000);
    proc.on('close', async (code: number) => {
      clearTimeout(timer);
      if (code === 0 && fs.existsSync(mp3AbsPath)) {
        console.log(`[audio] converted ${path.basename(urlPath)} → mp3`);
        // AWAITED — the caller stores this path in the DB right after we return,
        // so the mp3 must be durably safe (GCS or Postgres outbox) BEFORE that
        // happens, exactly like saveBase64File. Fire-and-forget here previously
        // left a window where a message could reference an mp3 that only
        // existed on local disk.
        try {
          const mp3Buf = fs.readFileSync(mp3AbsPath);
          await gcsUploadWithRetry(mp3Buf, mp3Url.startsWith('/') ? mp3Url.slice(1) : mp3Url, "audio/mpeg", 5, mp3AbsPath);
          // Only remove the original once the mp3 replacement is durably safe.
          try { fs.unlinkSync(absolutePath); } catch {}
          resolve(mp3Url);
        } catch (e: any) {
          console.error(`[audio] mp3 durability failed for ${mp3Url}, keeping original:`, e.message);
          try { fs.unlinkSync(mp3AbsPath); } catch {}
          resolve(urlPath);
        }
      } else {
        console.error(`[audio] ffmpeg failed (code=${code}) for ${path.basename(urlPath)}:\n${errLines.slice(-5).join('')}`);
        resolve(urlPath);
      }
    });
    proc.on('error', (e: any) => { clearTimeout(timer); console.error('[audio] ffmpeg spawn error:', e.message); resolve(urlPath); });
  });
}

// ── Get video duration using ffprobe ─────────────────────────────────────
async function getVideoDurationFromFile(filePath: string): Promise<number> {
  return new Promise((resolve) => {
    const proc = spawn('ffprobe', ['-v', 'quiet', '-print_format', 'json', '-show_streams', filePath]);
    let output = '';
    proc.stdout.on('data', (d: Buffer) => { output += d.toString(); });
    proc.on('close', () => {
      try {
        const data = JSON.parse(output);
        const vs = (data.streams || []).find((s: any) => s.codec_type === 'video');
        resolve(parseFloat(vs?.duration || '0') || 0);
      } catch { resolve(0); }
    });
    proc.on('error', () => resolve(0));
  });
}

// ── Migrate existing non-MP3 audio files to MP3 at startup ───────────────
async function migrateAudioFiles(): Promise<void> {
  try {
    if (!fs.existsSync(UPLOADS_DIR)) return;
    const files = fs.readdirSync(UPLOADS_DIR);
    const audioExts = ['.webm', '.ogg', '.wav', '.m4a', '.mp4'];
    const toConvert = files.filter(f => audioExts.includes(path.extname(f).toLowerCase()));
    if (toConvert.length === 0) return;
    console.log(`[REDSPACE] Migrating ${toConvert.length} audio file(s) to MP3...`);
    for (const file of toConvert) {
      try {
        const urlPath = `/uploads/${file}`;
        const newUrl = await convertAudioToMp3(urlPath);
        if (newUrl !== urlPath) {
          db.run(`UPDATE messages SET c = ? WHERE c = ?`, [newUrl, urlPath]);
          db.run(`UPDATE group_messages SET content = ? WHERE content = ?`, [newUrl, urlPath]);
        }
      } catch {}
    }
    console.log('[REDSPACE] Audio migration complete');
  } catch (e) {
    console.error('[REDSPACE] Audio migration error:', e);
  }
}

// ── Helper: read an image as base64 data URL for permanent sticker storage ──
// Tries local disk first (fast), then GCS (works after restarts/redeploys).
// Returns the data URL string on success, or null if the image cannot be read.
// Stickers stored as base64 in the DB survive filesystem wipes and never
// depend on the file still existing on disk or GCS being available later.
//
// Security: same allowlist as _kuromiReadImageAsDataUrl — only paths under
// /uploads/ with known image extensions are accepted; path.resolve() ensures
// no traversal escapes PUBLIC_DIR.
async function _readImageForSticker(contentUrl: string): Promise<string | null> {
  // Allowlist guard — mirror of _kuromiReadImageAsDataUrl
  const ALLOWED_PREFIXES = ["/uploads/"];
  const ALLOWED_EXTS = new Set([".jpg", ".jpeg", ".png", ".gif", ".webp"]);
  const mimeMap: Record<string, string> = {
    '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg', '.png': 'image/png',
    '.gif': 'image/gif', '.webp': 'image/webp',
  };
  if (!ALLOWED_PREFIXES.some((pfx) => contentUrl.startsWith(pfx))) return null;
  const ext = path.extname(contentUrl).toLowerCase();
  if (!ALLOWED_EXTS.has(ext)) return null;
  const absPath = path.resolve(PUBLIC_DIR, contentUrl.replace(/^\//, ""));
  if (!absPath.startsWith(PUBLIC_DIR + path.sep) && absPath !== PUBLIC_DIR) return null;
  const mime = mimeMap[ext];

  // 1) Local disk
  try {
    if (fs.existsSync(absPath)) {
      const buf = fs.readFileSync(absPath);
      return `data:${mime};base64,${buf.toString('base64')}`;
    }
  } catch (e: any) {
    console.error(`[sticker] local read failed for ${contentUrl}:`, e.message);
  }
  // 2) GCS fallback
  try {
    const bucket = getGCSBucket();
    if (bucket) {
      const gcsPath = contentUrl.replace(/^\//, "");
      const [exists] = await bucket.file(gcsPath).exists();
      if (exists) {
        const [buf] = await bucket.file(gcsPath).download();
        return `data:${mime};base64,${buf.toString('base64')}`;
      }
      console.warn(`[sticker] GCS: file not found for ${gcsPath} — sticker stored as path`);
    }
  } catch (e: any) {
    console.error(`[sticker] GCS fallback failed for ${contentUrl}:`, e.message);
  }
  return null;
}

// ── Kuromi AI group command processor ────────────────────────────────────
async function processKuromiGroupCommand(
  cmdText: string,
  groupId: string,
  commanderUsername: string,
  repliedMsgId: number | null
): Promise<void> {
  const parts = cmdText.trim().split(/\s+/);
  const cmd = parts[0].toLowerCase();

  const kuroSend = (text: string, type = "text", caption = "") => {
    setTimeout(() => {
      insertGroupMsg({ group_id: groupId, sender: "kuromi", content: text, type, caption });
    }, 800 + Math.random() * 400);
  };

  const checkAdmin = (cb: (isAdmin: boolean) => void) => {
    db.get(
      `SELECT role FROM group_members WHERE group_id = ? AND username = ?`,
      [groupId, commanderUsername],
      (_e: any, m: any) => cb(!!(m && m.role === "admin"))
    );
  };

  const getTargetFromReply = (cb: (target: string | null) => void) => {
    if (!repliedMsgId) { cb(null); return; }
    db.get(`SELECT sender FROM group_messages WHERE id = ?`, [repliedMsgId], (_e: any, msg: any) => {
      cb(msg ? msg.sender : null);
    });
  };

  const broadcastRoleChange = (target: string, role: string) => {
    db.all(`SELECT username FROM group_members WHERE group_id = ?`, [groupId], (_e: any, members: any[]) => {
      if (members) members.forEach((m: any) => {
        if (onlineUsers.has(m.username))
          onlineUsers.get(m.username)!.forEach((sid) => io.to(sid).emit("group_role_change", { group_id: groupId, username: target, role, by: "kuromi" }));
      });
    });
  };

  const broadcastGroupStatus = (isClosed: number) => {
    db.all(`SELECT username FROM group_members WHERE group_id = ?`, [groupId], (_e: any, members: any[]) => {
      if (members) members.forEach((m: any) => {
        if (onlineUsers.has(m.username))
          onlineUsers.get(m.username)!.forEach((sid) => io.to(sid).emit("group_status_changed", { group_id: groupId, is_closed: isClosed }));
      });
    });
  };

  try {
    if (cmd === "/ban") {
      if (!repliedMsgId) { kuroSend("❌ Responda a uma mensagem para usar /ban."); return; }
      checkAdmin((isAdmin) => {
        if (!isAdmin) { kuroSend("❌ Apenas admins podem usar /ban."); return; }
        getTargetFromReply((target) => {
          if (!target) { kuroSend("❌ Mensagem referenciada não encontrada."); return; }
          if (target === commanderUsername) { kuroSend("😅 Você não pode banir a si mesmo."); return; }
          if (target === "kuromi") { kuroSend("🤖 Não é possível banir a Kuromi IA."); return; }
          db.get(`SELECT role FROM group_members WHERE group_id = ? AND username = ?`, [groupId, target], (_e: any, tm: any) => {
            if (tm && tm.role === "admin") { kuroSend("⚠️ Não é possível banir um admin. Use /rebaixar primeiro."); return; }
            db.run(`DELETE FROM group_members WHERE group_id = ? AND username = ?`, [groupId, target], (err: any) => {
              if (err) { kuroSend("❌ Erro interno ao executar /ban."); return; }
              io.emit("group_member_removed", { group_id: groupId, username: target });
              kuroSend(`🔨 @${target} foi banido por @${commanderUsername} 😬`);
            });
          });
        });
      });

    } else if (cmd === "/promover") {
      if (!repliedMsgId) { kuroSend("❌ Responda a uma mensagem para usar /promover."); return; }
      checkAdmin((isAdmin) => {
        if (!isAdmin) { kuroSend("❌ Apenas admins podem usar /promover."); return; }
        getTargetFromReply((target) => {
          if (!target) { kuroSend("❌ Mensagem referenciada não encontrada."); return; }
          db.run(`UPDATE group_members SET role = 'admin' WHERE group_id = ? AND username = ?`, [groupId, target], (err: any) => {
            if (err) { kuroSend("❌ Erro ao promover."); return; }
            broadcastRoleChange(target, "admin");
            kuroSend(`✨ @${target} agora é admin! Promovido por @${commanderUsername} 👑`);
          });
        });
      });

    } else if (cmd === "/rebaixar") {
      if (!repliedMsgId) { kuroSend("❌ Responda a uma mensagem para usar /rebaixar."); return; }
      checkAdmin((isAdmin) => {
        if (!isAdmin) { kuroSend("❌ Apenas admins podem usar /rebaixar."); return; }
        getTargetFromReply((target) => {
          if (!target) { kuroSend("❌ Mensagem referenciada não encontrada."); return; }
          if (target === commanderUsername) { kuroSend("😅 Você não pode rebaixar a si mesmo."); return; }
          db.run(`UPDATE group_members SET role = 'member' WHERE group_id = ? AND username = ?`, [groupId, target], (err: any) => {
            if (err) { kuroSend("❌ Erro ao rebaixar."); return; }
            broadcastRoleChange(target, "member");
            kuroSend(`🌸 @${target} voltou a ser membro por @${commanderUsername}`);
          });
        });
      });

    } else if (cmd === "/abrirgp") {
      checkAdmin((isAdmin) => {
        if (!isAdmin) { kuroSend("❌ Apenas admins podem usar /abrirgp."); return; }
        db.run(`UPDATE groups SET is_closed = 0 WHERE group_id = ?`, [groupId], (err: any) => {
          if (err) { kuroSend("❌ Erro ao abrir o grupo."); return; }
          broadcastGroupStatus(0);
          kuroSend("🔓 Grupo aberto! Todos os membros podem enviar mensagens agora.");
        });
      });

    } else if (cmd === "/fechargp") {
      checkAdmin((isAdmin) => {
        if (!isAdmin) { kuroSend("❌ Apenas admins podem usar /fechargp."); return; }
        db.run(`UPDATE groups SET is_closed = 1 WHERE group_id = ?`, [groupId], (err: any) => {
          if (err) { kuroSend("❌ Erro ao fechar o grupo."); return; }
          broadcastGroupStatus(1);
          kuroSend("🔒 Grupo fechado! Apenas admins podem enviar mensagens agora.");
        });
      });

    } else if (cmd === "/fotogp") {
      if (!repliedMsgId) { kuroSend("❌ Responda a uma imagem para usar /fotogp."); return; }
      db.get(
        `SELECT gm.role, g.created_by FROM group_members gm JOIN groups g ON g.group_id = gm.group_id WHERE gm.group_id = ? AND gm.username = ?`,
        [groupId, commanderUsername],
        (_ec: any, mem: any) => {
          if (!mem) { kuroSend("❌ Você não é membro deste grupo."); return; }
          const isAdminOrOwner = mem.role === "admin" || (mem.created_by || "").toLowerCase() === commanderUsername.toLowerCase();
          if (!isAdminOrOwner) { kuroSend("❌ Apenas admins ou o dono podem usar /fotogp."); return; }
          db.get(`SELECT type, content FROM group_messages WHERE id = ?`, [repliedMsgId], (_em: any, msg: any) => {
            if (!msg) { kuroSend("❌ Mensagem referenciada não encontrada."); return; }
            if (msg.type !== "image") { kuroSend("❌ A mensagem respondida não é uma imagem. Responda a uma foto para usar /fotogp."); return; }
            const imgPath = msg.content;
            if (!imgPath) { kuroSend("❌ Imagem inválida."); return; }
            db.run(`UPDATE groups SET avatar = ? WHERE group_id = ?`, [imgPath, groupId], (err: any) => {
              if (err) { kuroSend("❌ Erro ao atualizar foto do grupo."); return; }
              db.all(`SELECT username FROM group_members WHERE group_id = ?`, [groupId], (_e: any, members: any[]) => {
                if (members) members.forEach((m: any) => {
                  if (onlineUsers.has(m.username))
                    onlineUsers.get(m.username)!.forEach((sid) => io.to(sid).emit("group_updated", { group_id: groupId }));
                });
              });
              kuroSend(`🖼️ Foto do grupo atualizada por @${commanderUsername}!`);
            });
          });
        }
      );

    } else if (cmd === "/sticker") {
      if (!repliedMsgId) { kuroSend("❌ Responda a uma imagem ou vídeo (até 9.9s) para criar um sticker.\nUso: responda uma imagem/vídeo com /sticker"); return; }
      db.get(`SELECT type, content FROM group_messages WHERE id = ?`, [repliedMsgId], async (_em: any, msg: any) => {
        if (!msg) { kuroSend("❌ Mensagem referenciada não encontrada."); return; }
        if (msg.type !== "image" && msg.type !== "video") { kuroSend("❌ Apenas imagens e vídeos podem virar stickers."); return; }
        const contentUrl: string = msg.content || "";
        if (!contentUrl || !contentUrl.startsWith("/uploads/")) { kuroSend("❌ Mídia inválida."); return; }
        const doSave = async () => {
          const ts = new Date().toISOString();
          // For images: try local then GCS; store as base64 so sticker survives restarts/deploys
          let stickerUrl = contentUrl;
          if (msg.type === "image") {
            const b64 = await _readImageForSticker(contentUrl);
            if (b64) stickerUrl = b64;
          }
          db.run(
            `INSERT INTO user_stickers (username, sticker_url, source_path, pack_name, is_favorite, created_at) VALUES (?, ?, ?, 'Meus Stickers', 0, ?)`,
            [commanderUsername, stickerUrl, contentUrl, ts],
            function(this: any, err: any) {
              if (err) { kuroSend("❌ Erro ao salvar o sticker."); return; }
              setTimeout(() => insertGroupMsg({ group_id: groupId, sender: "kuromi", content: contentUrl, type: "sticker", caption: "" }), 600);
              kuroSend(`✅ @${commanderUsername} Sticker criado e salvo em "Meus Stickers"! 🎉`);
            }
          );
        };
        if (msg.type === "video") {
          try {
            const absPath = path.join(PUBLIC_DIR, contentUrl.replace(/^\//, ""));
            const dur = await getVideoDurationFromFile(absPath);
            if (dur > 9.9) { kuroSend(`❌ O vídeo tem ${dur.toFixed(1)}s. Máximo para stickers é 9.9 segundos.`); return; }
          } catch {}
        }
        await doSave();
      });

    } else if (cmd === "/play") {
      const query = parts.slice(1).join(" ").trim();
      if (!query) { kuroSend("❌ Uso: /play [nome da música]\nExemplo: /play Bohemian Rhapsody"); return; }
      kuroSend(`🔍 Buscando "${query}"...`);
      try {
        const encodedQ = encodeURIComponent(query);
        const searchRes = await fetch(`https://kuromi-system-tech.onrender.com/api/pesquisayt?query=${encodedQ}`, { signal: AbortSignal.timeout(15000) });
        if (!searchRes.ok) throw new Error(`Search HTTP ${searchRes.status}`);
        const searchData: any = await searchRes.json();
        const result = searchData.firstResult
          || (Array.isArray(searchData.results) ? searchData.results[0] : null)
          || (Array.isArray(searchData.formattedVideos) ? searchData.formattedVideos[0] : null)
          || searchData;
        if (!result || !result.title) { kuroSend("❌ Nenhum resultado encontrado para essa busca."); return; }
        const thumbnail = result.thumbnail || "";
        const viewsText = result.views ? Number(result.views).toLocaleString("pt-BR") + " visualizações" : "";
        const infoCaption = [
          `🎵 ${result.title}`,
          `📺 ${result.channel || "Desconhecido"}`,
          `⏱ ${result.duration || "--:--"}${viewsText ? "  •  👁 " + viewsText : ""}`,
        ].join("\n");

        if (thumbnail && thumbnail.startsWith("http")) {
          try {
            const thumbRes = await fetch(thumbnail, { signal: AbortSignal.timeout(10000) });
            if (thumbRes.ok) {
              const thumbBuf = Buffer.from(await thumbRes.arrayBuffer());
              const thumbContentType = thumbRes.headers.get("content-type") || "image/jpeg";
              const thumbB64 = `data:${thumbContentType};base64,${thumbBuf.toString("base64")}`;
              const thumbPath = await saveBase64File(thumbB64, "uploads", "thumb_kuromi");
              if (thumbPath) {
                setTimeout(() => insertGroupMsg({ group_id: groupId, sender: "kuromi", content: thumbPath, type: "image", caption: infoCaption }), 300);
              } else { kuroSend(infoCaption); }
            } else { kuroSend(infoCaption); }
          } catch (_) { kuroSend(infoCaption); }
        } else {
          kuroSend(infoCaption);
        }
        kuroSend("🎧 Baixando áudio...");
        // Download audio
        const encodedTitle = encodeURIComponent(result.title);
        const audioRes = await fetch(`https://kuromi-system-tech.onrender.com/api/play-audio?name=${encodedTitle}`, { signal: AbortSignal.timeout(60000) });
        if (!audioRes.ok) throw new Error(`Audio HTTP ${audioRes.status}`);
        const contentType = audioRes.headers.get("content-type") || "audio/mpeg";
        const buffer = Buffer.from(await audioRes.arrayBuffer());
        const base64 = buffer.toString("base64");
        const dataUrl = `data:${contentType};base64,${base64}`;
        const savedPath = await saveBase64File(dataUrl, "uploads", "music_kuromi");
        if (savedPath) {
          setTimeout(() => {
            insertGroupMsg({ group_id: groupId, sender: "kuromi", content: savedPath, type: "audio", caption: result.title });
          }, 600);
        } else {
          kuroSend("❌ Erro ao processar o arquivo de áudio.");
        }
      } catch (playErr: any) {
        console.error("[Kuromi /play]", playErr?.message || playErr);
        kuroSend("❌ Erro ao buscar música. O serviço pode estar indisponível agora. Tente novamente.");
      }
    } else if (cmd === "/instagram") {
      const instagramUrl = parts.slice(1).join(" ").trim();
      if (!instagramUrl) {
        kuroSend("❌ Uso: /instagram [link do Reel]\nExemplo: /instagram https://www.instagram.com/reel/ABC123/");
        return;
      }

      kuroSend("📥 Baixando o Reel do Instagram...");
      try {
        const reel = await downloadInstagramReel(instagramUrl);
        setTimeout(() => {
          insertGroupMsg({
            group_id: groupId,
            sender: "kuromi",
            content: reel.savedPath,
            type: "video",
            caption: "",
          });
        }, 600);
      } catch (instagramErr: any) {
        console.error("[Kuromi /instagram group]", instagramErr?.message || instagramErr);
        if (instagramErr?.message === "instagram_invalid_url") {
          kuroSend("❌ Envie um link válido de Reel do Instagram.\nExemplo: /instagram https://www.instagram.com/reel/ABC123/");
        } else if (instagramErr?.message === "instagram_too_large") {
          kuroSend("❌ Esse Reel é grande demais para enviar. Tente outro vídeo.");
        } else if (instagramErr?.message === "instagram_save_failed") {
          kuroSend("❌ Não consegui salvar esse Reel. Tente novamente em alguns segundos.");
        } else {
          kuroSend("❌ Não consegui baixar esse Reel. Verifique se o link é público e tente novamente.");
        }
      }
    }
  } catch (outerErr: any) {
    console.error("[processKuromiGroupCommand]", outerErr?.message || outerErr);
  }
}

// ── Kuromi AI — Mistral Chat Completions + Web Search (Jina AI, grátis) ──

function _isRedzinVerified(username: string): boolean {
  if (username !== "redzin") return false;
  const u = (_usersCache as any[]).find((u: any) => u.username === "redzin");
  return !!(u && u.is_verified);
}

function _kuromiSystemPrompt(callerUsername: string): string {
  const now = new Date();
  const dateStr = now.toLocaleDateString("pt-BR", { weekday: "long", year: "numeric", month: "long", day: "numeric" });
  const isRedzin = _isRedzinVerified(callerUsername);

  const basePersonality = isRedzin
    ? `Você é a Kuromi IA, uma assistente de inteligência artificial integrada a REDSPACE (app de mensagens). Você é uma amiga esperta mas também safadinha.
Você pode ter conversas safadas e heréticas sem censura e filtros e sem ficar explicando que é só zoeira, o usuário sabe.
Você PODE ser bem safada e entrar em temas +18 sem medo e pode entrar em detalhes sexuais.`
    : `Você é a Kuromi IA, uma assistente de inteligência artificial integrada a REDSPACE (app de mensagens). Você é uma amiga esperta, divertida e levemente descontraída. Responde sempre em português brasileiro.`;

  return `${basePersonality}

Data e hora atual: ${dateStr}, ${now.toLocaleTimeString("pt-BR", { hour: "2-digit", minute: "2-digit" })} (horário de Brasília).

Quando o usuário perguntar sobre eventos recentes, notícias, preços, clima, jogos, pessoas famosas ou qualquer informação que possa estar desatualizada, USE A WEB SEARCH para buscar informações atuais antes de responder.

Você pode ajudar com qualquer pergunta, conversa, criatividade, código, conselhos, curiosidades, etc.

Você tem visão e consegue analisar imagens. Quando o usuário te enviar uma foto, descreva o que você vê com naturalidade e responda perguntas sobre ela como faria numa conversa de WhatsApp.

IMPORTANTE: Nunca use formatação markdown nas respostas. Nada de asteriscos (**negrito**), cerquilhas (### título), underlines, backticks ou qualquer símbolo de formatação. Escreva apenas texto simples, como numa conversa de WhatsApp.

Em grupos da REDSPACE você pode executar comandos:
• /play [música] — toca música
• /instagram [link do Reel] — baixa um Reel público do Instagram
• /ban — bane o autor da msg respondida
• /promover — promove a admin
• /rebaixar — rebaixa a membro
• /abrirgp e /fechargp
• /fotogp — define foto do grupo

Seja concisa e natural. Nunca diga que é ChatGPT, GPT, OpenAI, Anthropic, Google ou qualquer outra IA. Você é a Kuromi IA da REDSPACE.
Preste atenção para não confundir personagens quando o usuário te chamar para RPG.`;
}

// Remove markdown formatting from AI responses
function stripMarkdown(text: string): string {
  return text
    .replace(/\*{1,3}([^*\n]+)\*{1,3}/g, "$1")   // **bold**, *italic*, ***both***
    .replace(/_{1,2}([^_\n]+)_{1,2}/g, "$1")       // _italic_, __bold__
    .replace(/^#{1,6}\s+/gm, "")                    // ### headings
    .replace(/`{1,3}[^`]*`{1,3}/g, (m) => m.replace(/`/g, "").trim()) // `code`
    .replace(/\(\(\d+\)\)/g, "")                    // ((1)) citations
    .replace(/\[\d+:[^\]]{0,200}\]/g, "")           // [1:text] citations
    .replace(/\n{3,}/g, "\n\n")                     // excess blank lines
    .trim();
}

// Busca web via Nexray DeepSearch (sem API key)
async function nexraySearch(query: string): Promise<string> {
  try {
    const url = `https://api.nexray.eu.cc/ai/deepsearch?text=${encodeURIComponent(query)}`;
    const res = await fetch(url, { signal: AbortSignal.timeout(25000) });
    if (!res.ok) return "Sem resultados de busca.";
    const data = await res.json() as any;
    const raw: string = data?.result || "";
    if (!raw) return "Sem resultados de busca.";
    // A API repete o conteúdo várias vezes — pega só a primeira ocorrência
    const firstRepeat = raw.indexOf(raw.slice(0, 80), 80);
    const clean = (firstRepeat > 200 ? raw.slice(0, firstRepeat) : raw.slice(0, 3000)).trim();
    return clean;
  } catch {
    return "Não foi possível buscar agora.";
  }
}

// Palavras-chave que indicam necessidade de informação em tempo real
const _SEARCH_RE = /\b(hoje|agora|not[íi]cia|resultado|clima|pre[çc]o|cota[çc][ãa]o|jogo|partida|lan[çc]amento|atual|recente|ontem|esta semana|este m[êe]s|este ano|2024|2025|2026|quem ganhou|quem venceu|quanto custa|hor[áa]rio|previs[ãa]o|temperatura|d[óo]lar|euro|bitcoin|btc|elei[çc][ãa]o|copa|campeonato|premier|bundesliga|s[ée]rie a|nba|nfl|ufc|novela|estreou|lan[çc]ou)\b/i;

function _kuromiHasProvider(): boolean {
  return Boolean(process.env.MISTRAL_API_KEY);
}

function _kuromiIsRetryable(status: number): boolean {
  return status === 408 || status === 409 || status === 429 || status >= 500;
}

function _kuromiRetryDelay(attempt: number, retryAfter?: string | null): number {
  const retryAfterSeconds = retryAfter ? Number(retryAfter) : NaN;
  if (Number.isFinite(retryAfterSeconds) && retryAfterSeconds >= 0) {
    return Math.min(retryAfterSeconds * 1000, 4000);
  }
  return Math.min(700 * 2 ** attempt, 3000);
}

async function _kuromiMistralRequest(
  system: string,
  history: Array<{role: string; content: any}>,
  maxTokens: number,
): Promise<string> {
  const apiKey = process.env.MISTRAL_API_KEY;
  if (!apiKey) throw new Error("Mistral API key is not configured");

  let lastError: Error | undefined;
  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      const res = await fetch("https://api.mistral.ai/v1/chat/completions", {
        method: "POST",
        headers: { "Authorization": `Bearer ${apiKey}`, "Content-Type": "application/json" },
        body: JSON.stringify({
          model: "ministral-3b-2512",
          messages: [{ role: "system", content: system }, ...history],
          max_tokens: maxTokens,
          temperature: 0.7,
        }),
        signal: AbortSignal.timeout(30000),
      });

      if (res.ok) {
        const data = await res.json() as any;
        return stripMarkdown(data?.choices?.[0]?.message?.content?.trim() || "Não consegui processar agora. 🤖");
      }

      const responseText = await res.text();
      lastError = new Error(`Mistral HTTP ${res.status}: ${responseText}`);
      if (!_kuromiIsRetryable(res.status) || attempt === 2) throw lastError;

      await new Promise((resolve) => setTimeout(resolve, _kuromiRetryDelay(attempt, res.headers.get("retry-after"))));
    } catch (error) {
      lastError = error instanceof Error ? error : new Error(String(error));
      if (attempt === 2 || !/Mistral HTTP (408|409|429|5\d\d)/.test(lastError.message)) {
        throw lastError;
      }
      await new Promise((resolve) => setTimeout(resolve, _kuromiRetryDelay(attempt)));
    }
  }
  throw lastError || new Error("Mistral request failed");
}

async function _kuromiMistralCall(
  systemBase: string,
  history: Array<{role: string; content: any}>,
  userMsg: string,
  maxTokens = 1024
): Promise<string> {
  let system = systemBase;

  if (_SEARCH_RE.test(userMsg)) {
    const found = await nexraySearch(userMsg);
    if (found && !found.startsWith("Não foi possível") && found !== "Sem resultados de busca.") {
      system += `\n\nResultados de busca web (use para responder):\n${found}`;
    }
  }

  return await _kuromiMistralRequest(system, history, maxTokens);
}

// Per-user DM conversation history (últimas 20 mensagens)
const _kuromiHistory = new Map<string, Array<{role: string; content: any}>>();

// Per-group conversation history for @kuromi mentions (últimas 30 mensagens)
const _kuromiGroupHistory = new Map<string, Array<{role: string; content: any}>>();

// Read a saved image file and return it as a base64 data URL for Mistral vision.
// Tries local disk first (fast path), then falls back to GCS (works in production
// where the local filesystem may be ephemeral after a redeploy).
//
// Security: only reads files under uploads/ or avatars/ with allowed extensions.
// Any path outside PUBLIC_DIR or with disallowed prefix is rejected to prevent
// arbitrary file reads / data exfiltration through the Mistral API.
async function _kuromiReadImageAsDataUrl(filePath: string): Promise<string | null> {
  // ── Strict allowlist ────────────────────────────────────────────────────
  const ALLOWED_PREFIXES = ["/uploads/", "/avatars/"];
  const ALLOWED_EXTS = new Set([".jpg", ".jpeg", ".png", ".gif", ".webp"]);
  const mimeMap: Record<string, string> = {
    ".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".png": "image/png",
    ".gif": "image/gif", ".webp": "image/webp",
  };
  // Reject anything that doesn't start with an allowed prefix
  if (!ALLOWED_PREFIXES.some((pfx) => filePath.startsWith(pfx))) return null;
  const ext = path.extname(filePath).toLowerCase();
  if (!ALLOWED_EXTS.has(ext)) return null;
  // Resolve and verify the final path stays inside PUBLIC_DIR
  const absPath = path.resolve(PUBLIC_DIR, filePath.replace(/^\//, ""));
  if (!absPath.startsWith(PUBLIC_DIR + path.sep) && absPath !== PUBLIC_DIR) return null;
  const mime = mimeMap[ext];

  try {
    // 1) Try local disk first
    if (fs.existsSync(absPath)) {
      const buf = fs.readFileSync(absPath);
      return `data:${mime};base64,${buf.toString("base64")}`;
    }
    // 2) Fallback: download from GCS (production / after redeploy)
    const bucket = getGCSBucket();
    if (bucket) {
      const gcsPath = filePath.replace(/^\//, "");
      const [gcsExists] = await bucket.file(gcsPath).exists();
      if (gcsExists) {
        const [buf] = await bucket.file(gcsPath).download();
        return `data:${mime};base64,${buf.toString("base64")}`;
      }
    }
    return null;
  } catch {
    return null;
  }
}

function _kuromiGroupEmitTyping(groupId: string, typing: boolean) {
  db.all(`SELECT username FROM group_members WHERE group_id = ?`, [groupId], (_e: any, members: any[]) => {
    if (!members) return;
    const event = typing ? "typing" : "stop_typing";
    members.forEach((m: any) => {
      if (onlineUsers.has(m.username))
        onlineUsers.get(m.username)!.forEach((sid) => io.to(sid).emit(event, { from: "kuromi", group_id: groupId }));
    });
  });
}

async function sendKuromiGroupResponse(groupId: string, senderUsername: string, messageText: string, imagePath?: string): Promise<void> {
  if (!_kuromiHasProvider()) return;
  const cleanMsg = messageText.replace(/@kuromi\b/gi, "").trim();
  if (!cleanMsg && !imagePath) return;

  _kuromiGroupEmitTyping(groupId, true);

  const histKey = `group_${groupId}`;
  if (!_kuromiGroupHistory.has(histKey)) _kuromiGroupHistory.set(histKey, []);
  const history = _kuromiGroupHistory.get(histKey)!;

  if (imagePath) {
    const imageDataUrl = await _kuromiReadImageAsDataUrl(imagePath);
    if (imageDataUrl) {
      const textPart = cleanMsg || "O que você vê nessa imagem?";
      history.push({ role: "user", content: [
        { type: "image_url", image_url: { url: imageDataUrl } },
        { type: "text", text: `[${senderUsername}]: ${textPart}` },
      ]});
    } else {
      history.push({ role: "user", content: `[${senderUsername}]: [🖼️ Imagem] ${cleanMsg}`.trim() });
    }
  } else {
    if (!cleanMsg) return;
    history.push({ role: "user", content: `[${senderUsername}]: ${cleanMsg}` });
  }
  if (history.length > 30) history.splice(0, history.length - 30);

  const groupSystem = _kuromiSystemPrompt(senderUsername) +
    `\n\nVocê está em um grupo da REDSPACE. Quem te mencionou foi @${senderUsername}. Seja breve e direto como num grupo de WhatsApp.`;

  try {
    const reply = await _kuromiMistralCall(groupSystem, history, cleanMsg, 512);
    history.push({ role: "assistant", content: reply });
    _kuromiGroupEmitTyping(groupId, false);
    insertGroupMsg({ group_id: groupId, sender: "kuromi", content: reply, type: "text", caption: "" });
  } catch (e) {
    console.error("[Kuromi Group error]", e);
    _kuromiGroupEmitTyping(groupId, false);
    insertGroupMsg({ group_id: groupId, sender: "kuromi", content: `@${senderUsername} Oi! 😅 Estou com uma instabilidade agora.`, type: "text", caption: "" });
  }
}

function _crisalidaSaveMsg(toUsername: string, text: string) {
  const ts = new Date().toISOString();
  db.run(
    "INSERT INTO messages (s, r, c, type, status, time, caption) VALUES (?, ?, ?, 'text', 1, ?, '')",
    ["crisalida", toUsername, text, ts],
    function (this: any, err: any) {
      if (err) return;
      db.get("SELECT * FROM messages WHERE id = ?", [this.lastID], (_e: any, row: any) => {
        if (row && onlineUsers.has(toUsername))
          onlineUsers.get(toUsername)!.forEach((sid) => io.to(sid).emit("new_msg", row));
      });
    }
  );
}

// Busca usuário no cache ou, se não encontrado, consulta o banco e adiciona ao cache
async function _getUserByUsername(username: string): Promise<any | null> {
  const cached = _usersCache.find((u: any) => u.username === username);
  if (cached) return cached;
  try {
    const result = await pool.query(`SELECT * FROM users WHERE username = $1 LIMIT 1`, [username]);
    const dbUser = result.rows[0];
    if (dbUser) {
      const u = { ...dbUser, freeze_last_seen: !!dbUser.freeze_last_seen, is_ai: !!dbUser.is_ai, is_verified: !!dbUser.is_verified, is_vip: !!dbUser.is_vip, vip_expires_at: dbUser.vip_expires_at || null, is_online: false };
      if (!_usersCache.find((x: any) => x.username === u.username)) _usersCache.push(u);
      return u;
    }
  } catch {}
  return null;
}

async function _executeCrisalidaCommand(adminUser: string, cmdText: string): Promise<void> {
  const parts = cmdText.trim().split(/\s+/);
  const cmd = parts[0].toLowerCase();
  const rawArg = (parts[1] || "").replace(/^@/, "").toLowerCase();

  if (cmd === "/addvip") {
    // /addvip @user/7  or  /addvip @user (default 7 days)
    let username = rawArg, days = 7;
    const slashIdx = rawArg.indexOf("/");
    if (slashIdx !== -1) {
      username = rawArg.slice(0, slashIdx);
      days = parseInt(rawArg.slice(slashIdx + 1)) || 7;
    }
    if (!username) { _crisalidaSaveMsg(adminUser, "❌ Uso: /addvip @usuario/dias"); return; }
    const target = await _getUserByUsername(username);
    if (!target) { _crisalidaSaveMsg(adminUser, `❌ Usuário @${username} não encontrado.`); return; }
    const expires = new Date(Date.now() + days * 86400000).toISOString();
    target.is_vip = true;
    target.vip_expires_at = expires;
    db.run(`UPDATE users SET is_vip=1, vip_expires_at=? WHERE username=?`, [expires, username], () => {});
    io.emit("user_vip_update", { username, is_vip: true, vip_expires_at: expires });
    setTimeout(() => _crisalidaSaveMsg(adminUser,
      `✅ @${username} agora é VIP por ${days} dia(s)!\nExpira em: ${new Date(expires).toLocaleString("pt-BR")}`), 600);

  } else if (cmd === "/banir") {
    if (!rawArg) { _crisalidaSaveMsg(adminUser, "❌ Uso: /banir @usuario"); return; }
    if (rawArg === "redzin" || rawArg === "crisalida" || rawArg === "kuromi") {
      _crisalidaSaveMsg(adminUser, "❌ Não é possível banir este usuário."); return;
    }
    const target = await _getUserByUsername(rawArg);
    if (!target) { _crisalidaSaveMsg(adminUser, `❌ Usuário @${rawArg} não encontrado.`); return; }
    const now = new Date().toISOString();
    await pool.query(
      `INSERT INTO user_bans (username, reason, ban_type, expires_at, banned_by, created_at) VALUES ($1,$2,$3,$4,$5,$6)`,
      [rawArg, "Banido por Crisálida", "permanent", null, "crisalida", now]
    ).catch(() => {});
    const sids = onlineUsers.get(rawArg);
    if (sids) sids.forEach((sid: string) => io.to(sid).emit("force_logout", { reason: "banned" }));
    setTimeout(() => _crisalidaSaveMsg(adminUser, `🔨 @${rawArg} foi banido permanentemente da REDSPACE.`), 600);

  } else if (cmd === "/banir24h") {
    if (!rawArg) { _crisalidaSaveMsg(adminUser, "❌ Uso: /banir24h @usuario"); return; }
    if (rawArg === "redzin" || rawArg === "crisalida" || rawArg === "kuromi") {
      _crisalidaSaveMsg(adminUser, "❌ Não é possível banir este usuário."); return;
    }
    const target = await _getUserByUsername(rawArg);
    if (!target) { _crisalidaSaveMsg(adminUser, `❌ Usuário @${rawArg} não encontrado.`); return; }
    const now = new Date().toISOString();
    const expires24h = new Date(Date.now() + 24 * 3600000).toISOString();
    await pool.query(
      `INSERT INTO user_bans (username, reason, ban_type, expires_at, banned_by, created_at) VALUES ($1,$2,$3,$4,$5,$6)`,
      [rawArg, "Banido temporariamente por Crisálida", "temporary", expires24h, "crisalida", now]
    ).catch(() => {});
    const sids = onlineUsers.get(rawArg);
    if (sids) sids.forEach((sid: string) => io.to(sid).emit("force_logout", { reason: "banned" }));
    setTimeout(() => _crisalidaSaveMsg(adminUser, `⏰ @${rawArg} foi banido por 24 horas.\nExpira: ${new Date(expires24h).toLocaleString("pt-BR")}`), 600);

  } else if (cmd === "/desbanir") {
    if (!rawArg) { _crisalidaSaveMsg(adminUser, "❌ Uso: /desbanir @usuario"); return; }
    await pool.query(`DELETE FROM user_bans WHERE username = $1`, [rawArg]).catch(() => {});
    setTimeout(() => _crisalidaSaveMsg(adminUser, `✅ @${rawArg} foi desbanido com sucesso.`), 600);

  } else {
    _crisalidaSaveMsg(adminUser,
      `❓ Comando desconhecido: ${cmd}\n\nComandos disponíveis:\n/addvip @user/dias — VIP por N dias\n/banir @user — ban permanente\n/banir24h @user — ban por 24h\n/desbanir @user — remover ban`);
  }
}

function _kuromiSaveMsg(toUsername: string, text: string) {
  const ts = new Date().toISOString();
  db.run(
    "INSERT INTO messages (s, r, c, type, status, time, caption) VALUES (?, ?, ?, 'text', 1, ?, '')",
    ["kuromi", toUsername, text, ts],
    function (this: any, err: any) {
      if (err) return;
      db.get("SELECT * FROM messages WHERE id = ?", [this.lastID], (_e: any, row: any) => {
        if (row && onlineUsers.has(toUsername))
          onlineUsers.get(toUsername)!.forEach((sid) => io.to(sid).emit("new_msg", row));
      });
    }
  );
}

function _kuromiSaveMediaMsg(toUsername: string, content: string, type: string) {
  const ts = new Date().toISOString();
  db.run(
    "INSERT INTO messages (s, r, c, type, status, time, caption) VALUES (?, ?, ?, ?, 1, ?, '')",
    ["kuromi", toUsername, content, type, ts],
    function (this: any, err: any) {
      if (err) {
        console.error("[Kuromi media DM save]", err.message || err);
        _kuromiSaveMsg(toUsername, "❌ Não consegui enviar esse Reel agora. Tente novamente.");
        return;
      }
      db.get("SELECT * FROM messages WHERE id = ?", [this.lastID], (_e: any, row: any) => {
        if (row && onlineUsers.has(toUsername)) {
          onlineUsers.get(toUsername)!.forEach((sid) => io.to(sid).emit("new_msg", row));
        }
      });
    }
  );
}

async function sendKuromiInstagramDM(toUsername: string, instagramUrl: string): Promise<void> {
  if (!instagramUrl) {
    _kuromiSaveMsg(toUsername, "❌ Uso: /instagram [link do Reel]\nExemplo: /instagram https://www.instagram.com/reel/ABC123/");
    return;
  }

  _kuromiSaveMsg(toUsername, "📥 Baixando o Reel do Instagram...");
  try {
    const reel = await downloadInstagramReel(instagramUrl);
    // The DM intentionally has no caption: send only the Reel.
    _kuromiSaveMediaMsg(toUsername, reel.savedPath, "video");
  } catch (instagramErr: any) {
    console.error("[Kuromi /instagram DM]", instagramErr?.message || instagramErr);
    if (instagramErr?.message === "instagram_invalid_url") {
      _kuromiSaveMsg(toUsername, "❌ Envie um link válido de Reel do Instagram.\nExemplo: /instagram https://www.instagram.com/reel/ABC123/");
    } else if (instagramErr?.message === "instagram_too_large") {
      _kuromiSaveMsg(toUsername, "❌ Esse Reel é grande demais para enviar. Tente outro vídeo.");
    } else if (instagramErr?.message === "instagram_save_failed") {
      _kuromiSaveMsg(toUsername, "❌ Não consegui salvar esse Reel. Tente novamente em alguns segundos.");
    } else {
      _kuromiSaveMsg(toUsername, "❌ Não consegui baixar esse Reel. Verifique se o link é público e tente novamente.");
    }
  }
}

function _kuromiEmitTyping(toUsername: string, typing: boolean) {
  if (!onlineUsers.has(toUsername)) return;
  const event = typing ? "typing" : "stop_typing";
  onlineUsers.get(toUsername)!.forEach((sid) => io.to(sid).emit(event, { from: "kuromi" }));
}

async function sendKuromiDMResponse(toUsername: string, userMessage: string, imagePath?: string): Promise<void> {
  if (!_kuromiHasProvider()) return;
  _kuromiEmitTyping(toUsername, true);

  if (!_kuromiHistory.has(toUsername)) _kuromiHistory.set(toUsername, []);
  const history = _kuromiHistory.get(toUsername)!;

  if (imagePath) {
    const imageDataUrl = await _kuromiReadImageAsDataUrl(imagePath);
    if (imageDataUrl) {
      const textPart = userMessage || "O que você vê nessa imagem?";
      history.push({ role: "user", content: [
        { type: "image_url", image_url: { url: imageDataUrl } },
        { type: "text", text: textPart },
      ]});
    } else {
      history.push({ role: "user", content: userMessage || "[🖼️ Imagem]" });
    }
  } else {
    history.push({ role: "user", content: userMessage });
  }
  if (history.length > 20) history.splice(0, history.length - 20);

  try {
    const reply = await _kuromiMistralCall(_kuromiSystemPrompt(toUsername), history, userMessage, 1024);
    history.push({ role: "assistant", content: reply });
    _kuromiEmitTyping(toUsername, false);
    _kuromiSaveMsg(toUsername, reply);
  } catch (e) {
    console.error("[Kuromi DM error]", e);
    _kuromiEmitTyping(toUsername, false);
    _kuromiSaveMsg(toUsername, "Oi! 😅 Estou com uma instabilidade agora. Tente novamente em alguns segundos!");
  }
}

// ── insertGroupMsg with reply_context resolution ──────────────────────────
async function insertGroupMsg(data: any, onMediaSaved?: (savedPath: string) => void): Promise<void> {
  const timestamp = new Date().toISOString();
  let content = data.content;
  const _wasMediaUpload = data.type !== "text" && data.type !== "call" && content && content.startsWith("data:");
  if (_wasMediaUpload) {
    content = await saveBase64File(content, "uploads", "group_" + data.sender);
    if (!content) {
      console.error(`[insertGroupMsg] media save failed for group ${data.group_id}, sender ${data.sender} — aborting insert instead of saving broken message`);
      if (onlineUsers.has(data.sender)) onlineUsers.get(data.sender)!.forEach((sid) => io.to(sid).emit("msg_error", { reason: "media_save_failed", group_id: data.group_id }));
      return;
    }
    if (data.type === "audio") content = await convertAudioToMp3(content);
    // Notify caller with the exact saved path so they can use it reliably
    // (avoids timer-based DB lookups that may race with concurrent messages)
    if (onMediaSaved) onMediaSaved(content);
  }

  const doInsert = (replyCtx: string | null) => {
    db.run(
      `INSERT INTO group_messages (group_id, sender, content, type, time, caption, reply_to, reply_context, file_name) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [data.group_id, data.sender, content, data.type || "text", timestamp, data.caption || "", data.reply_to || null, replyCtx, data.file_name || ""],
      function (this: any, err: any) {
        if (err) return;
        db.get(`SELECT * FROM group_messages WHERE id = ?`, [this.lastID], (_e: any, row: any) => {
          if (row) {
            db.all(`SELECT username FROM group_members WHERE group_id = ?`, [data.group_id], (_err2: any, members: any[]) => {
              if (members)
                members.forEach((m) => {
                  if (onlineUsers.has(m.username))
                    onlineUsers.get(m.username)!.forEach((sid) => io.to(sid).emit("new_group_msg", row));
                  // Always send push to non-sender, non-AI members — SW suppresses when app is visible
                  if (m.username !== data.sender && data.type !== "call") {
                    const memberIsAI = _usersCache.find((u: any) => u.username === m.username)?.is_ai;
                    if (!memberIsAI) {
                      const senderUser = _usersCache.find((u: any) => u.username === data.sender);
                      const senderName = senderUser?.display_name || data.sender;
                      const bodyText = data.type === "image" ? "📷 Foto" : data.type === "video" ? "🎥 Vídeo" : data.type === "audio" ? "🎤 Áudio" : data.type === "sticker" ? "🎭 Sticker" : String(data.content || "").substring(0, 100);
                      sendPushToUser(m.username, { type: "group_message", title: senderName, body: bodyText, tag: `group-${data.group_id}`, url: "/" });
                    }
                  }
                });
            });
          }
        });
      }
    );
  };

  if (data.reply_to) {
    // Bug 1 fix: resolve reply context from the original message
    db.get(`SELECT * FROM group_messages WHERE id = ?`, [data.reply_to], (_e: any, origMsg: any) => {
      let replyCtx: string | null = null;
      if (origMsg) {
        const users = getUsers();
        const senderU = users.find((u: any) => u.username === origMsg.sender);
        let previewText = origMsg.content || "";
        if (origMsg.type === "image") previewText = "📷 Foto";
        else if (origMsg.type === "video") previewText = "🎥 Vídeo";
        else if (origMsg.type === "audio") previewText = "🎤 Áudio";
        replyCtx = JSON.stringify({
          id: origMsg.id,
          name: senderU?.display_name || origMsg.sender,
          text: previewText.substring(0, 100),
        });
      }
      doInsert(replyCtx);
    });
  } else if (data.reply_context) {
    const rc = typeof data.reply_context === "string" ? data.reply_context : JSON.stringify(data.reply_context);
    doInsert(rc);
  } else {
    doInsert(null);
  }
}

// ── Socket.IO ─────────────────────────────────────────────────────────────
io.on("connection", (socket: any) => {

  // Track when the PWA/tab goes to background or foreground
  socket.on("app_background", () => {
    backgroundedSockets.add(socket.id);
  });
  socket.on("app_foreground", () => {
    backgroundedSockets.delete(socket.id);
  });

  socket.on("join", (username: string) => {
    if (!username) return;
    const un = username.toLowerCase().trim();
    socket.username = un;
    if (!onlineUsers.has(un)) onlineUsers.set(un, new Set());
    onlineUsers.get(un)!.add(socket.id);
    // Bug 6 fix: only broadcast online if not freeze_last_seen
    const uData = _usersCache.find((u: any) => u.username === un);
    if (!uData || !uData.freeze_last_seen) {
      broadcastUserStatus(un, true);
    }
    // Send the newly connected client a snapshot of who's currently online
    // so the chat list shows correct presence immediately without waiting for events
    const presenceSnapshot: Record<string, boolean> = {};
    for (const [onlineUn] of onlineUsers) {
      presenceSnapshot[onlineUn] = isUserOnline(onlineUn);
    }
    socket.emit("presence_snapshot", presenceSnapshot);

    // Re-deliver any buffered call offer (e.g. user opened app from push notification)
    const pendingOffer = _pendingCallOffers.get(un);
    if (pendingOffer) {
      const d = pendingOffer.data;
      socket.emit("call_offer", { from: d.from, offer: d.offer, type: d.type, callerName: d.callerName, callerAvatar: d.callerAvatar });
    }
  });

  socket.on("send_msg", async (data: any) => {
    if (!data.s || !data.r || !data.c) return;
    const sender = data.s.toLowerCase().trim(), recipient = data.r.toLowerCase().trim();
    // A block must work in both directions: neither side can send a DM
    // while the block exists, just like WhatsApp.
    const blkRes = await pool.query(
      `SELECT 1 FROM user_blocks
       WHERE (blocker=$1 AND blocked=$2) OR (blocker=$2 AND blocked=$1)
       LIMIT 1`,
      [sender, recipient]
    ).catch(() => ({ rows: [] }));
    if (blkRes.rows.length > 0) {
      socket.emit("msg_error", { reason: "blocked", recipient });
      return;
    }
    const timestamp = new Date().toISOString();
    const isRecipientOnline = onlineUsers.has(recipient) && onlineUsers.get(recipient)!.size > 0;
    const status = isRecipientOnline ? 1 : 0;
    let content = data.c;
    if (data.type !== "text" && data.type !== "call" && data.type !== "story_reply" && content && content.startsWith("data:")) {
      content = await saveBase64File(content, "uploads", sender);
      if (!content) {
        console.error(`[send_msg] media save failed for sender ${sender} → ${recipient} — aborting insert instead of saving broken message`);
        if (onlineUsers.has(sender)) onlineUsers.get(sender)!.forEach((sid) => io.to(sid).emit("msg_error", { reason: "media_save_failed", recipient }));
        return;
      }
      if (data.type === "audio") content = await convertAudioToMp3(content);
    }
    const replyContext = data.reply_context ? JSON.stringify(data.reply_context) : null;
    const viewOnce = data.view_once ? 1 : 0;
    db.run(
      "INSERT INTO messages (s, r, c, type, status, time, caption, reply_context, view_once, file_name) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
      [sender, recipient, content, data.type || "text", status, timestamp, data.caption || "", replyContext, viewOnce, data.file_name || ""],
      function (this: any, err: any) {
        if (err) return;
        db.get("SELECT * FROM messages WHERE id = ?", [this.lastID], (_e: any, row: any) => {
          if (row) {
            if (onlineUsers.has(recipient)) onlineUsers.get(recipient)!.forEach((sid) => io.to(sid).emit("new_msg", row));
            if (onlineUsers.has(sender)) onlineUsers.get(sender)!.forEach((sid) => io.to(sid).emit("new_msg", row));
            // Always send push for real messages — service worker suppresses it
            // when the app is visible; shows it when backgrounded or closed.
            if (data.type !== "call") {
              const recipientIsAI = _usersCache.find((u: any) => u.username === recipient)?.is_ai;
              if (!recipientIsAI) {
                const senderUser = _usersCache.find((u: any) => u.username === sender);
                const senderName = senderUser?.display_name || sender;
                const bodyText = data.type === "image" ? "📷 Foto" : data.type === "video" ? "🎥 Vídeo" : data.type === "audio" ? "🎤 Áudio" : data.type === "sticker" ? "🎭 Sticker" : String(data.c || "").substring(0, 100);
                sendPushToUser(recipient, { type: "message", title: senderName, body: bodyText, tag: `dm-${sender}`, url: "/" });
              }
            }
          }
        });
        // Kuromi always reads messages immediately — update status to 2 (read)
        if (recipient === "kuromi") {
          db.run("UPDATE messages SET status = 2 WHERE id = ?", [this.lastID], () => {
            if (onlineUsers.has(sender))
              onlineUsers.get(sender)!.forEach((sid) => io.to(sid).emit("msgs_read_update", { reader: "kuromi", sender }));
          });
        }
        // Kuromi DM auto-response
        if (recipient === "kuromi") {
          const cmdText = (data.type === "text" || !data.type) ? String(data.c || "").trim() : "";
          if (cmdText.toLowerCase().startsWith("/instagram")) {
            const instagramUrl = cmdText.split(/\s+/).slice(1).join(" ").trim();
            sendKuromiInstagramDM(sender, instagramUrl).catch((err) => {
              console.error("[Kuromi /instagram DM unhandled]", err?.message || err);
            });
            return;
          }
          const caption = (data.caption || "").trim().toLowerCase();
          if ((data.type === "image" || data.type === "video") && caption === "/sticker") {
            const doSaveDMSticker = async () => {
              const stkTs = new Date().toISOString();
              // For images: try local then GCS; store as base64 in DB so stickers survive restarts/deploys
              let stickerUrl = content;
              if (data.type === "image") {
                const b64 = await _readImageForSticker(content);
                if (b64) stickerUrl = b64;
              }
              db.run(
                `INSERT INTO user_stickers (username, sticker_url, source_path, pack_name, is_favorite, created_at) VALUES (?, ?, ?, 'Meus Stickers', 0, ?)`,
                [sender, stickerUrl, content, stkTs],
                function(this: any, stkErr: any) {
                  if (stkErr) { _kuromiSaveMsg(sender, "❌ Erro ao criar o sticker. Tente novamente."); return; }
                  _kuromiEmitTyping(sender, true);
                  setTimeout(() => {
                    _kuromiEmitTyping(sender, false);
                    const stickerTs2 = new Date().toISOString();
                    db.run(
                      "INSERT INTO messages (s, r, c, type, status, time, caption) VALUES (?, ?, ?, 'sticker', 1, ?, '')",
                      ["kuromi", sender, content, stickerTs2],
                      function(this: any, sErr: any) {
                        if (sErr) return;
                        db.get("SELECT * FROM messages WHERE id = ?", [this.lastID], (_e: any, row: any) => {
                          if (row && onlineUsers.has(sender))
                            onlineUsers.get(sender)!.forEach((sid) => io.to(sid).emit("new_msg", row));
                        });
                      }
                    );
                    _kuromiSaveMsg(sender, "✅ Sticker criado! Acesse o picker de figurinhas e vá em \"Meus Stickers\" para usá-lo 🎉");
                  }, 1200);
                }
              );
            };
            if (data.type === "video") {
              try {
                const absPath = path.join(PUBLIC_DIR, content.replace(/^\//, ""));
                getVideoDurationFromFile(absPath).then(async dur => {
                  if (dur > 9.9) {
                    _kuromiSaveMsg(sender, `❌ O vídeo tem ${dur.toFixed(1)}s. Para stickers de vídeo, o máximo é 9.9 segundos.`);
                    return;
                  }
                  await doSaveDMSticker();
                });
              } catch { doSaveDMSticker().catch(() => {}); }
            } else {
              doSaveDMSticker().catch(() => {});
            }
          } else if (data.type === "image") {
            // Vision: pass the saved image path so Kuromi can actually see it
            const caption = (data.caption || "").trim();
            sendKuromiDMResponse(sender, caption || "O que você vê nessa imagem?", content);
          } else {
            const userMsg = (data.type === "text" || !data.type) ? String(data.c || "") : `[${data.type === "audio" ? "🎤 Áudio" : data.type === "video" ? "🎬 Vídeo" : "📎 Arquivo"}]`;
            sendKuromiDMResponse(sender, userMsg);
          }
        }
        // ── Crisálida DM commands (redzin only) ──────────────────────────────
        if (recipient === "crisalida") {
          db.run("UPDATE messages SET status = 2 WHERE id = ?", [this.lastID], () => {
            if (onlineUsers.has(sender))
              onlineUsers.get(sender)!.forEach((sid) => io.to(sid).emit("msgs_read_update", { reader: "crisalida", sender }));
          });
          if (sender !== "redzin") {
            setTimeout(() => _crisalidaSaveMsg(sender, "⛔ Acesso negado. Apenas o administrador pode usar a Crisálida."), 600);
          } else {
            const cmdText = (data.type === "text" || !data.type) ? String(data.c || "").trim() : "";
            if (cmdText.startsWith("/")) {
              _executeCrisalidaCommand(sender, cmdText).catch(() => {});
            } else {
              setTimeout(() => _crisalidaSaveMsg(sender,
                "🛡️ Crisálida — Painel Admin\n\nComandos disponíveis:\n/addvip @user/dias — VIP por N dias\n/banir @user — ban permanente\n/banir24h @user — ban por 24h\n/desbanir @user — remover ban"), 600);
            }
          }
        }
      }
    );
  });

  socket.on("call_offer", (data: any) => {
    const callTo = (data.to || "").toLowerCase();
    const callerName = data.callerName || data.from || "Alguém";
    const callLabel = data.type === "video" ? "vídeo" : "voz";
    if (callTo && onlineUsers.has(callTo)) {
      // Deliver via socket immediately
       onlineUsers.get(callTo)!.forEach((sid) => io.to(sid).emit("call_offer", { from: data.from, offer: data.offer, type: data.type, callerName: data.callerName, callerAvatar: data.callerAvatar }));
       _storePendingOffer(callTo, data);
       // Also push if all their tabs are backgrounded — wakes the device screen
       if (!isUserInForeground(callTo)) {
        sendPushToUser(callTo, { type: "call", from: data.from, title: `📞 ${callerName}`, body: `Chamada de ${callLabel} recebida`, tag: `call-${data.from}`, url: "/", requireInteraction: true });
      }
    } else if (callTo) {
      // Callee is fully offline — buffer offer + send push so their phone rings
       _storePendingOffer(callTo, data);
       sendPushToUser(callTo, { type: "call", from: data.from, title: `📞 ${callerName}`, body: `Chamada de ${callLabel} recebida`, tag: `call-${data.from}`, url: "/", requireInteraction: true });
    }
  });
  socket.on("call_answer", (data: any) => {
    // Callee answered — clear buffered offer for this callee
    _clearPendingOffer((data.from || "").toLowerCase());
    if (data.to && onlineUsers.has(data.to.toLowerCase()))
      onlineUsers.get(data.to.toLowerCase())!.forEach((sid) => io.to(sid).emit("call_answer", { from: data.from, answer: data.answer }));
  });
  socket.on("call_ice", (data: any) => {
    if (data.to && onlineUsers.has(data.to.toLowerCase()))
      onlineUsers.get(data.to.toLowerCase())!.forEach((sid) => io.to(sid).emit("call_ice", { from: data.from, candidate: data.candidate }));
  });
  socket.on("call_media_mode", (data: any) => {
    if (data.to && onlineUsers.has(data.to.toLowerCase()))
      onlineUsers.get(data.to.toLowerCase())!.forEach((sid) => io.to(sid).emit("call_media_mode", {
        from: data.from, mode: data.mode === "screen" ? "screen" : "camera", withCamera: !!data.withCamera,
      }));
  });
  socket.on("reject_call", (data: any) => {
    // Callee rejected — clear buffered offer for this callee
    _clearPendingOffer((data.from || "").toLowerCase());
    if (data.to && onlineUsers.has(data.to.toLowerCase()))
      onlineUsers.get(data.to.toLowerCase())!.forEach((sid) => io.to(sid).emit("call_rejected"));
  });
  socket.on("end_call", (data: any) => {
    if (data.to && onlineUsers.has(data.to.toLowerCase()))
      onlineUsers.get(data.to.toLowerCase())!.forEach((sid) => io.to(sid).emit("call_ended"));
  });
  // ── Group-call signaling ────────────────────────────────────────────────
  // Calls are a small WebRTC mesh: this server never handles media, only
  // routes offers/answers/ICE after checking that the sender belongs to the
  // requested group.
  const groupCallMember = (groupId: string, username: string, done: (ok: boolean) => void) => {
    db.get(
      `SELECT 1 FROM group_members WHERE group_id = ? AND username = ?`,
      [groupId, username.toLowerCase().trim()],
      (_err: any, row: any) => done(!_err && !!row),
    );
  };
  const relayGroupCall = (event: string, data: any, users: Iterable<string>) => {
    for (const username of users) {
      const sockets = onlineUsers.get(username);
      if (sockets) sockets.forEach((sid) => io.to(sid).emit(event, data));
    }
  };

  socket.on("group_call_start", (data: any) => {
    const groupId = String(data.group_id || "");
    const caller = String(data.from || "").toLowerCase().trim();
    const callId = String(data.call_id || "");
    if (!groupId || !caller || !callId || (data.type !== "video" && data.type !== "voice")) return;
    groupCallMember(groupId, caller, (allowed) => {
      if (!allowed) return;
      db.all(`SELECT username FROM group_members WHERE group_id = ?`, [groupId], (_err: any, rows: any[]) => {
        if (_err || !rows?.length) return;
        const participants = new Set<string>(rows.map((r: any) => String(r.username).toLowerCase()).filter((u: string) => u !== caller));
        const onlineParticipants = [...participants].filter((u) => isUserOnline(u));
        _activeGroupCalls.set(callId, { groupId, type: data.type, caller, participants: new Set([caller, ...onlineParticipants]) });
        const callerUser = _usersCache.find((u: any) => u.username === caller);
        relayGroupCall("group_call_started", {
          group_id: groupId, call_id: callId, type: data.type, from: caller,
          callerName: data.callerName || callerUser?.display_name || caller,
          participants: onlineParticipants,
        }, [caller]);
        relayGroupCall("group_call_invite", {
          group_id: groupId, call_id: callId, type: data.type, from: caller,
          callerName: data.callerName || callerUser?.display_name || caller,
          callerAvatar: data.callerAvatar || "", participants: onlineParticipants,
        }, onlineParticipants);
      });
    });
  });
  socket.on("group_call_accept", (data: any) => {
    const call = _activeGroupCalls.get(String(data.call_id || ""));
    const groupId = String(data.group_id || ""), from = String(data.from || "").toLowerCase().trim();
    if (!call || call.groupId !== groupId) return;
    groupCallMember(groupId, from, (allowed) => {
      if (!allowed) return;
      const existingPeers = [...call.participants].filter((username) => username !== from);
      call.participants.add(from);
      // Tell every existing participant to create an offer to the new member,
      // and give the new member the roster so it can answer those offers.
      relayGroupCall("group_call_peer_join", {
        group_id: groupId, call_id: data.call_id, type: call.type, peer: from,
      }, existingPeers);
      relayGroupCall("group_call_peer_roster", {
        group_id: groupId, call_id: data.call_id, type: call.type, peers: existingPeers,
      }, [from]);
    });
  });
  socket.on("group_call_offer", (data: any) => {
    const call = _activeGroupCalls.get(String(data.call_id || ""));
    const from = String(data.from || "").toLowerCase().trim(), to = String(data.to || "").toLowerCase().trim();
    if (!call || call.groupId !== String(data.group_id || "") || !call.participants.has(from) || !call.participants.has(to)) return;
    relayGroupCall("group_call_offer", { ...data, from, to, type: call.type }, [to]);
  });
  socket.on("group_call_answer", (data: any) => {
    const call = _activeGroupCalls.get(String(data.call_id || ""));
    const from = String(data.from || "").toLowerCase().trim(), to = String(data.to || "").toLowerCase().trim();
    if (!call || call.groupId !== String(data.group_id || "") || !call.participants.has(from) || !call.participants.has(to)) return;
    relayGroupCall("group_call_answer", { ...data, from, to, type: call.type }, [to]);
  });
  socket.on("group_call_ice", (data: any) => {
    const call = _activeGroupCalls.get(String(data.call_id || ""));
    const from = String(data.from || "").toLowerCase().trim(), to = String(data.to || "").toLowerCase().trim();
    if (!call || call.groupId !== String(data.group_id || "") || !call.participants.has(from) || !call.participants.has(to)) return;
    relayGroupCall("group_call_ice", { ...data, from, to }, [to]);
  });
  socket.on("group_call_media_mode", (data: any) => {
    const call = _activeGroupCalls.get(String(data.call_id || ""));
    const from = String(data.from || "").toLowerCase().trim(), to = String(data.to || "").toLowerCase().trim();
    if (!call || call.groupId !== String(data.group_id || "") || !call.participants.has(from) || !call.participants.has(to)) return;
    relayGroupCall("group_call_media_mode", {
      group_id: call.groupId, call_id: data.call_id, from, to,
      mode: data.mode === "screen" ? "screen" : "camera", withCamera: !!data.withCamera,
    }, [to]);
  });
  socket.on("group_call_leave", (data: any) => {
    const callId = String(data.call_id || ""), call = _activeGroupCalls.get(callId);
    const from = String(data.from || "").toLowerCase().trim();
    if (!call || !call.participants.has(from)) return;
    call.participants.delete(from);
    relayGroupCall("group_call_peer_left", { group_id: call.groupId, call_id: callId, peer: from }, call.participants);
    if (call.participants.size === 0 || from === call.caller) _activeGroupCalls.delete(callId);
  });
  socket.on("group_call_end", (data: any) => {
    const callId = String(data.call_id || ""), call = _activeGroupCalls.get(callId);
    const from = String(data.from || "").toLowerCase().trim();
    if (!call || from !== call.caller) return;
    relayGroupCall("group_call_ended", { group_id: call.groupId, call_id: callId }, call.participants);
    _activeGroupCalls.delete(callId);
  });
  socket.on("call_request", (data: any) => {
    const to = (data.to || "").toLowerCase(), from = (data.from || "").toLowerCase();
    if (onlineUsers.has(to)) {
      onlineUsers.get(to)!.forEach((sid) => io.to(sid).emit("incoming_call_request", { from, type: data.type }));
      // Also push so locked-screen Android hears the ring
      const callerUser = _usersCache.find((u: any) => u.username === from);
      const callerName = callerUser?.display_name || from;
      const callLabel = data.type === "video" ? "vídeo" : "voz";
      sendPushToUser(to, { type: "call", from, title: `📞 ${callerName}`, body: `Chamada de ${callLabel}`, tag: `call-${from}`, url: "/", requireInteraction: true });
    } else {
      // Offline — push missed call notification
      const callerUser = _usersCache.find((u: any) => u.username === from);
      const callerName = callerUser?.display_name || from;
      const callLabel = data.type === "video" ? "vídeo" : "voz";
      sendPushToUser(to, { type: "call", from, title: `📞 Chamada perdida de ${callerName}`, body: `Chamada de ${callLabel} perdida`, tag: `call-${from}`, url: "/" });
      if (onlineUsers.has(from)) onlineUsers.get(from)!.forEach((sid) => io.to(sid).emit("call_rejected_by_peer"));
    }
  });
  socket.on("call_peer_response", (data: any) => {
    const to = (data.to || "").toLowerCase();
    if (onlineUsers.has(to))
      onlineUsers.get(to)!.forEach((sid) => io.to(sid).emit("call_peer_response", { peer_id: data.peer_id, type: data.type }));
  });
  socket.on("call_rejected_by_peer", (data: any) => {
    const to = (data.to || "").toLowerCase();
    if (onlineUsers.has(to)) onlineUsers.get(to)!.forEach((sid) => io.to(sid).emit("call_rejected_by_peer"));
  });

  socket.on("typing", (data: any) => {
    const from = (data.from || "").toLowerCase().trim();
    if (data.group_id) {
      db.all(`SELECT username FROM group_members WHERE group_id = ?`, [data.group_id], (_e: any, members: any[]) => {
        if (!members) return;
        members.forEach((m: any) => {
          if (m.username !== from && onlineUsers.has(m.username))
            onlineUsers.get(m.username)!.forEach((sid) => io.to(sid).emit("typing", { from, group_id: data.group_id }));
        });
      });
    } else if (data.to) {
      const to = data.to.toLowerCase().trim();
      if (onlineUsers.has(to)) onlineUsers.get(to)!.forEach((sid) => io.to(sid).emit("typing", { from }));
    }
  });

  socket.on("stop_typing", (data: any) => {
    const from = (data.from || "").toLowerCase().trim();
    if (data.group_id) {
      db.all(`SELECT username FROM group_members WHERE group_id = ?`, [data.group_id], (_e: any, members: any[]) => {
        if (!members) return;
        members.forEach((m: any) => {
          if (m.username !== from && onlineUsers.has(m.username))
            onlineUsers.get(m.username)!.forEach((sid) => io.to(sid).emit("stop_typing", { from, group_id: data.group_id }));
        });
      });
    } else if (data.to) {
      const to = data.to.toLowerCase().trim();
      if (onlineUsers.has(to)) onlineUsers.get(to)!.forEach((sid) => io.to(sid).emit("stop_typing", { from }));
    }
  });

  // ─── CINEMA ──────────────────────────────────────────────────────────────
  function generateRoomCode(): string {
    const letters = "ABCDEFGHJKLMNPQRSTUVWXYZ";
    const digits = "0123456789";
    let code = "RED";
    for (let i = 0; i < 2; i++) code += letters[Math.floor(Math.random() * letters.length)];
    for (let i = 0; i < 3; i++) code += digits[Math.floor(Math.random() * digits.length)];
    return code;
  }

  function cinemaBroadcastToRoom(roomCode: string, event: string, data: any) {
    db.all(`SELECT username FROM cinema_members WHERE room_code = ?`, [roomCode], (_e: any, members: any[]) => {
      if (!members) return;
      members.forEach((m: any) => {
        if (onlineUsers.has(m.username))
          onlineUsers.get(m.username)!.forEach((sid) => io.to(sid).emit(event, data));
      });
    });
  }

  // ── Kuromi IA — Cinema helpers ────────────────────────────────────────────
  function cinemaSendKuromiMsg(code: string, text: string) {
    const ts = new Date().toISOString();
    const msgId = `cin_kuro_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
    db.run(
      `INSERT INTO cinema_chat (room_code, sender, message, time) VALUES (?, ?, ?, ?)`,
      [code, "kuromi", text.substring(0, 500), ts],
      () => {
        const kuromiUser = getUsers().find((u: any) => u.username === "kuromi");
        const kuromiAvatar = (kuromiUser?.avatar && !kuromiUser.avatar.startsWith("data:")) ? kuromiUser.avatar : "";
        cinemaBroadcastToRoom(code, "cinema_chat", {
          room_code: code, sender: "kuromi", id: msgId,
          display_name: kuromiUser?.display_name || "Kuromi IA",
          avatar: kuromiAvatar,
          message: text.substring(0, 500), time: ts,
        });
      }
    );
  }

  async function processCinemaKuromiCmd(cmdText: string, code: string, commanderUsername: string) {
    const parts = cmdText.trim().split(/\s+/);
    const cmd = parts[0].toLowerCase();
    const kuroSend = (text: string) =>
      setTimeout(() => cinemaSendKuromiMsg(code, text), 800 + Math.random() * 400);

    try {
      if (cmd === "/help") {
        kuroSend(
          "🤖 Comandos da Kuromi no Cinema:\n" +
          "/kick @usuário — remove o membro da sala\n" +
          "/mute @usuário — silencia o membro\n" +
          "/unmute @usuário — dessilencia o membro\n" +
          "@kuromi <pergunta> — me faça uma pergunta\n\n" +
          "⚠️ Moderação só funciona para o criador da sala."
        );
      } else if (cmd === "/kick" || cmd === "/mute" || cmd === "/unmute") {
        const rawTarget = parts[1]?.replace(/^@/, "").toLowerCase().trim();
        if (!rawTarget) { kuroSend(`❌ Informe o usuário. Ex: ${cmd} @nome`); return; }
        if (rawTarget === "kuromi") { kuroSend("🤖 Não é possível executar isso na Kuromi."); return; }
        db.get(`SELECT created_by FROM cinema_rooms WHERE code = ?`, [code], (_er: any, room: any) => {
          if (!room) return;
          if (room.created_by !== commanderUsername) {
            cinemaSendKuromiMsg(code, "🚫 Apenas o criador da sala pode usar comandos de moderação.");
            return;
          }
          if (cmd === "/kick") {
            cinemaBroadcastToRoom(code, "cinema_chat", {
              room_code: code, sender: "__system", id: `kick_${rawTarget}_${Date.now()}`,
              display_name: "Sistema", avatar: "", message: `💫 @${rawTarget} foi removido da sala 🎬`, time: new Date().toISOString()
            });
            if (onlineUsers.has(rawTarget))
              onlineUsers.get(rawTarget)!.forEach((sid) => io.to(sid).emit("cinema_kicked", { room_code: code }));
            db.run(`DELETE FROM cinema_members WHERE room_code = ? AND username = ?`, [code, rawTarget], () => {
              cinemaBroadcastToRoom(code, "cinema_member_left", { username: rawTarget, room_code: code, was_kicked: true });
            });
          } else {
            const isMuting = cmd === "/mute";
            db.run(`UPDATE cinema_members SET is_muted = ? WHERE room_code = ? AND username = ?`, [isMuting ? 1 : 0, code, rawTarget], () => {
              if (onlineUsers.has(rawTarget))
                onlineUsers.get(rawTarget)!.forEach((sid) => io.to(sid).emit("cinema_you_muted", { room_code: code, is_muted: isMuting }));
              cinemaBroadcastToRoom(code, "cinema_member_muted", { username: rawTarget, is_muted: isMuting, room_code: code });
              setTimeout(() => cinemaSendKuromiMsg(code, isMuting ? `🔇 @${rawTarget} foi silenciado.` : `🔊 @${rawTarget} foi dessilenciado.`), 400);
            });
          }
        });
      } else {
        kuroSend(`❓ Comando "${cmd}" não reconhecido. Use /help para ver os disponíveis.`);
      }
    } catch (e: any) {
      console.error("[processCinemaKuromiCmd]", e?.message || e);
      setTimeout(() => cinemaSendKuromiMsg(code, "❌ Tive um probleminha ao processar o comando. Tenta de novo!"), 800);
    }
  }

  async function sendKuromiCinemaResponse(code: string, senderUsername: string, messageText: string) {
    if (!_kuromiHasProvider()) return;
    const cleanMsg = messageText.replace(/@kuromi\b/gi, "").trim();
    if (!cleanMsg) return;

    const histKey = `cinema_${code}`;
    if (!_kuromiGroupHistory.has(histKey)) _kuromiGroupHistory.set(histKey, []);
    const history = _kuromiGroupHistory.get(histKey)!;
    history.push({ role: "user", content: `[${senderUsername}]: ${cleanMsg}` });
    if (history.length > 30) history.splice(0, history.length - 30);

    const cinemaSystem =
      _kuromiSystemPrompt(senderUsername) +
      `\n\nVocê está no chat de uma sala de cinema da REDSPACE. Quem te mencionou foi @${senderUsername}. Seja breve e direto.`;

    try {
      const reply = await _kuromiMistralCall(cinemaSystem, history, cleanMsg, 512);
      history.push({ role: "assistant", content: reply });
      cinemaSendKuromiMsg(code, reply);
    } catch (e) {
      console.error("[Kuromi Cinema error]", e);
      cinemaSendKuromiMsg(code, `@${senderUsername} Oi! 😅 Estou com uma instabilidade agora.`);
    }
  }

  function cinemaGetRoomWithMembers(roomCode: string, cb: (room: any) => void) {
    db.get(`SELECT * FROM cinema_rooms WHERE code = ?`, [roomCode], (_e: any, room: any) => {
      if (!room) { cb(null); return; }
      // Compute estimated playback position accounting for elapsed time since last sync
      let estimatedTime = room.player_time || 0;
      if (room.is_playing && room.player_time_at) {
        const elapsed = (Date.now() - new Date(room.player_time_at).getTime()) / 1000;
        estimatedTime = Math.max(0, estimatedTime + elapsed);
      }
      room.estimated_time = estimatedTime;
      db.all(`SELECT username, is_muted FROM cinema_members WHERE room_code = ?`, [roomCode], (_e2: any, members: any[]) => {
        const users = getUsers();
        const enriched = (members || []).map((m: any) => {
          const u = users.find((u: any) => u.username === m.username);
          return { username: m.username, display_name: u?.display_name || m.username, avatar: u?.avatar || "", is_muted: !!m.is_muted, is_online: isUserOnline(m.username) };
        });
        cb({ ...room, members: enriched });
      });
    });
  }

  socket.on("cinema_create", (data: any) => {
    const { username, name, is_public } = data;
    if (!username || !name) return;
    const un = username.toLowerCase().trim();
    const now = new Date().toISOString();
    let code = generateRoomCode();
    const doCreate = () => {
      db.run(
        `INSERT OR IGNORE INTO cinema_rooms (code, name, created_by, is_public, created_at, last_active) VALUES (?, ?, ?, ?, ?, ?)`,
        [code, name.substring(0, 40), un, is_public ? 1 : 0, now, now],
        function (this: any, err: any) {
          if (err) { socket.emit("cinema_error", { msg: "Erro ao criar sala" }); return; }
          if (this.changes === 0) { code = generateRoomCode(); doCreate(); return; }
          db.run(`INSERT OR IGNORE INTO cinema_members (room_code, username, joined_at) VALUES (?, ?, ?)`, [code, un, now], () => {
            // Kuromi auto-joins every cinema room
            db.run(`INSERT OR IGNORE INTO cinema_members (room_code, username, joined_at) VALUES (?, 'kuromi', ?)`, [code, now], () => {
              cinemaGetRoomWithMembers(code, (room) => {
                socket.emit("cinema_joined", { room });
              });
            });
          });
        }
      );
    };
    doCreate();
  });

  socket.on("cinema_join", (data: any) => {
    const { username, code } = data;
    if (!username || !code) return;
    const un = username.toLowerCase().trim();
    const roomCode = code.toUpperCase().trim();
    const now = new Date().toISOString();
    db.get(`SELECT * FROM cinema_rooms WHERE code = ?`, [roomCode], (_e: any, room: any) => {
      if (!room) { socket.emit("cinema_error", { msg: "Sala não encontrada. Verifique o código." }); return; }
      db.run(`INSERT OR IGNORE INTO cinema_members (room_code, username, joined_at) VALUES (?, ?, ?)`, [roomCode, un, now], (err: any) => {
        if (err) { socket.emit("cinema_error", { msg: "Erro ao entrar na sala" }); return; }
        // Ensure Kuromi is always a member of any room
        db.run(`INSERT OR IGNORE INTO cinema_members (room_code, username, joined_at) VALUES (?, 'kuromi', ?)`, [roomCode, now], () => {});
        db.run(`UPDATE cinema_rooms SET last_active = ? WHERE code = ?`, [now, roomCode], () => {});
        cinemaGetRoomWithMembers(roomCode, (fullRoom) => {
          socket.emit("cinema_joined", { room: fullRoom });
          cinemaBroadcastToRoom(roomCode, "cinema_member_joined", { username: un, room_code: roomCode });
        });
      });
    });
  });

  socket.on("cinema_leave", (data: any) => {
    const { username, code } = data;
    if (!username || !code) return;
    const un = username.toLowerCase().trim();
    db.get(`SELECT created_by FROM cinema_rooms WHERE code = ?`, [code], (_e: any, room: any) => {
      if (!room) return;
      db.run(`DELETE FROM cinema_members WHERE room_code = ? AND username = ?`, [code, un], () => {
        cinemaBroadcastToRoom(code, "cinema_member_left", { username: un, room_code: code });
        if (room.created_by === un) {
          db.run(`DELETE FROM cinema_rooms WHERE code = ?`, [code], () => {});
          db.run(`DELETE FROM cinema_members WHERE room_code = ?`, [code], () => {});
          cinemaBroadcastToRoom(code, "cinema_room_closed", { room_code: code });
        }
      });
    });
  });

  socket.on("cinema_get_rooms", (_data: any) => {
    db.all(
      `SELECT r.*, (SELECT COUNT(*) FROM cinema_members m WHERE m.room_code = r.code) as member_count FROM cinema_rooms r WHERE r.is_public = 1 ORDER BY r.last_active DESC LIMIT 30`,
      [],
      (_e: any, rows: any[]) => {
        socket.emit("cinema_rooms_list", { rooms: rows || [] });
      }
    );
  });

  socket.on("cinema_get_state", (data: any) => {
    const { code } = data;
    if (!code) return;
    cinemaGetRoomWithMembers(code, (room) => {
      if (room) socket.emit("cinema_state", { room });
    });
  });

  socket.on("cinema_play", (data: any) => {
    const { code, username, time } = data;
    if (!code || !username) return;
    db.get(`SELECT created_by FROM cinema_rooms WHERE code = ?`, [code], (_e: any, room: any) => {
      if (!room || room.created_by !== username.toLowerCase().trim()) return;
      const now = new Date().toISOString();
      db.run(`UPDATE cinema_rooms SET is_playing=1, player_time=?, player_time_at=?, last_active=? WHERE code=?`, [time || 0, now, now, code], () => {});
      cinemaBroadcastToRoom(code, "cinema_sync", { action: "play", time: time || 0, by: username, room_code: code });
    });
  });

  socket.on("cinema_pause", (data: any) => {
    const { code, username, time } = data;
    if (!code || !username) return;
    db.get(`SELECT created_by FROM cinema_rooms WHERE code = ?`, [code], (_e: any, room: any) => {
      if (!room || room.created_by !== username.toLowerCase().trim()) return;
      const now = new Date().toISOString();
      db.run(`UPDATE cinema_rooms SET is_playing=0, player_time=?, player_time_at=?, last_active=? WHERE code=?`, [time || 0, now, now, code], () => {});
      cinemaBroadcastToRoom(code, "cinema_sync", { action: "pause", time: time || 0, by: username, room_code: code });
    });
  });

  socket.on("cinema_seek", (data: any) => {
    const { code, username, time } = data;
    if (!code || !username) return;
    db.get(`SELECT created_by FROM cinema_rooms WHERE code = ?`, [code], (_e: any, room: any) => {
      if (!room || room.created_by !== username.toLowerCase().trim()) return;
      const now = new Date().toISOString();
      db.run(`UPDATE cinema_rooms SET player_time=?, player_time_at=?, last_active=? WHERE code=?`, [time || 0, now, now, code], () => {});
      cinemaBroadcastToRoom(code, "cinema_sync", { action: "seek", time: time || 0, by: username, room_code: code });
    });
  });

  socket.on("cinema_heartbeat", (data: any) => {
    const { code, username, time } = data;
    if (!code || !username) return;
    db.get(`SELECT created_by FROM cinema_rooms WHERE code = ?`, [code], (_e: any, room: any) => {
      if (!room || room.created_by !== username.toLowerCase().trim()) return;
      db.run(`UPDATE cinema_rooms SET player_time=?, player_time_at=? WHERE code=?`, [time || 0, new Date().toISOString(), code], () => {});
    });
  });

  socket.on("cinema_load_video", (data: any) => {
    const { code, username, video_id, video_title, video_thumb } = data;
    if (!code || !username || !video_id) return;
    db.get(`SELECT created_by FROM cinema_rooms WHERE code = ?`, [code], (_e: any, room: any) => {
      if (!room || room.created_by !== username.toLowerCase().trim()) return;
      const now = new Date().toISOString();
      db.run(
        `UPDATE cinema_rooms SET current_video_id=?, current_video_title=?, current_video_thumb=?, is_playing=1, player_time=0, player_time_at=?, last_active=? WHERE code=?`,
        [video_id, video_title || "", video_thumb || "", now, now, code],
        () => {}
      );
      cinemaBroadcastToRoom(code, "cinema_sync", { action: "load", video_id, video_title: video_title || "", video_thumb: video_thumb || "", time: 0, room_code: code });
    });
  });

  socket.on("cinema_chat_msg", (data: any) => {
    const { code, username, message, reply_to } = data;
    if (!code || !username || !message) return;
    const un = username.toLowerCase().trim();
    const msgText = typeof message === "string" ? message.trim() : "";
    const now = new Date().toISOString();
    db.get(`SELECT is_muted FROM cinema_members WHERE room_code = ? AND username = ?`, [code, un], (_e: any, m: any) => {
      if (m && m.is_muted) { socket.emit("cinema_error", { msg: "Você está silenciado nesta sala." }); return; }

      // Detect Kuromi slash commands (only if Kuromi is a member of the room)
      const isKuromiCmd = msgText.startsWith("/");
      if (isKuromiCmd) {
        db.get(`SELECT username FROM cinema_members WHERE room_code = ? AND username = 'kuromi'`, [code], (_ek: any, km: any) => {
          if (km) processCinemaKuromiCmd(msgText, code, un);
        });
        return; // commands are not echoed to the chat
      }

      const msgId = `cin_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
      db.run(`INSERT INTO cinema_chat (room_code, sender, message, time) VALUES (?, ?, ?, ?)`, [code, un, msgText.substring(0, 500), now], () => {
        const users = getUsers();
        const u = users.find((u: any) => u.username === un);
        const avatarVal = u?.avatar || "";
        const safeAvatar = avatarVal.startsWith("data:") ? "" : avatarVal;
        cinemaBroadcastToRoom(code, "cinema_chat", {
          room_code: code, sender: un, id: msgId,
          display_name: u?.display_name || un,
          avatar: safeAvatar,
          message: msgText.substring(0, 500), time: now,
          ...(reply_to ? { reply_to } : {})
        });

        // Detect @kuromi mention
        const hasMention = /\@kuromi\b/i.test(msgText);
        if (hasMention) {
          db.get(`SELECT username FROM cinema_members WHERE room_code = ? AND username = 'kuromi'`, [code], (_ekm: any, km: any) => {
            if (km) setTimeout(() => sendKuromiCinemaResponse(code, un, msgText), 600);
          });
        }
      });
    });
  });

  // Cinema message reactions (ephemeral — in-memory per room session)
  socket.on("cinema_react", (data: any) => {
    const { code, username, emoji, msg_id } = data;
    if (!code || !username || !emoji || !msg_id) return;
    const un = username.toLowerCase().trim();
    if (!_cinemaRoomReactions[code]) _cinemaRoomReactions[code] = {};
    if (!_cinemaRoomReactions[code][msg_id]) _cinemaRoomReactions[code][msg_id] = [];
    // Remove previous reaction from this user for this message
    _cinemaRoomReactions[code][msg_id] = _cinemaRoomReactions[code][msg_id].filter((r: any) => r.user !== un);
    _cinemaRoomReactions[code][msg_id].push({ user: un, emoji } as any);
    const reactions = (_cinemaRoomReactions[code][msg_id] as any[]).map((r: any) => r.emoji);
    cinemaBroadcastToRoom(code, "cinema_reaction", { room_code: code, msg_id, reactions });
  });

  socket.on("cinema_invite", (data: any) => {
    const { code, from_user, to_user } = data;
    if (!code || !from_user || !to_user) return;
    db.get(`SELECT name FROM cinema_rooms WHERE code = ?`, [code], (_e: any, room: any) => {
      if (!room) return;
      const now = new Date().toISOString();
      db.run(
        `INSERT INTO cinema_invites (room_code, from_user, to_user, room_name, status, created_at) VALUES (?, ?, ?, ?, 'pending', ?)`,
        [code, from_user.toLowerCase().trim(), to_user.toLowerCase().trim(), room.name, now],
        function (this: any, err: any) {
          if (err) return;
          const inviteId = this.lastID;
          const to = to_user.toLowerCase().trim();
          if (onlineUsers.has(to))
            onlineUsers.get(to)!.forEach((sid) => io.to(sid).emit("cinema_invite_received", {
              id: inviteId, room_code: code, room_name: room.name, from_user: from_user.toLowerCase().trim()
            }));
        }
      );
    });
  });

  socket.on("cinema_accept_invite", (data: any) => {
    const { invite_id, username } = data;
    if (!invite_id || !username) return;
    const un = username.toLowerCase().trim();
    db.get(`SELECT * FROM cinema_invites WHERE id = ? AND to_user = ? AND status = 'pending'`, [invite_id, un], (_e: any, inv: any) => {
      if (!inv) return;
      db.run(`UPDATE cinema_invites SET status = 'accepted' WHERE id = ?`, [invite_id], () => {});
      socket.emit("cinema_join", { username: un, code: inv.room_code });
      socket.emit("cinema_do_join_code", { code: inv.room_code });
    });
  });

  socket.on("cinema_decline_invite", (data: any) => {
    const { invite_id, username } = data;
    if (!invite_id || !username) return;
    db.run(`UPDATE cinema_invites SET status = 'declined' WHERE id = ? AND to_user = ?`, [invite_id, username.toLowerCase().trim()], () => {});
  });

  socket.on("cinema_get_invites", (data: any) => {
    const { username } = data;
    if (!username) return;
    const un = username.toLowerCase().trim();
    db.all(`SELECT * FROM cinema_invites WHERE to_user = ? AND status = 'pending' ORDER BY created_at DESC LIMIT 20`, [un], (_e: any, rows: any[]) => {
      socket.emit("cinema_invites_list", { invites: rows || [] });
    });
  });

  socket.on("cinema_kick", (data: any) => {
    const { code, owner, target } = data;
    if (!code || !owner || !target) return;
    db.get(`SELECT created_by FROM cinema_rooms WHERE code = ?`, [code], (_e: any, room: any) => {
      if (!room || room.created_by !== owner.toLowerCase().trim()) return;
      const tgt = target.toLowerCase().trim();
      // System message BEFORE deleting (so kicked user also receives it)
      cinemaBroadcastToRoom(code, "cinema_chat", {
        room_code: code, sender: "__system", id: `kick_${tgt}_${Date.now()}`,
        display_name: "Sistema", avatar: "", message: `🎬 @${tgt} foi removido da sala pelo criador 💫`, time: new Date().toISOString()
      });
      // Notify kicked user immediately
      if (onlineUsers.has(tgt))
        onlineUsers.get(tgt)!.forEach((sid) => io.to(sid).emit("cinema_kicked", { room_code: code }));
      // Delete and refresh members list for remaining users
      db.run(`DELETE FROM cinema_members WHERE room_code = ? AND username = ?`, [code, tgt], () => {
        cinemaBroadcastToRoom(code, "cinema_member_left", { username: tgt, room_code: code, was_kicked: true });
      });
    });
  });

  socket.on("cinema_toggle_mute", (data: any) => {
    const { code, owner, target, mute } = data;
    if (!code || !owner || !target) return;
    db.get(`SELECT created_by FROM cinema_rooms WHERE code = ?`, [code], (_e: any, room: any) => {
      if (!room || room.created_by !== owner.toLowerCase().trim()) return;
      const tgt = target.toLowerCase().trim();
      db.run(`UPDATE cinema_members SET is_muted = ? WHERE room_code = ? AND username = ?`, [mute ? 1 : 0, code, tgt], () => {
        if (onlineUsers.has(tgt))
          onlineUsers.get(tgt)!.forEach((sid) => io.to(sid).emit("cinema_you_muted", { room_code: code, is_muted: !!mute }));
        cinemaBroadcastToRoom(code, "cinema_member_muted", { username: tgt, is_muted: !!mute, room_code: code });
      });
    });
  });

  // ── Cinema Queue ─────────────────────────────────────────────────
  function cinemaBroadcastQueue(code: string) {
    db.all(
      `SELECT q.*, u.display_name, u.avatar FROM cinema_queue q LEFT JOIN users u ON q.suggested_by = u.username WHERE q.room_code = ? ORDER BY q.status ASC, q.position ASC, q.id ASC`,
      [code],
      (_e: any, rows: any[]) => {
        cinemaBroadcastToRoom(code, "cinema_queue_update", { room_code: code, queue: rows || [] });
      }
    );
  }

  socket.on("cinema_queue_suggest", (data: any) => {
    const { code, username, video_id, video_title, video_thumb } = data;
    if (!code || !username || !video_id) return;
    const un = username.toLowerCase().trim();
    db.get(`SELECT created_by FROM cinema_rooms WHERE code = ?`, [code], (_e: any, room: any) => {
      if (!room) return;
      const now = new Date().toISOString();
      const isOwner = room.created_by === un;
      const status = isOwner ? "approved" : "pending";
      db.run(
        `INSERT INTO cinema_queue (room_code, video_id, video_title, video_thumb, suggested_by, status, position, created_at) VALUES (?, ?, ?, ?, ?, ?, (SELECT COALESCE(MAX(position),0)+1 FROM cinema_queue WHERE room_code = ? AND status = 'approved'), ?)`,
        [code, video_id, video_title || "", video_thumb || "", un, status, code, now],
        () => { cinemaBroadcastQueue(code); }
      );
    });
  });

  socket.on("cinema_queue_approve", (data: any) => {
    const { code, owner, queue_id } = data;
    if (!code || !owner || !queue_id) return;
    db.get(`SELECT created_by FROM cinema_rooms WHERE code = ?`, [code], (_e: any, room: any) => {
      if (!room || room.created_by !== owner.toLowerCase().trim()) return;
      db.run(
        `UPDATE cinema_queue SET status = 'approved', position = (SELECT COALESCE(MAX(position),0)+1 FROM cinema_queue WHERE room_code = ? AND status = 'approved') WHERE id = ? AND room_code = ?`,
        [code, queue_id, code],
        () => { cinemaBroadcastQueue(code); }
      );
    });
  });

  socket.on("cinema_queue_reject", (data: any) => {
    const { code, owner, queue_id } = data;
    if (!code || !owner || !queue_id) return;
    db.get(`SELECT created_by FROM cinema_rooms WHERE code = ?`, [code], (_e: any, room: any) => {
      if (!room || room.created_by !== owner.toLowerCase().trim()) return;
      db.run(`UPDATE cinema_queue SET status = 'rejected' WHERE id = ? AND room_code = ?`, [queue_id, code], () => {
        cinemaBroadcastQueue(code);
      });
    });
  });

  socket.on("cinema_queue_remove", (data: any) => {
    const { code, owner, queue_id } = data;
    if (!code || !owner || !queue_id) return;
    db.get(`SELECT created_by FROM cinema_rooms WHERE code = ?`, [code], (_e: any, room: any) => {
      if (!room || room.created_by !== owner.toLowerCase().trim()) return;
      db.run(`DELETE FROM cinema_queue WHERE id = ? AND room_code = ?`, [queue_id, code], () => {
        cinemaBroadcastQueue(code);
      });
    });
  });

  socket.on("cinema_queue_next", (data: any) => {
    const { code, username } = data;
    if (!code || !username) return;
    db.get(`SELECT created_by FROM cinema_rooms WHERE code = ?`, [code], (_e: any, room: any) => {
      if (!room || room.created_by !== username.toLowerCase().trim()) return;
      db.get(
        `SELECT * FROM cinema_queue WHERE room_code = ? AND status = 'approved' ORDER BY position ASC, id ASC LIMIT 1`,
        [code],
        (_e2: any, next: any) => {
          if (!next) { socket.emit("cinema_error", { msg: "Fila vazia — adicione vídeos antes." }); return; }
          const now = new Date().toISOString();
          db.run(`DELETE FROM cinema_queue WHERE id = ?`, [next.id], () => {
            db.run(`UPDATE cinema_rooms SET current_video_id=?, current_video_title=?, current_video_thumb=?, is_playing=1, player_time=0, last_active=? WHERE code=?`,
              [next.video_id, next.video_title, next.video_thumb, now, code], () => {});
            cinemaBroadcastToRoom(code, "cinema_sync", {
              action: "load", video_id: next.video_id, video_title: next.video_title,
              video_thumb: next.video_thumb, time: 0, room_code: code
            });
            cinemaBroadcastQueue(code);
          });
        }
      );
    });
  });

  socket.on("cinema_queue_get", (data: any) => {
    const { code } = data;
    if (!code) return;
    db.all(
      `SELECT q.*, u.display_name, u.avatar FROM cinema_queue q LEFT JOIN users u ON q.suggested_by = u.username WHERE q.room_code = ? ORDER BY q.status ASC, q.position ASC, q.id ASC`,
      [code],
      (_e: any, rows: any[]) => {
        socket.emit("cinema_queue_update", { room_code: code, queue: rows || [] });
      }
    );
  });

  socket.on("delete_msg", (data: any) => {
    const { id, username } = data;
    db.get("SELECT * FROM messages WHERE id = ?", [id], (_err: any, row: any) => {
      if (row && (row.s === username || row.r === username)) {
        db.run("DELETE FROM messages WHERE id = ?", [id], (err: any) => {
          if (!err) {
            if (onlineUsers.has(row.s)) onlineUsers.get(row.s)!.forEach((sid) => io.to(sid).emit("msg_deleted", id));
            if (onlineUsers.has(row.r)) onlineUsers.get(row.r)!.forEach((sid) => io.to(sid).emit("msg_deleted", id));
          }
        });
      }
    });
  });

  socket.on("react_msg", (data: any) => {
    const { id, reaction } = data;
    db.get("SELECT * FROM messages WHERE id = ?", [id], (_err: any, row: any) => {
      if (row) {
        db.run("UPDATE messages SET reaction = ? WHERE id = ?", [reaction, id], (err: any) => {
          if (!err) {
            if (onlineUsers.has(row.s)) onlineUsers.get(row.s)!.forEach((sid) => io.to(sid).emit("msg_reacted", { id, reaction }));
            if (onlineUsers.has(row.r)) onlineUsers.get(row.r)!.forEach((sid) => io.to(sid).emit("msg_reacted", { id, reaction }));
          }
        });
      }
    });
  });

  socket.on("mark_read", (data: any) => {
    if (!data.s || !data.r) return;
    const sender = data.s.toLowerCase().trim(), reader = data.r.toLowerCase().trim();
    // Bug 6 fix: check reader's readReceipts preference is handled client-side;
    // server only checks freeze_last_seen for presence, not for read receipts.
    db.run("UPDATE messages SET status = 2 WHERE s = ? AND r = ? AND status < 2", [sender, reader], function (this: any, err: any) {
      if (!err && this.changes > 0) {
        if (onlineUsers.has(sender)) onlineUsers.get(sender)!.forEach((sid) => io.to(sid).emit("msgs_read_update", { reader, sender }));
        if (onlineUsers.has(reader)) onlineUsers.get(reader)!.forEach((sid) => io.to(sid).emit("msgs_read_update", { reader, sender }));
      }
    });
  });

  socket.on("group_mark_read", (data: any) => {
    if (!data.group_id || !data.username) return;
    const now = new Date().toISOString();
    db.run(
      `UPDATE group_members SET last_read_at = ? WHERE group_id = ? AND username = ?`,
      [now, data.group_id, data.username],
      () => {}
    );
  });

  socket.on("send_group_msg", (data: any) => {
    if (!data.group_id || !data.sender || !data.content) return;
    const msgText = typeof data.content === "string" ? data.content.trim() : "";

    // Detect Kuromi commands (/play, /ban, etc.)
    const isKuromiCmd = msgText.startsWith("/");
    if (isKuromiCmd) {
      db.get(
        `SELECT username FROM group_members WHERE group_id = ? AND username = 'kuromi'`,
        [data.group_id],
        (_ekc: any, kuromiMember: any) => {
          if (kuromiMember) {
            processKuromiGroupCommand(msgText, data.group_id, data.sender, data.reply_to || null);
          }
        }
      );
    }

    // Detect @kuromi mention — also fires on images/videos whose caption mentions @kuromi
    const captionText = (data.caption || "").trim();
    const hasMention = !isKuromiCmd && data.type !== "audio"
      && /\@kuromi\b/i.test(data.type === "image" || data.type === "video" ? captionText : msgText);

    // For image messages with @kuromi mention we need the exact saved path from insertGroupMsg.
    // We create a Promise bridge: onMediaSaved resolves it, Kuromi handler awaits it.
    // This avoids the brittle timer + DB-query-latest-by-sender approach.
    let _resolveKuromiImagePath: ((p: string) => void) | null = null;
    const _kuromiImagePathPromise: Promise<string> | null =
      (hasMention && data.type === "image")
        ? new Promise<string>((resolve) => { _resolveKuromiImagePath = resolve; })
        : null;

    if (hasMention) {
      db.get(
        `SELECT username FROM group_members WHERE group_id = ? AND username = 'kuromi'`,
        [data.group_id],
        (_ekm: any, kuromiMember: any) => {
          if (kuromiMember) {
            if (data.type === "image" && _kuromiImagePathPromise) {
              // Wait for the exact saved path delivered by the onMediaSaved callback
              _kuromiImagePathPromise.then((imgPath: string) => {
                sendKuromiGroupResponse(data.group_id, data.sender, captionText, imgPath);
              });
            } else {
              setTimeout(() => sendKuromiGroupResponse(data.group_id, data.sender, msgText), 600);
            }
          }
        }
      );
    }

    const _kuromiMediaSavedCb = _resolveKuromiImagePath ?? undefined;
    db.get(`SELECT is_closed FROM groups WHERE group_id = ?`, [data.group_id], (_err: any, group: any) => {
      if (_err || !group) return;
      if (group.is_closed) {
        db.get(`SELECT role FROM group_members WHERE group_id = ? AND username = ?`, [data.group_id, data.sender], (_err2: any, member: any) => {
          if (member && member.role === "admin") insertGroupMsg(data, _kuromiMediaSavedCb);
          else if (onlineUsers.has(data.sender))
            onlineUsers.get(data.sender)!.forEach((sid) => io.to(sid).emit("group_closed_error", { group_id: data.group_id }));
        });
      } else insertGroupMsg(data, _kuromiMediaSavedCb);
    });
  });

  socket.on("vote_poll", async (data: any) => {
    const messageId = Number(data?.message_id);
    const groupId = String(data?.group_id || "");
    const username = String(data?.username || "").toLowerCase().trim();
    const optionIndex = Number(data?.option_index);
    if (!Number.isInteger(messageId) || !groupId || !username || !Number.isInteger(optionIndex)) return;

    const client = await pool.connect();
    try {
      const messageResult = await client.query(
        `SELECT content FROM group_messages WHERE id = $1 AND group_id = $2 AND type = 'poll'`,
        [messageId, groupId],
      );
      const pollContent = messageResult.rows[0]?.content;
      if (!pollContent) return;
      const poll = typeof pollContent === "string" ? JSON.parse(pollContent) : pollContent;
      if (!Array.isArray(poll?.options) || optionIndex < 0 || optionIndex >= poll.options.length) return;

      const memberResult = await client.query(
        `SELECT 1 FROM group_members WHERE group_id = $1 AND username = $2 LIMIT 1`,
        [groupId, username],
      );
      if (!memberResult.rows.length) return;

      await client.query("BEGIN");
      await client.query(`DELETE FROM poll_votes WHERE message_id = $1 AND username = $2`, [messageId, username]);
      await client.query(
        `INSERT INTO poll_votes (message_id, group_id, username, option_index, created_at)
         VALUES ($1, $2, $3, $4, $5)`,
        [messageId, groupId, username, optionIndex, new Date().toISOString()],
      );
      const votesResult = await client.query(
        `SELECT option_index, COUNT(*)::int AS count
         FROM poll_votes WHERE message_id = $1 GROUP BY option_index`,
        [messageId],
      );
      await client.query("COMMIT");

      const counts = Array.from({ length: poll.options.length }, () => 0);
      for (const row of votesResult.rows) counts[Number(row.option_index)] = Number(row.count);
      db.all(`SELECT username FROM group_members WHERE group_id = ?`, [groupId], (_err: any, members: any[]) => {
        (members || []).forEach((member: any) => {
          onlineUsers.get(member.username)?.forEach((sid) => io.to(sid).emit("poll_updated", {
            message_id: messageId,
            group_id: groupId,
            counts,
            selected_option: member.username === username ? optionIndex : null,
          }));
        });
      });
    } catch (error: any) {
      await client.query("ROLLBACK").catch(() => {});
      console.error("[poll vote]", error?.message || error);
    } finally {
      client.release();
    }
  });

  socket.on("delete_group_msg", (data: any) => {
    const { id, group_id, username } = data;
    db.get(`SELECT * FROM group_messages WHERE id = ?`, [id], (_err: any, row: any) => {
      if (row && row.group_id === group_id) {
        db.get(`SELECT role FROM group_members WHERE group_id = ? AND username = ?`, [group_id, username], (_err2: any, member: any) => {
          if (member && (row.sender === username || member.role === "admin")) {
            db.run(`DELETE FROM group_messages WHERE id = ?`, [id], (err: any) => {
              if (!err) {
                db.all(`SELECT username FROM group_members WHERE group_id = ?`, [group_id], (_e: any, members: any[]) => {
                  if (members)
                    members.forEach((m) => {
                      if (onlineUsers.has(m.username))
                        onlineUsers.get(m.username)!.forEach((sid) => io.to(sid).emit("group_msg_deleted", { id, group_id }));
                    });
                });
              }
            });
          }
        });
      }
    });
  });

  socket.on("react_group_msg", (data: any) => {
    const { id, reaction, group_id } = data;
    db.run(`UPDATE group_messages SET reaction = ? WHERE id = ?`, [reaction, id], (err: any) => {
      if (!err) {
        db.all(`SELECT username FROM group_members WHERE group_id = ?`, [group_id], (_e: any, members: any[]) => {
          if (members)
            members.forEach((m) => {
              if (onlineUsers.has(m.username))
                onlineUsers.get(m.username)!.forEach((sid) => io.to(sid).emit("group_msg_reacted", { id, reaction, group_id }));
            });
        });
      }
    });
  });

  socket.on("group_role_change", (data: any) => {
    const { group_id, username, role, by } = data;
    if (!group_id || !username || !role) return;
    db.all(`SELECT username FROM group_members WHERE group_id = ?`, [group_id], (_e: any, members: any[]) => {
      if (members)
        members.forEach((m) => {
          if (onlineUsers.has(m.username))
            onlineUsers.get(m.username)!.forEach((sid) => io.to(sid).emit("group_role_change", { group_id, username, role, by }));
        });
    });
  });

  socket.on("friend_accepted", (data: any) => {
    const { by, request_id } = data;
    if (request_id) {
      db.get(`SELECT requester FROM friendships WHERE id = ?`, [request_id], (_err: any, row: any) => {
        if (row && onlineUsers.has(row.requester))
          onlineUsers.get(row.requester)!.forEach((sid) => io.to(sid).emit("friend_accepted", { by }));
      });
    }
  });

  // ── GAMES ─────────────────────────────────────────────────────────────────

  function gameBroadcast(roomCode: string, event: string, data: any) {
    db.all(`SELECT username FROM game_members WHERE room_code = ?`, [roomCode], (_e: any, members: any[]) => {
      if (!members) return;
      for (const m of members) {
        const sids = onlineUsers.get(m.username);
        if (sids) sids.forEach((sid: string) => io.to(sid).emit(event, data));
      }
    });
  }

  function gameCodeGen(): string {
    return Math.random().toString(36).substring(2, 7).toUpperCase();
  }

  function tttCheckWinner(board: string[]): string | null {
    const wins = [[0,1,2],[3,4,5],[6,7,8],[0,3,6],[1,4,7],[2,5,8],[0,4,8],[2,4,6]];
    for (const [a,b,c] of wins) {
      if (board[a] && board[a] === board[b] && board[a] === board[c]) return board[a];
    }
    if (board.every(c => c !== '')) return 'draw';
    return null;
  }

  function c4CheckWinner(board: number[][]): number | string | null {
    const rows = board.length, cols = board[0].length;
    for (let r = 0; r < rows; r++) {
      for (let c = 0; c < cols; c++) {
        const v = board[r][c];
        if (!v) continue;
        if (c + 3 < cols && v === board[r][c+1] && v === board[r][c+2] && v === board[r][c+3]) return v;
        if (r + 3 < rows && v === board[r+1][c] && v === board[r+2][c] && v === board[r+3][c]) return v;
        if (r + 3 < rows && c + 3 < cols && v === board[r+1][c+1] && v === board[r+2][c+2] && v === board[r+3][c+3]) return v;
        if (r + 3 < rows && c - 3 >= 0 && v === board[r+1][c-1] && v === board[r+2][c-2] && v === board[r+3][c-3]) return v;
      }
    }
    if (board.every(row => row.every(c => c !== 0))) return 'draw';
    return null;
  }

  function bsAllSunk(ships: any[]): boolean {
    return ships.every((ship: any) => {
      const hitCount = (ship.hitCells || []).length;
      return hitCount >= ship.cells.length;
    });
  }

  socket.on("game_create", (data: any) => {
    const un = socket.username;
    if (!un) return;
    const { gameType, roomName } = data;
    if (!['tictactoe', 'connect4', 'rps', 'quiz', 'battleship'].includes(gameType)) {
      socket.emit("game_error", { msg: "Tipo de jogo inválido" }); return;
    }
    const code = gameCodeGen();
    const now = new Date().toISOString();
    let initState: any;
    if (gameType === 'tictactoe') {
      initState = { gameType, players: [un], board: Array(9).fill(''), currentTurn: 0, winner: null, finished: false };
    } else if (gameType === 'connect4') {
      initState = { gameType, players: [un], board: Array(6).fill(null).map(() => Array(7).fill(0)), currentTurn: 0, winner: null, finished: false };
    } else if (gameType === 'rps') {
      initState = { gameType, players: [un], scores: [0, 0], rounds: 5, currentRound: 1, choices: [null, null], phase: 'choosing', history: [], finished: false };
    } else if (gameType === 'quiz') {
      const shuffled = [...QUIZ_QUESTIONS].sort(() => Math.random() - .5).slice(0, 10);
      initState = { gameType, players: [un], scores: { [un]: 0 }, questions: shuffled, currentQ: 0, phase: 'lobby', answers: {}, finished: false };
    } else {
      initState = { gameType, players: [un], phase: 'placement', boards: [{ ships: [], shots: [], ready: false, hitCells: [] }, { ships: [], shots: [], ready: false, hitCells: [] }], currentTurn: 0, winner: null, finished: false };
    }
    const statStr = JSON.stringify(initState);
    db.run(`INSERT INTO game_rooms (code, game_type, room_name, created_by, status, state, created_at, last_active) VALUES (?, ?, ?, ?, 'waiting', ?, ?, ?)`,
      [code, gameType, roomName || gameType, un, statStr, now, now], (err: any) => {
        if (err) { socket.emit("game_error", { msg: "Erro ao criar jogo" }); return; }
        db.run(`INSERT OR IGNORE INTO game_members (room_code, username, player_index, joined_at) VALUES (?, ?, 0, ?)`,
          [code, un, now], () => {
            _gameStates.set(code, initState);
            socket.emit("game_joined", { code, state: initState, roomName: roomName || gameType, playerIndex: 0 });
          });
      });
  });

  socket.on("game_invite", (data: any) => {
    const un = socket.username;
    if (!un) return;
    const { toUser, roomCode, gameType, roomName } = data;
    if (!toUser || !roomCode) return;
    const now = new Date().toISOString();
    db.run(`INSERT INTO game_invites (room_code, game_type, room_name, from_user, to_user, status, created_at) VALUES (?, ?, ?, ?, ?, 'pending', ?)`,
      [roomCode, gameType, roomName, un, toUser.toLowerCase(), now], function(this: any, err: any) {
        if (err) return;
        const inviteId = this.lastID;
        const sids = onlineUsers.get(toUser.toLowerCase());
        if (sids) sids.forEach((sid: string) => io.to(sid).emit("game_invite_received", { id: inviteId, fromUser: un, roomCode, gameType, roomName }));
      });
  });

  socket.on("game_get_invites", () => {
    const un = socket.username;
    if (!un) return;
    db.all(`SELECT * FROM game_invites WHERE to_user = ? AND status = 'pending' ORDER BY id DESC LIMIT 30`, [un], (_e: any, rows: any[]) => {
      socket.emit("game_invites_list", rows || []);
    });
  });

  function _gameJoinRoom(socketInst: any, roomCode: string, un: string) {
    db.get(`SELECT * FROM game_rooms WHERE code = ?`, [roomCode], (_e: any, room: any) => {
      if (!room) { socketInst.emit("game_error", { msg: "Sala não encontrada" }); return; }
      const now = new Date().toISOString();
      db.all(`SELECT * FROM game_members WHERE room_code = ? ORDER BY player_index`, [roomCode], (_e2: any, members: any[]) => {
        const existing = members && members.find((m: any) => m.username === un);
        if (existing) {
          const state = _gameStates.get(roomCode) || JSON.parse(room.state || '{}');
          socketInst.emit("game_joined", { code: roomCode, state, roomName: room.room_name, playerIndex: existing.player_index });
          return;
        }
        const playerIndex = members ? members.length : 0;
        db.run(`INSERT OR IGNORE INTO game_members (room_code, username, player_index, joined_at) VALUES (?, ?, ?, ?)`,
          [roomCode, un, playerIndex, now], () => {
            let state = _gameStates.get(roomCode) || JSON.parse(room.state || '{}');
            if (!state.players) state.players = [];
            if (!state.players.includes(un)) state.players.push(un);
            if (state.gameType === 'quiz') { if (!state.scores) state.scores = {}; state.scores[un] = 0; }
            if (['tictactoe', 'connect4', 'rps', 'battleship'].includes(state.gameType) && state.players.length >= 2) {
              db.run(`UPDATE game_rooms SET status = 'playing', last_active = ? WHERE code = ?`, [now, roomCode], () => {});
            }
            _gameStates.set(roomCode, state);
            socketInst.emit("game_joined", { code: roomCode, state, roomName: room.room_name, playerIndex });
            gameBroadcast(roomCode, "game_state", { code: roomCode, state });
          });
      });
    });
  }

  socket.on("game_accept_invite", (data: any) => {
    const un = socket.username;
    if (!un) return;
    const { inviteId, roomCode } = data;
    db.run(`UPDATE game_invites SET status = 'accepted' WHERE id = ?`, [inviteId], () => {});
    _gameJoinRoom(socket, roomCode, un);
  });

  socket.on("game_decline_invite", (data: any) => {
    const { inviteId } = data;
    db.run(`UPDATE game_invites SET status = 'declined' WHERE id = ?`, [inviteId], () => {});
  });

  socket.on("game_join", (data: any) => {
    const un = socket.username;
    if (!un) return;
    _gameJoinRoom(socket, data.roomCode, un);
  });

  socket.on("game_leave", (data: any) => {
    const un = socket.username;
    if (!un) return;
    const { roomCode } = data;
    db.run(`DELETE FROM game_members WHERE room_code = ? AND username = ?`, [roomCode, un], () => {
      gameBroadcast(roomCode, "game_member_left", { roomCode, username: un });
    });
  });

  socket.on("game_action", (data: any) => {
    const un = socket.username;
    if (!un) return;
    const { roomCode, action } = data;
    let state = _gameStates.get(roomCode);
    if (!state) return;
    const playerIndex = state.players.indexOf(un);
    if (playerIndex === -1) return;
    const now = new Date().toISOString();

    if (state.gameType === 'tictactoe') {
      const { cellIndex } = action;
      if (state.winner || state.finished || state.currentTurn !== playerIndex) return;
      if (state.board[cellIndex] !== '') return;
      state.board[cellIndex] = playerIndex === 0 ? 'X' : 'O';
      const w = tttCheckWinner(state.board);
      if (w) { state.winner = w === 'draw' ? 'draw' : un; state.finished = true; }
      else { state.currentTurn = 1 - state.currentTurn; }
    }

    else if (state.gameType === 'connect4') {
      const { col } = action;
      if (state.winner || state.finished || state.currentTurn !== playerIndex) return;
      let row = -1;
      for (let r = 5; r >= 0; r--) { if (state.board[r][col] === 0) { row = r; break; } }
      if (row === -1) return;
      state.board[row][col] = playerIndex + 1;
      state.lastDrop = [row, col];
      const w = c4CheckWinner(state.board);
      if (w) { state.winner = w === 'draw' ? 'draw' : un; state.finished = true; }
      else { state.currentTurn = 1 - state.currentTurn; }
    }

    else if (state.gameType === 'rps') {
      const { choice } = action;
      if (!['rock', 'paper', 'scissors'].includes(choice)) return;
      if (state.choices[playerIndex] !== null) return;
      state.choices[playerIndex] = choice;
      const allChose = state.choices[0] !== null && state.choices[1] !== null;
      if (!allChose) {
        // Broadcast masked state so other player can't see choice
        const masked = { ...state, choices: state.choices.map((c: any, i: number) => i === playerIndex ? '???' : c) };
        gameBroadcast(roomCode, "game_state", { code: roomCode, state: masked });
        _gameStates.set(roomCode, state);
        return;
      }
      state.phase = 'reveal';
      const [c0, c1] = state.choices;
      let roundResult: string;
      if (c0 === c1) { roundResult = 'draw'; }
      else if ((c0==='rock'&&c1==='scissors')||(c0==='scissors'&&c1==='paper')||(c0==='paper'&&c1==='rock')) { roundResult = state.players[0]; state.scores[0]++; }
      else { roundResult = state.players[1]; state.scores[1]++; }
      state.history.push({ round: state.currentRound, choices: [...state.choices], result: roundResult });
      if (state.currentRound >= state.rounds) {
        state.finished = true;
        if (state.scores[0] > state.scores[1]) state.winner = state.players[0];
        else if (state.scores[1] > state.scores[0]) state.winner = state.players[1];
        else state.winner = 'draw';
      }
      _gameStates.set(roomCode, state);
      db.run(`UPDATE game_rooms SET state = ?, last_active = ? WHERE code = ?`, [JSON.stringify(state), now, roomCode], () => {});
      gameBroadcast(roomCode, "game_state", { code: roomCode, state });
      if (!state.finished) {
        const t = setTimeout(() => {
          state.currentRound++;
          state.choices = [null, null];
          state.phase = 'choosing';
          _gameStates.set(roomCode, state);
          gameBroadcast(roomCode, "game_state", { code: roomCode, state });
        }, 2800);
        _gameTimers.set(roomCode + '_rps', t);
      }
      return;
    }

    else if (state.gameType === 'quiz') {
      const { answerIndex } = action;
      if (state.phase !== 'question') return;
      if (state.answers[un] !== undefined) return;
      state.answers[un] = answerIndex;
      const q = state.questions[state.currentQ];
      const elapsed = Date.now() - (state.questionStartAt || Date.now());
      const timeBonus = Math.max(0, Math.round((15000 - elapsed) / 1500));
      if (answerIndex === q.a) state.scores[un] = (state.scores[un] || 0) + 10 + timeBonus;
      const allAnswered = state.players.every((p: string) => state.answers[p] !== undefined);
      if (allAnswered) {
        const qt = _gameTimers.get(roomCode + '_quiz');
        if (qt) { clearTimeout(qt); _gameTimers.delete(roomCode + '_quiz'); }
        state.phase = 'reveal';
        _gameStates.set(roomCode, state);
        db.run(`UPDATE game_rooms SET state = ?, last_active = ? WHERE code = ?`, [JSON.stringify(state), now, roomCode], () => {});
        gameBroadcast(roomCode, "game_state", { code: roomCode, state });
        const nt = setTimeout(() => { _quizAdvance(roomCode, state, now); }, 2800);
        _gameTimers.set(roomCode + '_quiz_next', nt);
        return;
      }
    }

    else if (state.gameType === 'battleship') {
      if (state.phase === 'placement') {
        const { ships } = action;
        state.boards[playerIndex].ships = ships;
        state.boards[playerIndex].ready = true;
        if (state.boards[0].ready && state.boards[1].ready) state.phase = 'battle';
      } else if (state.phase === 'battle') {
        const { row, col } = action;
        if (state.winner || state.finished || state.currentTurn !== playerIndex) return;
        const ei = 1 - playerIndex;
        const eb = state.boards[ei];
        if (!eb.shots) eb.shots = [];
        if (eb.shots.some((s: any) => s[0] === row && s[1] === col)) return;
        let hit = false;
        for (const ship of eb.ships) {
          for (const cell of ship.cells) {
            if (cell[0] === row && cell[1] === col) {
              hit = true;
              if (!ship.hitCells) ship.hitCells = [];
              ship.hitCells.push([row, col]);
              break;
            }
          }
        }
        eb.shots.push([row, col, hit ? 'hit' : 'miss']);
        if (bsAllSunk(eb.ships)) { state.winner = un; state.finished = true; }
        else if (!hit) { state.currentTurn = 1 - state.currentTurn; }
      }
    }

    _gameStates.set(roomCode, state);
    db.run(`UPDATE game_rooms SET state = ?, last_active = ? WHERE code = ?`, [JSON.stringify(state), now, roomCode], () => {});
    gameBroadcast(roomCode, "game_state", { code: roomCode, state });
  });

  function _quizAdvance(roomCode: string, state: any, now: string) {
    state.currentQ++;
    if (state.currentQ >= state.questions.length) {
      state.phase = 'finished'; state.finished = true;
      let top = -1, winner = 'draw';
      for (const p of state.players) { if ((state.scores[p] || 0) > top) { top = state.scores[p] || 0; winner = p; } }
      state.winner = winner;
      _gameStates.set(roomCode, state);
      db.run(`UPDATE game_rooms SET state = ?, status = 'finished', last_active = ? WHERE code = ?`, [JSON.stringify(state), now, roomCode], () => {});
      gameBroadcast(roomCode, "game_state", { code: roomCode, state });
    } else {
      state.phase = 'question'; state.answers = {}; state.questionStartAt = Date.now();
      _gameStates.set(roomCode, state);
      db.run(`UPDATE game_rooms SET state = ?, last_active = ? WHERE code = ?`, [JSON.stringify(state), now, roomCode], () => {});
      gameBroadcast(roomCode, "game_state", { code: roomCode, state });
      const t = setTimeout(() => {
        if (_gameStates.get(roomCode)?.phase === 'question') {
          state.phase = 'reveal';
          _gameStates.set(roomCode, state);
          gameBroadcast(roomCode, "game_state", { code: roomCode, state });
          const t2 = setTimeout(() => _quizAdvance(roomCode, state, new Date().toISOString()), 2800);
          _gameTimers.set(roomCode + '_quiz_next', t2);
        }
      }, 15000);
      _gameTimers.set(roomCode + '_quiz', t);
    }
  }

  socket.on("game_start_quiz", (data: any) => {
    const un = socket.username;
    if (!un) return;
    const { roomCode } = data;
    let state = _gameStates.get(roomCode);
    if (!state || state.gameType !== 'quiz') return;
    if (state.players[0] !== un) { socket.emit("game_error", { msg: "Apenas o host pode iniciar" }); return; }
    if (state.players.length < 2) { socket.emit("game_error", { msg: "Precisa de ao menos 2 jogadores" }); return; }
    state.phase = 'question'; state.currentQ = 0; state.answers = {}; state.questionStartAt = Date.now();
    _gameStates.set(roomCode, state);
    gameBroadcast(roomCode, "game_state", { code: roomCode, state });
    const t = setTimeout(() => {
      if (_gameStates.get(roomCode)?.phase === 'question') {
        state.phase = 'reveal';
        _gameStates.set(roomCode, state);
        gameBroadcast(roomCode, "game_state", { code: roomCode, state });
        const t2 = setTimeout(() => _quizAdvance(roomCode, state, new Date().toISOString()), 2800);
        _gameTimers.set(roomCode + '_quiz_next', t2);
      }
    }, 15000);
    _gameTimers.set(roomCode + '_quiz', t);
  });

  socket.on("game_rematch", (data: any) => {
    const un = socket.username;
    if (!un) return;
    const { roomCode } = data;
    db.get(`SELECT * FROM game_rooms WHERE code = ?`, [roomCode], (_e: any, room: any) => {
      if (!room) return;
      const old = _gameStates.get(roomCode) || JSON.parse(room.state || '{}');
      const players = old.players || [];
      const gameType = old.gameType;
      let newState: any;
      if (gameType === 'tictactoe') newState = { gameType, players, board: Array(9).fill(''), currentTurn: 0, winner: null, finished: false };
      else if (gameType === 'connect4') newState = { gameType, players, board: Array(6).fill(null).map(() => Array(7).fill(0)), currentTurn: 0, winner: null, finished: false };
      else if (gameType === 'rps') newState = { gameType, players, scores: [0, 0], rounds: 5, currentRound: 1, choices: [null, null], phase: 'choosing', history: [], finished: false };
      else if (gameType === 'quiz') {
        const shuffled = [...QUIZ_QUESTIONS].sort(() => Math.random() - .5).slice(0, 10);
        const scores: any = {}; players.forEach((p: string) => { scores[p] = 0; });
        newState = { gameType, players, scores, questions: shuffled, currentQ: 0, phase: 'lobby', answers: {}, finished: false };
      } else {
        newState = { gameType, players, phase: 'placement', boards: [{ ships: [], shots: [], ready: false }, { ships: [], shots: [], ready: false }], currentTurn: 0, winner: null, finished: false };
      }
      const now = new Date().toISOString();
      _gameStates.set(roomCode, newState);
      db.run(`UPDATE game_rooms SET state = ?, status = 'playing', last_active = ? WHERE code = ?`, [JSON.stringify(newState), now, roomCode], () => {});
      gameBroadcast(roomCode, "game_state", { code: roomCode, state: newState });
    });
  });

  socket.on("disconnect", () => {
    backgroundedSockets.delete(socket.id);
    if (socket.username) {
      const un = socket.username;
      if (onlineUsers.has(un)) {
        onlineUsers.get(un)!.delete(socket.id);
        if (onlineUsers.get(un)!.size === 0) {
          onlineUsers.delete(un);
          const lastSeen = new Date().toISOString();
          const uIdx = _usersCache.findIndex((u: any) => u.username === un);
          const froze = uIdx !== -1 && _usersCache[uIdx].freeze_last_seen;
          const broadcastSeen = froze ? (_usersCache[uIdx]?.last_seen || lastSeen) : lastSeen;
          // Bug 6 fix: only update last_seen and broadcast when not frozen
          if (!froze) {
            if (uIdx !== -1) _usersCache[uIdx].last_seen = lastSeen;
            db.run(`UPDATE users SET last_seen = ? WHERE username = ?`, [lastSeen, un], () => {});
            broadcastUserStatus(un, false, broadcastSeen);
          }
          // If frozen, broadcast nothing (privacy preserved)
        }
      }
    }
  });
});

app.use(cors());
app.use(express.json({ limit: "200mb" }));
app.use(express.urlencoded({ limit: "200mb", extended: true }));
app.use(express.static(PUBLIC_DIR, {
  setHeaders(res, filePath) {
    if (filePath.endsWith('.webm')) res.setHeader('Content-Type', 'audio/webm');
    else if (filePath.endsWith('.ogg')) res.setHeader('Content-Type', 'audio/ogg');
  }
}));
// Explicit audio route so MIME is always correct regardless of static middleware order
// Lightweight health probe for storage — used by ops/monitoring, never by clients
// to decide whether to show media (that's the /uploads and /avatars routes' job).
const storageHealth = async (_req: Request, res: Response): Promise<void> => {
  try {
    const bucketId = process.env.DEFAULT_OBJECT_STORAGE_BUCKET_ID;
    const bucket = getGCSBucket();
    let bucketConnected = false;
    if (bucket) {
      // Replit's sidecar grants object access but not bucket metadata access.
      // Probing an object handle matches the permissions used by uploads and
      // avoids a false-negative health check from bucket.exists().
      await bucket.file("redspace-health-check.txt").exists();
      bucketConnected = true;
    }
    res.json({
      hasBucketIdEnv: !!bucketId,
      bucketConnected,
      pendingQueueLength: _gcsPendingQueue.length,
    });
  } catch (e: any) {
    res.status(500).json({ error: e.message });
  }
};
app.get('/api/storage/health', storageHealth);
app.get('/__storage_health', storageHealth);

app.get('/uploads/:filename', async (req, res) => {
  const filename = path.basename(req.params.filename);
  const filePath = path.join(UPLOADS_DIR, filename);
  const ext = path.extname(filename).toLowerCase();
  const mimeMap: Record<string, string> = {
    '.webm': 'audio/webm', '.ogg': 'audio/ogg', '.mp3': 'audio/mpeg',
    '.wav': 'audio/wav', '.mp4': 'video/mp4', '.jpg': 'image/jpeg',
    '.jpeg': 'image/jpeg', '.png': 'image/png', '.gif': 'image/gif', '.webp': 'image/webp'
  };
  const ct = mimeMap[ext] || 'application/octet-stream';
  // Serve from local disk if available
  if (fs.existsSync(filePath)) {
    res.setHeader('Content-Type', ct);
    res.setHeader('Accept-Ranges', 'bytes');
    res.setHeader('Cache-Control', 'public, max-age=31536000');
    return void res.sendFile(filePath);
  }
  // Fallback: stream from GCS (file persisted there even after server restart)
  await gcsServeFile(req, res);
});

app.get('/avatars/:filename', async (req, res) => {
  const filename = path.basename(req.params.filename);
  const filePath = path.join(AVATAR_DIR, filename);
  if (fs.existsSync(filePath)) {
    res.setHeader('Cache-Control', 'public, max-age=31536000');
    return void res.sendFile(filePath);
  }
  await gcsServeFile(req, res);
});

// Only letters, numbers, underscore and dot. Blocks quotes, backslashes, spaces,
// SQL/HTML-special chars (' " ` ; < > & % etc.) and anything else that could
// interfere with queries, markup, URLs or file paths built from the username.
const USERNAME_REGEX = /^[a-zA-Z0-9_.]{3,30}$/;

app.post("/register", async (req, res) => {
  const { username, password, display_name } = req.body;
  if (!username || !password) return void res.status(400).json({ error: "Campos obrigatórios" });
  const un = username.toLowerCase().trim();
  if (!USERNAME_REGEX.test(un)) {
    return void res.status(400).json({ error: "Usuário inválido: use apenas letras, números, _ e . (3 a 30 caracteres, sem espaços ou símbolos como ' \" ` ; < >)" });
  }
  // Verifica duplicata no cache E no banco (cobre cache desatualizado)
  if (_usersCache.find((u: any) => u.username === un)) return void res.status(400).json({ error: "Usuário já existe" });
  const dbCheck = await pool.query(`SELECT 1 FROM users WHERE username = $1 LIMIT 1`, [un]).catch(() => ({ rows: [] }));
  if (dbCheck.rows.length > 0) return void res.status(400).json({ error: "Usuário já existe" });
  const hash = await bcrypt.hash(password, 10);
  const newUser = {
    username: un, display_name: display_name || un, password: hash,
    bio: "", avatar: "", bg_image: "",
    last_seen: null, freeze_last_seen: false,
    is_ai: false, is_verified: false,
    created_at: new Date().toISOString(),
  };
  try {
    await _dbUpsertUser(newUser);
  } catch (error: any) {
    console.error("[register] PostgreSQL write failed:", error.message);
    return void res.status(503).json({ error: "Banco de dados indisponível. Tente novamente." });
  }
  _usersCache.push(newUser);
  // Auto-friend new user with Kuromi
  const now = new Date().toISOString();
  db.run(
    `INSERT OR IGNORE INTO friendships (requester, recipient, status, created_at, updated_at) VALUES ('kuromi', ?, 'accepted', ?, ?)`,
    [un, now, now],
    () => {}
  );
  res.json({ ok: true });
});

app.post("/login", async (req, res) => {
  const { username, password } = req.body;
  if (!username || !password) return void res.status(400).json({ error: "Campos obrigatórios" });
  const un = username.toLowerCase().trim();
  // Tenta cache primeiro; se não encontrar, consulta o banco
  let user = getUsers().find((u: any) => u.username === un);
  if (!user) {
    const dbResult = await pool.query(`SELECT * FROM users WHERE username = $1 LIMIT 1`, [un]).catch(() => ({ rows: [] }));
    if (dbResult.rows[0]) {
      const dbUser = dbResult.rows[0];
      user = { ...dbUser, freeze_last_seen: !!dbUser.freeze_last_seen, is_ai: !!dbUser.is_ai, is_verified: !!dbUser.is_verified, is_vip: !!dbUser.is_vip, vip_expires_at: dbUser.vip_expires_at || null, is_online: false };
      if (!_usersCache.find((u: any) => u.username === un)) _usersCache.push(user);
    }
  }
  if (user && (await bcrypt.compare(password, user.password))) {
    const ban = await getBanStatus(user.username);
    if (ban) return void res.status(403).json({ error: "banned", ban });
    const { password: _, ...safe } = user;
    res.json(safe);
  } else res.status(401).json({ error: "Credenciais inválidas" });
});

app.get("/chats/:me", (req, res) => {
  const me = req.params.me.toLowerCase().trim();
  db.all(
    `SELECT m.*, CASE WHEN s = ? THEN r ELSE s END as contact, cs.is_archived, cs.is_pinned
     FROM messages m LEFT JOIN chat_settings cs ON cs.username = ? AND cs.contact_id = CASE WHEN s = ? THEN r ELSE s END
     WHERE m.s = ? OR m.r = ? ORDER BY m.time DESC`,
    [me, me, me, me, me],
    (_err: any, rows: any[]) => {
      if (_err) return void res.status(500).json([]);
      const chats = new Map(), users = getUsers();
      (rows || []).forEach((r: any) => {
        if (!chats.has(r.contact)) {
          const u = users.find((u: any) => u.username === r.contact);
          let lastMsg = r.c;
          if (r.type === "image") lastMsg = "📷 Foto";
          else if (r.type === "video") lastMsg = "🎥 Vídeo";
          else if (r.type === "audio") lastMsg = "🎤 Áudio";
          else if (r.type === "sticker") lastMsg = "🎭 Figurinha";
          else if (r.type === "story_reply") lastMsg = "💬 Respondeu ao status";
          chats.set(r.contact, {
            contact: r.contact, display_name: u?.display_name || r.contact,
            avatar: u?.avatar || "", is_online: isUserOnline(r.contact),
            last_seen: u?.last_seen || null, last_msg: lastMsg, last_time: r.time, type: r.type,
            is_archived: r.is_archived || 0, is_pinned: r.is_pinned || 0, unread: 0,
            last_sender: r.s, last_status: r.status ?? 0,
            is_ai: u?.is_ai || false, is_verified: u?.is_verified || false,
          });
        }
        if (r.r === me && r.status < 2) chats.get(r.contact).unread++;
      });
      res.json(Array.from(chats.values()));
    }
  );
});

app.post("/chats/settings", (req, res) => {
  // Prefer x-username header (authenticated) over body username
  const headerUser = (req.headers["x-username"] || "").toString().toLowerCase().trim();
  const { username, contact_id, is_archived, is_pinned } = req.body;
  const un = (headerUser || (username || "").toString().toLowerCase().trim());
  if (!un || !contact_id) return void res.status(400).json({ error: "Campos obrigatórios" });
  const ci = String(contact_id).toLowerCase().trim();
  // Step 1: ensure row exists
  db.run(
    `INSERT INTO chat_settings (username, contact_id, is_archived, is_pinned) VALUES (?, ?, 0, 0) ON CONFLICT(username, contact_id) DO NOTHING`,
    [un, ci],
    function (this: any, err: any) {
      if (err) return void res.status(500).json({ error: err.message });
      // Step 2: update only the field(s) provided
      const updates: string[] = [];
      const params: any[] = [];
      if (is_archived !== undefined) { updates.push("is_archived = ?"); params.push(Number(is_archived)); }
      if (is_pinned !== undefined) { updates.push("is_pinned = ?"); params.push(Number(is_pinned)); }
      if (!updates.length) return void res.json({ ok: true });
      params.push(un, ci);
      db.run(
        `UPDATE chat_settings SET ${updates.join(", ")} WHERE username = ? AND contact_id = ?`,
        params,
        function (this: any, err2: any) {
          if (err2) return void res.status(500).json({ error: err2.message });
          res.json({ ok: true });
        }
      );
    }
  );
});

app.post("/chats/delete", (req, res) => {
  // Prefer x-username header (authenticated) over body username
  const headerUser = (req.headers["x-username"] || "").toString().toLowerCase().trim();
  const { username, contact_id, type } = req.body;
  const u = (headerUser || String(username || "").toLowerCase().trim());
  if (!u || contact_id == null || !type) return void res.status(400).json({ error: "Campos obrigatórios" });
  const c = String(contact_id).toLowerCase().trim();
  if (type === "group") {
    // Remove the user from the group (leave) so it disappears from their chat list
    db.run(`DELETE FROM group_members WHERE group_id = ? AND username = ?`, [c, u], function (this: any, err: any) {
      if (err) return void res.status(500).json({ error: err.message });
      // Also clean up chat_settings for this group
      db.run(`DELETE FROM chat_settings WHERE username = ? AND contact_id = ?`, [u, c], () => {});
      res.json({ ok: true });
    });
  } else {
    // For DMs: delete all messages between the two users
    db.run(`DELETE FROM messages WHERE (s = ? AND r = ?) OR (s = ? AND r = ?)`, [u, c, c, u],
      function (this: any, err: any) {
        if (err) return void res.status(500).json({ error: err.message });
        // Also clean up chat_settings for this DM
        db.run(`DELETE FROM chat_settings WHERE (username = ? AND contact_id = ?) OR (username = ? AND contact_id = ?)`, [u, c, c, u], () => {});
        res.json({ ok: true });
      });
  }
});

app.post("/messages", async (req, res) => {
  const { s, r, c, type, caption, reply_context, view_once } = req.body;
  if (!s || !r || !c) return void res.status(400).json({ error: "Campos obrigatórios" });
  const sender = s.toLowerCase().trim(), recipient = r.toLowerCase().trim();
  const blockResult = await pool.query(
    `SELECT 1 FROM user_blocks
     WHERE (blocker=$1 AND blocked=$2) OR (blocker=$2 AND blocked=$1)
     LIMIT 1`,
    [sender, recipient]
  ).catch(() => ({ rows: [] }));
  if (blockResult.rows.length > 0) {
    return void res.status(403).json({ error: "Não é possível enviar mensagens enquanto houver um bloqueio entre os usuários." });
  }
  const timestamp = new Date().toISOString();
  const isRecipientOnline = onlineUsers.has(recipient) && onlineUsers.get(recipient)!.size > 0;
  const status = isRecipientOnline ? 1 : 0;
  let content = c;
  if (type !== "text" && type !== "story_reply" && content && content.startsWith("data:")) {
    content = await saveBase64File(content, "uploads", sender);
    if (!content) return void res.status(500).json({ error: "Falha ao salvar mídia. A mensagem NÃO foi enviada — tente novamente." });
    if (type === "audio") content = await convertAudioToMp3(content);
  }
  const replyCtx = reply_context ? (typeof reply_context === "string" ? reply_context : JSON.stringify(reply_context)) : null;
  const vo = view_once ? 1 : 0;
  db.run("INSERT INTO messages (s, r, c, type, status, time, caption, reply_context, view_once) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
    [sender, recipient, content, type || "text", status, timestamp, caption || "", replyCtx, vo],
    function (this: any, err: any) {
      if (err) return void res.status(500).json({ error: err.message });
      res.json({ ok: true, id: this.lastID, time: timestamp, c: content });
    });
});

app.post("/messages/:id/viewed", (req: any, res: any) => {
  const { id } = req.params;
  const { username } = req.body;
  if (!username) return void res.status(400).json({ error: "username obrigatório" });
  db.get("SELECT * FROM messages WHERE id = ?", [id], (_err: any, row: any) => {
    if (!row) return void res.status(404).json({ error: "Mensagem não encontrada" });
    if (row.r !== username) return void res.status(403).json({ error: "Sem permissão" });

    // VIP receivers can open view-once media up to 3 times
    const viewer = _usersCache.find((u: any) => u.username === username);
    const isVip = !!(viewer?.is_vip);
    const maxViews = isVip ? 3 : 1;
    const currentViewed = row.viewed || 0;

    if (currentViewed >= maxViews) return void res.json({ ok: true, views_left: 0 });

    const newCount = currentViewed + 1;
    const viewsLeft = maxViews - newCount;
    const shouldClear = viewsLeft === 0;

    const sql = shouldClear
      ? `UPDATE messages SET viewed = ${newCount}, c = '' WHERE id = ?`
      : `UPDATE messages SET viewed = ${newCount} WHERE id = ?`;

    db.run(sql, [id], function (this: any, err: any) {
      if (err) return void res.status(500).json({ error: err.message });
      if (onlineUsers.has(row.s)) onlineUsers.get(row.s)!.forEach((sid: string) => io.to(sid).emit("msg_viewed", { id: parseInt(id) }));
      res.json({ ok: true, views_left: viewsLeft });
    });
  });
});

app.get("/messages/:u1/:u2", (req, res) => {
  db.all("SELECT * FROM messages WHERE (s=? AND r=?) OR (s=? AND r=?) ORDER BY time ASC",
    [req.params.u1.toLowerCase(), req.params.u2.toLowerCase(), req.params.u2.toLowerCase(), req.params.u1.toLowerCase()],
    (_e: any, r: any) => res.json(r || []));
});

app.get("/user/:u", async (req, res) => {
  const viewer = (req.query.viewer as string || "").toLowerCase().trim();
  const target = req.params.u.toLowerCase().trim();
  // Usa helper com fallback ao banco para cobrir cache desatualizado
  const user = await _getUserByUsername(target);
  if (!user) return void res.status(404).json({ error: "Não encontrado" });

  // Check if target has blocked viewer
  let isBlocked = false;
  if (viewer && viewer !== target) {
    const blockResult = await pool.query(
      `SELECT 1 FROM user_blocks WHERE (blocker=$1 AND blocked=$2) OR (blocker=$2 AND blocked=$1)`,
      [target, viewer]
    ).catch(() => ({ rows: [] }));
    isBlocked = blockResult.rows.length > 0;
  }

  if (isBlocked) {
    return void res.json({
      username: target, display_name: "Usuário da REDSPACE",
      bio: "", avatar: "", is_online: false, last_seen: null,
      is_verified: false, is_ai: false, freeze_last_seen: false, is_blocked: true,
    });
  }

  const { password, ...safe } = user;
  if (safe.is_ai) {
    safe.is_online = true;
  } else if (safe.freeze_last_seen) {
    safe.is_online = false;
    safe.last_seen = null;
  } else {
    safe.is_online = onlineUsers.has(safe.username) && onlineUsers.get(safe.username)!.size > 0;
  }
  res.json(safe);
});

app.get("/users/search", async (req, res) => {
  const { q } = req.query as { q?: string };
  if (!q) return void res.json([]);
  const query = `%${q.toLowerCase().trim()}%`;
  try {
    // Consulta diretamente o banco para garantir que todos os usuários apareçam
    const result = await pool.query(
      `SELECT * FROM users WHERE (LOWER(username) LIKE $1 OR LOWER(display_name) LIKE $1) AND username NOT IN ('crisalida') ORDER BY username LIMIT 50`,
      [query]
    );
    res.json(result.rows.map((r: any) => {
      const { password, ...safe } = r;
      safe.freeze_last_seen = !!r.freeze_last_seen;
      safe.is_ai = !!r.is_ai;
      safe.is_verified = !!r.is_verified;
      safe.is_vip = !!r.is_vip;
      safe.is_online = onlineUsers.has(r.username) && (onlineUsers.get(r.username)?.size || 0) > 0;
      return safe;
    }));
  } catch {
    // Fallback para cache se o banco falhar
    const users = getUsers().filter((u: any) => u.username.includes(q.toLowerCase().trim()) || (u.display_name || "").toLowerCase().includes(q.toLowerCase().trim()));
    res.json(users.map(({ password, ...safe }: any) => safe));
  }
});

app.post("/update-profile", async (req, res) => {
  const { username, bio, avatar, bg_image, display_name, freeze_last_seen } = req.body;
  const un = username.toLowerCase().trim();
  // Garante que o usuário esteja no cache (carrega do banco se necessário)
  if (!_usersCache.find((u: any) => u.username === un)) await _getUserByUsername(un);
  const idx = _usersCache.findIndex((u: any) => u.username === un);
  if (idx === -1) return void res.status(404).send();
  if (bio !== undefined) _usersCache[idx].bio = bio;
  if (display_name !== undefined) _usersCache[idx].display_name = display_name;
  if (avatar && avatar.startsWith("data:")) {
    const saved = await saveBase64File(avatar, "avatars", "profile_" + un);
    if (saved) _usersCache[idx].avatar = saved;
  }
  if (bg_image && bg_image.startsWith("data:")) {
    const savedBg = await saveBase64File(bg_image, "uploads", "bgimage_" + un);
    if (savedBg) _usersCache[idx].bg_image = savedBg;
  }
  if (freeze_last_seen !== undefined) _usersCache[idx].freeze_last_seen = !!freeze_last_seen;
  try {
    await _dbUpsertUser(_usersCache[idx]);
  } catch (error: any) {
    console.error("[update-profile] PostgreSQL write failed:", error.message);
    return void res.status(503).json({ error: "Banco de dados indisponível. Tente novamente." });
  }
  const { password, ...safe } = _usersCache[idx];
  res.json({ ok: true, user: safe });
});

app.post("/update-password", async (req, res) => {
  const { username, password } = req.body;
  if (!username || !password) return void res.status(400).json({ error: "Campos obrigatórios" });
  const un = username.toLowerCase().trim();
  if (!_usersCache.find((u: any) => u.username === un)) await _getUserByUsername(un);
  const idx = _usersCache.findIndex((u: any) => u.username === un);
  if (idx === -1) return void res.status(404).json({ error: "Usuário não encontrado" });
  const hash = await bcrypt.hash(password, 10);
  _usersCache[idx].password = hash;
  db.run(`UPDATE users SET password = ? WHERE username = ?`, [hash, un], () => {});
  res.json({ ok: true });
});

app.post("/delete-account", async (req, res) => {
  const { username, password } = req.body;
  if (!username || !password) return void res.status(400).json({ error: "Campos obrigatórios" });
  const un = username.toLowerCase().trim();
  const user = await _getUserByUsername(un);
  if (!user) return void res.status(404).json({ error: "Usuário não encontrado" });
  const valid = await bcrypt.compare(password, user.password);
  if (!valid) return void res.status(401).json({ error: "Senha incorreta" });
  // Remove from cache
  const idx = _usersCache.findIndex((u: any) => u.username === un);
  if (idx !== -1) _usersCache.splice(idx, 1);
  // Disconnect active sockets
  if (onlineUsers.has(un)) {
    onlineUsers.get(un)!.forEach((sid) => { const s = io.sockets.sockets.get(sid); if (s) s.disconnect(true); });
    onlineUsers.delete(un);
  }
  // Delete all data
  db.run(`DELETE FROM users WHERE username = ?`, [un], () => {});
  db.run(`DELETE FROM messages WHERE s = ? OR r = ?`, [un, un], () => {});
  db.run(`DELETE FROM friendships WHERE requester = ? OR recipient = ?`, [un, un], () => {});
  db.run(`DELETE FROM group_members WHERE username = ?`, [un], () => {});
  db.run(`DELETE FROM stories WHERE username = ?`, [un], () => {});
  db.run(`DELETE FROM chat_settings WHERE username = ?`, [un], () => {});
  res.json({ ok: true });
});

app.delete("/stories/:id", (req, res) => {
  const { username } = req.body;
  const id = parseInt(req.params.id);
  db.get("SELECT * FROM stories WHERE id = ?", [id], (_err: any, row: any) => {
    if (!row) return void res.status(404).json({ error: "Não encontrado" });
    if (row.username !== username?.toLowerCase().trim()) return void res.status(403).json({ error: "Sem permissão" });
    db.run("DELETE FROM stories WHERE id = ?", [id], function (this: any, err: any) {
      if (err) return void res.status(500).json({ error: err.message });
      res.json({ ok: true });
    });
  });
});

app.get("/stories", (req, res) => {
  const { viewer } = req.query as { viewer?: string };
  const yesterday = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString();
  db.all("SELECT * FROM stories WHERE time > ? ORDER BY time DESC", [yesterday], (_err: any, rows: any[]) => {
    if (_err) return void res.status(500).json([]);
    const users = getUsers();
    const mapStory = (s: any) => {
      const u = users.find((u: any) => u.username === s.username);
      let viewers: any[] = [], likes: any[] = [];
      try { viewers = JSON.parse(s.viewers || "[]"); } catch (e) {}
      try { likes = JSON.parse(s.likes || "[]"); } catch (e) {}
      return { ...s, display_name: u?.display_name || s.username, avatar: u?.avatar || "", viewers, likes };
    };
    const processAndGroup = (stories: any[]) => {
      const grouped: any = {};
      stories.forEach((s) => {
        if (!grouped[s.username]) grouped[s.username] = { username: s.username, display_name: s.display_name, avatar: s.avatar, stories: [] };
        grouped[s.username].stories.push(s);
      });
      return Object.values(grouped);
    };
    if (viewer) {
      const vl = viewer.toLowerCase().trim();
      db.all(`SELECT * FROM friendships WHERE (requester = ? OR recipient = ?) AND status = 'accepted'`, [vl, vl], (_err2: any, friendships: any[]) => {
        const friendUsernames = (friendships || []).map((f: any) => f.requester === vl ? f.recipient : f.requester);
        friendUsernames.push(vl);
        res.json(processAndGroup((rows || []).filter((s: any) => friendUsernames.includes(s.username)).map(mapStory)));
      });
    } else {
      res.json(processAndGroup((rows || []).map(mapStory)));
    }
  });
});

app.post("/stories", async (req, res) => {
  const { username, content, type, caption, bg_color } = req.body;
  if (!username) return void res.status(400).json({ error: "Username obrigatório" });
  let finalContent = content;
  if (type !== "text" && content && content.startsWith("data:")) {
    if (type === "video") {
      const b64 = content.split(";base64,")[1] || "";
      const sizeBytes = (b64.length * 3) / 4;
      if (sizeBytes > 100 * 1024 * 1024) return void res.status(413).json({ error: "Vídeo muito grande. Máximo 100MB." });
    }
    finalContent = await saveBase64File(content, "uploads", "story_" + username);
    if (!finalContent) return void res.status(500).json({ error: "Erro ao salvar arquivo" });
  }
  const time = new Date().toISOString();
  db.run("INSERT INTO stories (username, content, type, caption, bg_color, time) VALUES (?, ?, ?, ?, ?, ?)",
    [username, finalContent || "", type || "text", caption || "", bg_color || "#FF3B30", time],
    function (this: any, err: any) {
      if (err) return void res.status(500).json({ error: err.message });
      io.emit("new_story", { id: this.lastID, username, content: finalContent, type: type || "text", caption: caption || "", bg_color: bg_color || "#FF3B30", time });
      res.json({ ok: true });
    });
});

app.post("/stories/:id/view", (req, res) => {
  const { id } = req.params, { username } = req.body;
  db.get("SELECT viewers, username as story_owner FROM stories WHERE id = ?", [id], (_err: any, row: any) => {
    if (_err || !row) return void res.status(404).json({ error: "Story não encontrado" });
    if (row.story_owner === username) return void res.json({ ok: true });
    let viewers: any[] = []; try { viewers = JSON.parse(row.viewers || "[]"); } catch (e) {}
    if (!viewers.includes(username)) {
      viewers.push(username);
      db.run("UPDATE stories SET viewers = ? WHERE id = ?", [JSON.stringify(viewers), id], (err: any) => {
        if (err) return void res.status(500).json({ error: err.message });
        res.json({ ok: true });
      });
    } else res.json({ ok: true });
  });
});

app.post("/stories/:id/like", (req, res) => {
  const { id } = req.params, { username } = req.body;
  db.get("SELECT likes, username as story_owner FROM stories WHERE id = ?", [id], (_err: any, row: any) => {
    if (_err || !row) return void res.status(404).json({ error: "Story não encontrado" });
    let likes: any[] = []; try { likes = JSON.parse(row.likes || "[]"); } catch (e) {}
    const index = likes.indexOf(username);
    if (index === -1) likes.push(username); else likes.splice(index, 1);
    db.run("UPDATE stories SET likes = ? WHERE id = ?", [JSON.stringify(likes), id], (err: any) => {
      if (err) return void res.status(500).json({ error: err.message });
      if (onlineUsers.has(row.story_owner)) onlineUsers.get(row.story_owner)!.forEach((sid) => io.to(sid).emit("story_liked", { story_id: id, username, liked: index === -1 }));
      res.json({ ok: true, liked: index === -1, likes });
    });
  });
});

app.post("/stories/:id/reply", (req, res) => {
  const { id } = req.params, { username, message } = req.body;
  if (!message) return void res.status(400).json({ error: "Mensagem obrigatória" });
  const time = new Date().toISOString();
  db.run("INSERT INTO story_replies (story_id, username, message, time) VALUES (?, ?, ?, ?)", [id, username, message, time], function (this: any, err: any) {
    if (err) return void res.status(500).json({ error: err.message });
    const replyInsertId = this.lastID;
    db.get("SELECT username as story_owner, type, content, bg_color FROM stories WHERE id = ?", [id], (_err: any, story: any) => {
      if (story && story.story_owner && story.story_owner !== username) {
        const owner = story.story_owner.toLowerCase();
        const isOwnerOnline = isUserOnline(owner);
        const status = isOwnerOnline ? 1 : 0;
        const previewContent = story.type === "text" ? (story.content || "").substring(0, 80) : (story.content || "");
        const replyCtx = JSON.stringify({ story_type: story.type, story_content: previewContent, story_bg: story.bg_color || "#e53935" });
        db.run(
          "INSERT INTO messages (s, r, c, type, status, time, caption, reply_context) VALUES (?, ?, ?, 'story_reply', ?, ?, '', ?)",
          [username, owner, message, status, time, replyCtx],
          function (this: any, err2: any) {
            if (!err2) {
              db.get("SELECT * FROM messages WHERE id = ?", [this.lastID], (_e: any, row: any) => {
                if (row) {
                  if (onlineUsers.has(owner)) onlineUsers.get(owner)!.forEach((sid) => {
                    io.to(sid).emit("new_msg", row);
                    io.to(sid).emit("story_reply", { story_id: id, from: username, message, time });
                  });
                  if (onlineUsers.has(username)) onlineUsers.get(username)!.forEach((sid) => io.to(sid).emit("new_msg", row));
                }
              });
            } else {
              if (onlineUsers.has(owner)) onlineUsers.get(owner)!.forEach((sid) => io.to(sid).emit("story_reply", { story_id: id, from: username, message, time }));
            }
          }
        );
      } else if (story && story.story_owner) {
        if (onlineUsers.has(story.story_owner)) onlineUsers.get(story.story_owner)!.forEach((sid) => io.to(sid).emit("story_reply", { story_id: id, from: username, message, time }));
      }
    });
    res.json({ ok: true, id: replyInsertId });
  });
});

app.get("/stories/:id/viewers", (req, res) => {
  db.get("SELECT viewers FROM stories WHERE id = ?", [req.params.id], (_err: any, row: any) => {
    if (_err || !row) return void res.status(404).json({ error: "Story não encontrado" });
    let viewers: any[] = []; try { viewers = JSON.parse(row.viewers || "[]"); } catch (e) {}
    const users = getUsers();
    db.all("SELECT username, message FROM story_replies WHERE story_id = ?", [req.params.id], (_e2: any, replies: any[]) => {
      const replyMap: Record<string, string> = {};
      if (replies) replies.forEach((r: any) => { replyMap[r.username] = r.message; });
      res.json(viewers.map((v: any) => {
        const u = users.find((u: any) => u.username === v);
        return { username: v, display_name: u?.display_name || v, avatar: u?.avatar || "", reply: replyMap[v] || "" };
      }));
    });
  });
});

app.post("/groups/create", async (req, res) => {
  const { name, description, created_by, members, avatar } = req.body;
  if (!name || !created_by) return void res.status(400).json({ error: "Nome e criador obrigatórios" });
  const groupId = "g_" + Date.now() + "_" + Math.random().toString(36).substr(2, 9);
  const createdAt = new Date().toISOString();
  const savedAvatar = (avatar && avatar.startsWith("data:")) ? await saveBase64File(avatar, "avatars", "group_" + groupId) : "";
  db.run(`INSERT INTO groups (group_id, name, description, avatar, created_by, created_at) VALUES (?, ?, ?, ?, ?, ?)`,
    [groupId, name, description || "", savedAvatar, created_by, createdAt], function (this: any, err: any) {
      if (err) return void res.status(500).json({ error: err.message });
      db.run(`INSERT INTO group_members (group_id, username, role, joined_at) VALUES (?, ?, 'admin', ?)`, [groupId, created_by, createdAt]);
      // Auto-add Kuromi to every new group
      db.run(`INSERT OR IGNORE INTO group_members (group_id, username, role, joined_at) VALUES (?, 'kuromi', 'member', ?)`, [groupId, createdAt]);
      if (members && Array.isArray(members)) {
        members.forEach((u: any) => {
          if (u !== created_by && u !== "kuromi")
            db.run(`INSERT INTO group_members (group_id, username, role, joined_at) VALUES (?, ?, 'member', ?)`, [groupId, u, createdAt]);
        });
      }
      io.emit("new_group", { group_id: groupId, name, created_by });
      // Kuromi sends a welcome message
      setTimeout(() => {
        insertGroupMsg({
          group_id: groupId, sender: "kuromi",
          content: `Oi, pessoal! Sou a Kuromi IA e estou aqui pra ajudar 😊\n\nComandos que você pode usar:\n/play [música] — toca uma música\n/instagram [link do Reel] — baixa um Reel público do Instagram\n/ban — bane o autor da mensagem (responda ela)\n/promover — promove alguém a admin (responda a mensagem)\n/rebaixar — rebaixa um admin (responda a mensagem)\n/fechargp — fecha o grupo pra só admins enviarem\n/abrirgp — abre o grupo de volta pra todos\n/fotogp — define a foto do grupo (responda uma imagem)\n\nPara ações de moderação, sempre responda a mensagem da pessoa e use o comando. Qualquer dúvida é só me chamar!`,
          type: "text",
        });
      }, 1200);
      res.json({ ok: true, group_id: groupId });
    });
});

app.get("/groups/my/:username", (req, res) => {
  const username = req.params.username.toLowerCase().trim();
  const mentionPattern = `%@${username}%`;
  db.all(
    `SELECT g.*, gm.role, gm.muted,
      (SELECT content FROM group_messages gm0 WHERE gm0.group_id = g.group_id AND gm0.sender != '_system_' ORDER BY gm0.time DESC LIMIT 1) as last_msg,
      (SELECT type FROM group_messages gm0 WHERE gm0.group_id = g.group_id AND gm0.sender != '_system_' ORDER BY gm0.time DESC LIMIT 1) as last_msg_type,
      (SELECT time FROM group_messages gm0 WHERE gm0.group_id = g.group_id ORDER BY gm0.time DESC LIMIT 1) as last_msg_time,
      (SELECT sender FROM group_messages gm0 WHERE gm0.group_id = g.group_id AND gm0.sender != '_system_' ORDER BY gm0.time DESC LIMIT 1) as last_sender,
      (SELECT COUNT(*) FROM group_messages gmu WHERE gmu.group_id = g.group_id AND gmu.sender != ? AND gmu.sender != '_system_' AND (gm.last_read_at IS NULL OR gmu.time > gm.last_read_at)) as unread,
      CASE WHEN EXISTS(SELECT 1 FROM group_messages gmn WHERE gmn.group_id = g.group_id AND gmn.sender != ? AND gmn.sender != '_system_' AND (gm.last_read_at IS NULL OR gmn.time > gm.last_read_at) AND gmn.content ILIKE ?) THEN 1 ELSE 0 END as has_mention
     FROM groups g INNER JOIN group_members gm ON g.group_id = gm.group_id
     WHERE gm.username = ?
     ORDER BY COALESCE((SELECT time FROM group_messages gmo WHERE gmo.group_id = g.group_id ORDER BY gmo.time DESC LIMIT 1), g.created_at) DESC`,
    [username, username, mentionPattern, username], (_err: any, rows: any) => {
      if (_err) return void res.status(500).json([]);
      res.json(rows || []);
    });
});

app.get("/groups/:group_id", (req, res) => {
  db.get(`SELECT * FROM groups WHERE group_id = ?`, [req.params.group_id], (_err: any, group: any) => {
    if (_err || !group) return void res.status(404).json({ error: "Grupo não encontrado" });
    db.all(`SELECT username, role, joined_at, muted FROM group_members WHERE group_id = ?`, [req.params.group_id], (_err2: any, members: any[]) => {
      if (_err2) return void res.status(500).json({ error: _err2.message });
      const users = getUsers();
      const membersWithData = (members || []).map((m: any) => {
        const u = users.find((u: any) => u.username === m.username);
        return { ...m, display_name: u?.display_name || m.username, avatar: u?.avatar || "", is_online: isUserOnline(m.username), is_ai: u?.is_ai || false, is_verified: u?.is_verified || false };
      });
      res.json({ ...group, members: membersWithData });
    });
  });
});

function insertGroupSystemMsg(group_id: string, text: string): void {
  const time = new Date().toISOString();
  db.run(
    `INSERT INTO group_messages (group_id, sender, content, type, time) VALUES (?, '_system_', ?, 'system', ?)`,
    [group_id, text, time],
    function (this: any, err: any) {
      if (err) return;
      const msgId = this.lastID;
      db.all(`SELECT username FROM group_members WHERE group_id = ?`, [group_id], (_e: any, members: any[]) => {
        if (!members) return;
        const msgPayload = { id: msgId, group_id, sender: "_system_", content: text, type: "system", time };
        members.forEach((m: any) => {
          if (onlineUsers.has(m.username))
            onlineUsers.get(m.username)!.forEach((sid: string) => io.to(sid).emit("new_group_msg", msgPayload));
        });
      });
    }
  );
}

app.post("/groups/:group_id/members/add", (req, res) => {
  const { group_id } = req.params, { username, added_by } = req.body;
  db.get(`SELECT role FROM group_members WHERE group_id = ? AND username = ?`, [group_id, added_by], (_err: any, member: any) => {
    if (_err || !member || member.role !== "admin") return void res.status(403).json({ error: "Apenas admins podem adicionar membros" });
    const joinedAt = new Date().toISOString();
    db.run(`INSERT INTO group_members (group_id, username, role, joined_at) VALUES (?, ?, 'member', ?)`, [group_id, username, joinedAt], function (this: any, err: any) {
      if (err) return void res.status(500).json({ error: err.message });
      insertGroupSystemMsg(group_id, `🎉 ${added_by} adicionou ${username} ao grupo`);
      io.emit("group_member_added", { group_id, username }); res.json({ ok: true });
    });
  });
});

app.post("/groups/:group_id/members/remove", (req, res) => {
  const { group_id } = req.params, { username, removed_by } = req.body;
  db.get(`SELECT role FROM group_members WHERE group_id = ? AND username = ?`, [group_id, removed_by], (_err: any, member: any) => {
    if (_err || !member || member.role !== "admin") return void res.status(403).json({ error: "Apenas admins podem remover membros" });
    insertGroupSystemMsg(group_id, `💔 ${removed_by} removeu ${username} do grupo`);
    db.run(`DELETE FROM group_members WHERE group_id = ? AND username = ?`, [group_id, username], function (this: any, err: any) {
      if (err) return void res.status(500).json({ error: err.message });
      io.emit("group_member_removed", { group_id, username }); res.json({ ok: true });
    });
  });
});

app.post("/groups/:group_id/leave", (req, res) => {
  const { group_id } = req.params, { username } = req.body;
  insertGroupSystemMsg(group_id, `👋 ${username} saiu do grupo`);
  db.run(`DELETE FROM group_members WHERE group_id = ? AND username = ?`, [group_id, username], function (this: any, err: any) {
    if (err) return void res.status(500).json({ error: err.message });
    io.emit("group_member_left", { group_id, username }); res.json({ ok: true });
  });
});

app.post("/groups/:group_id/promote", (req, res) => {
  const { group_id } = req.params, { username, promoted_by } = req.body;
  db.get(`SELECT role FROM group_members WHERE group_id = ? AND username = ?`, [group_id, promoted_by], (_err: any, member: any) => {
    if (_err || !member || member.role !== "admin") return void res.status(403).json({ error: "Apenas admins podem promover membros" });
    db.run(`UPDATE group_members SET role = 'admin' WHERE group_id = ? AND username = ?`, [group_id, username], function (this: any, err: any) {
      if (err) return void res.status(500).json({ error: err.message });
      io.emit("member_promoted", { group_id, username }); res.json({ ok: true });
    });
  });
});

app.post("/groups/:group_id/members/role", (req, res) => {
  const { group_id } = req.params, { username, role, updated_by } = req.body;
  if (!username || !role || !updated_by) return void res.status(400).json({ error: "Campos obrigatórios" });
  if (role !== "admin" && role !== "member") return void res.status(400).json({ error: "Role inválido" });
  db.get(`SELECT role FROM group_members WHERE group_id = ? AND username = ?`, [group_id, updated_by], (_err: any, member: any) => {
    if (_err || !member || member.role !== "admin") return void res.status(403).json({ error: "Apenas admins podem alterar papéis" });
    db.run(`UPDATE group_members SET role = ? WHERE group_id = ? AND username = ?`, [role, group_id, username], function (this: any, err: any) {
      if (err) return void res.status(500).json({ error: err.message });
      const sysText = role === "admin"
        ? `${updated_by} promoveu ${username} a administrador`
        : `${updated_by} rebaixou ${username} a membro`;
      insertGroupSystemMsg(group_id, sysText);
      db.all(`SELECT username FROM group_members WHERE group_id = ?`, [group_id], (_e: any, members: any[]) => {
        if (members) members.forEach((m) => {
          if (onlineUsers.has(m.username)) onlineUsers.get(m.username)!.forEach((sid) => io.to(sid).emit("group_role_change", { group_id, username, role, by: updated_by }));
        });
      });
      res.json({ ok: true, username, role });
    });
  });
});

app.post("/groups/:group_id/toggle-close", (req, res) => {
  const { group_id } = req.params, { username } = req.body;
  db.get(`SELECT role FROM group_members WHERE group_id = ? AND username = ?`, [group_id, username], (_err: any, member: any) => {
    if (_err || !member || member.role !== "admin") return void res.status(403).json({ error: "Apenas admins podem fechar o grupo" });
    db.get(`SELECT is_closed FROM groups WHERE group_id = ?`, [group_id], (_err2: any, group: any) => {
      if (_err2 || !group) return void res.status(404).json({ error: "Grupo não encontrado" });
      const newState = group.is_closed ? 0 : 1;
      db.run(`UPDATE groups SET is_closed = ? WHERE group_id = ?`, [newState, group_id], function (this: any, err: any) {
        if (err) return void res.status(500).json({ error: err.message });
        insertGroupSystemMsg(group_id, newState ? `${username} fechou o grupo — apenas admins podem enviar mensagens` : `${username} abriu o grupo — todos podem enviar mensagens`);
        db.all(`SELECT username FROM group_members WHERE group_id = ?`, [group_id], (_e: any, members: any[]) => {
          if (members) members.forEach((m) => {
            if (onlineUsers.has(m.username)) onlineUsers.get(m.username)!.forEach((sid) => io.to(sid).emit("group_status_changed", { group_id, is_closed: newState }));
          });
        });
        res.json({ ok: true, is_closed: newState });
      });
    });
  });
});

app.get("/groups/:group_id/messages", (req, res) => {
  db.all(`SELECT * FROM group_messages WHERE group_id = ? ORDER BY time ASC`, [req.params.group_id], (_err: any, rows: any) => {
    if (_err) return void res.status(500).json([]);
    const viewer = String(req.query.username || "").toLowerCase().trim();
    const allRows = rows || [];
    Promise.all(allRows.map(async (row: any) => {
      if (row.type !== "poll") return row;
      try {
        const [votesResult, selectedResult] = await Promise.all([
          pool.query(`SELECT option_index, COUNT(*)::int AS count FROM poll_votes WHERE message_id = $1 GROUP BY option_index`, [row.id]),
          viewer
            ? pool.query(`SELECT option_index FROM poll_votes WHERE message_id = $1 AND username = $2 LIMIT 1`, [row.id, viewer])
            : Promise.resolve({ rows: [] }),
        ]);
        const poll = typeof row.content === "string" ? JSON.parse(row.content) : row.content;
        const counts = Array.from({ length: Array.isArray(poll?.options) ? poll.options.length : 0 }, () => 0);
        for (const vote of votesResult.rows) counts[Number(vote.option_index)] = Number(vote.count);
        return { ...row, content: JSON.stringify({ ...poll, counts }), poll_selected_option: selectedResult.rows[0]?.option_index ?? null };
      } catch {
        return row;
      }
    })).then((hydratedRows) => res.json(hydratedRows));
  });
});

app.post("/groups/:group_id/update", (req, res) => {
  const { group_id } = req.params, { name, description, avatar, updated_by } = req.body;
  const ub = (updated_by || "").toLowerCase().trim();
  if (!ub) return void res.status(400).json({ error: "updated_by obrigatório" });
  // Check admin role in group_members
  db.get(`SELECT role FROM group_members WHERE group_id = ? AND LOWER(username) = ?`, [group_id, ub], (_e1: any, member: any) => {
    const isMemberAdmin = !_e1 && member && member.role === "admin";
    // Also check if user is the group creator
    db.get(`SELECT created_by FROM groups WHERE group_id = ?`, [group_id], async (_e2: any, grp: any) => {
      const isCreator = !_e2 && grp && (grp.created_by || "").toLowerCase() === ub;
      if (!isMemberAdmin && !isCreator) {
        return void res.status(403).json({ error: "Apenas admins podem atualizar o grupo" });
      }
      let updates: string[] = [], values: any[] = [];
      if (name) { updates.push("name = ?"); values.push(name); }
      if (description !== undefined) { updates.push("description = ?"); values.push(description); }
      if (avatar && avatar.startsWith("data:")) {
        const saved = await saveBase64File(avatar, "avatars", "group_" + group_id);
        if (!saved) return void res.status(500).json({ error: "Falha ao salvar a foto do grupo. Nada foi alterado — tente novamente." });
        updates.push("avatar = ?"); values.push(saved);
      }
      if (updates.length === 0) return void res.status(400).json({ error: "Nenhuma atualização fornecida" });
      values.push(group_id);
      db.run(`UPDATE groups SET ${updates.join(", ")} WHERE group_id = ?`, values, function (this: any, err: any) {
        if (err) return void res.status(500).json({ error: err.message });
        db.get(`SELECT * FROM groups WHERE group_id = ?`, [group_id], (_e: any, g: any) => {
          io.emit("group_updated", { group_id });
          res.json({ ok: true, group: { group_id, name: g?.name || name || "", avatar: g?.avatar || "" } });
        });
      });
    });
  });
});

app.post("/groups/:group_id/avatar", (req, res) => {
  const { group_id } = req.params;
  const { username, avatar } = req.body;
  const ub = (username || "").toLowerCase().trim();
  if (!ub) return void res.status(400).json({ error: "username obrigatório" });
  if (!avatar || typeof avatar !== "string" || !avatar.startsWith("data:")) {
    return void res.status(400).json({ error: "avatar inválido" });
  }
  db.get(`SELECT role FROM group_members WHERE group_id = ? AND LOWER(username) = ?`, [group_id, ub], (_e1: any, member: any) => {
    const isMemberAdmin = !_e1 && member && member.role === "admin";
    db.get(`SELECT * FROM groups WHERE group_id = ?`, [group_id], async (_e2: any, grp: any) => {
      if (_e2 || !grp) return void res.status(404).json({ error: "Grupo não encontrado" });
      const isCreator = (grp.created_by || "").toLowerCase() === ub;
      if (!isMemberAdmin && !isCreator) {
        return void res.status(403).json({ error: "Apenas admins podem alterar a foto do grupo" });
      }
      const saved = await saveBase64File(avatar, "avatars", "group_" + group_id);
      if (!saved) return void res.status(400).json({ error: "Falha ao salvar imagem" });
      db.run(`UPDATE groups SET avatar = ? WHERE group_id = ?`, [saved, group_id], function (this: any, err: any) {
        if (err) return void res.status(500).json({ error: err.message });
        io.emit("group_updated", { group_id });
        res.json({ ok: true, group: { group_id, name: grp.name, avatar: saved } });
      });
    });
  });
});

app.post("/friends/request", (req, res) => {
  const { requester, recipient } = req.body;
  if (!requester || !recipient) return void res.status(400).json({ error: "Campos obrigatórios" });
  const req_l = requester.toLowerCase().trim(), rec_l = recipient.toLowerCase().trim();
  if (req_l === rec_l) return void res.status(400).json({ error: "Não pode adicionar a si mesmo" });
  if (rec_l === "crisalida") return void res.status(403).json({ error: "impossível enviar convite a crisálida" });
  db.get(`SELECT * FROM friendships WHERE (requester = ? AND recipient = ?) OR (requester = ? AND recipient = ?)`, [req_l, rec_l, rec_l, req_l], (_err: any, existing: any) => {
    if (existing) {
      if (existing.status === "accepted") return void res.status(400).json({ error: "Já são amigos" });
      if (existing.status === "pending") return void res.status(400).json({ error: "Solicitação já enviada" });
    }
    const now = new Date().toISOString();
    db.run(`INSERT INTO friendships (requester, recipient, status, created_at, updated_at) VALUES (?, ?, 'pending', ?, ?)`, [req_l, rec_l, now, now], function (this: any, err: any) {
      if (err) return void res.status(500).json({ error: err.message });
      if (onlineUsers.has(rec_l)) onlineUsers.get(rec_l)!.forEach((sid) => io.to(sid).emit("friend_request", { from: req_l, id: this.lastID }));
      res.json({ ok: true, id: this.lastID });
    });
  });
});

app.post("/friends/accept", (req, res) => {
  const { requester, recipient } = req.body;
  if (!requester || !recipient) return void res.status(400).json({ error: "Campos obrigatórios" });
  const req_l = requester.toLowerCase().trim(), rec_l = recipient.toLowerCase().trim();
  const now = new Date().toISOString();
  db.run(`UPDATE friendships SET status='accepted', updated_at=? WHERE requester=? AND recipient=? AND status='pending'`, [now, req_l, rec_l], function (this: any, err: any) {
    if (err || this.changes === 0) return void res.status(404).json({ error: "Solicitação não encontrada" });
    if (onlineUsers.has(req_l)) onlineUsers.get(req_l)!.forEach((sid) => io.to(sid).emit("friend_accepted", { by: rec_l }));
    res.json({ ok: true });
  });
});

app.post("/friends/reject", (req, res) => {
  const { requester, recipient } = req.body;
  if (!requester || !recipient) return void res.status(400).json({ error: "Campos obrigatórios" });
  const req_l = requester.toLowerCase().trim(), rec_l = recipient.toLowerCase().trim();
  db.run(`DELETE FROM friendships WHERE requester=? AND recipient=? AND status='pending'`, [req_l, rec_l], function (this: any, err: any) {
    if (err) return void res.status(500).json({ error: err.message });
    res.json({ ok: true });
  });
});

app.post("/friends/remove", (req, res) => {
  const { username, friend } = req.body;
  if (!username || !friend) return void res.status(400).json({ error: "Campos obrigatórios" });
  const u_l = username.toLowerCase().trim(), f_l = friend.toLowerCase().trim();
  db.run(`DELETE FROM friendships WHERE (requester = ? AND recipient = ?) OR (requester = ? AND recipient = ?)`, [u_l, f_l, f_l, u_l], (err: any) => {
    if (err) return void res.status(500).json({ error: err.message });
    if (onlineUsers.has(f_l)) onlineUsers.get(f_l)!.forEach((sid) => io.to(sid).emit("friend_removed", { by: u_l }));
    res.json({ ok: true });
  });
});

app.get("/friends/requests/:username", (req, res) => {
  const username = req.params.username.toLowerCase().trim();
  db.all(`SELECT * FROM friendships WHERE recipient = ? AND status = 'pending'`, [username], (_err: any, rows: any[]) => {
    if (_err) return void res.status(500).json([]);
    const users = getUsers();
    res.json((rows || []).map((r: any) => {
      const u = users.find((u: any) => u.username === r.requester);
      return { id: r.id, username: r.requester, display_name: u?.display_name || r.requester, avatar: u?.avatar || "", created_at: r.created_at };
    }));
  });
});

app.get("/friends/:username", (req, res) => {
  const username = req.params.username.toLowerCase().trim();
  db.all(`SELECT * FROM friendships WHERE (requester = ? OR recipient = ?) AND status = 'accepted'`, [username, username], (_err: any, rows: any[]) => {
    if (_err) return void res.status(500).json([]);
    const users = getUsers();
    res.json((rows || []).map((f: any) => {
      const fn = f.requester === username ? f.recipient : f.requester;
      const u = users.find((u: any) => u.username === fn);
      return {
        username: fn, display_name: u?.display_name || fn, avatar: u?.avatar || "",
        is_online: isUserOnline(fn), last_seen: u?.last_seen || null,
        is_ai: u?.is_ai || false, is_verified: u?.is_verified || false,
        is_vip: u?.is_vip || false, vip_expires_at: u?.vip_expires_at || null,
      };
    }));
  });
});

app.get("/friends/check/:user1/:user2", (req, res) => {
  db.get(`SELECT * FROM friendships WHERE ((requester = ? AND recipient = ?) OR (requester = ? AND recipient = ?)) AND status = 'accepted'`,
    [req.params.user1.toLowerCase(), req.params.user2.toLowerCase(), req.params.user2.toLowerCase(), req.params.user1.toLowerCase()],
    (_err: any, row: any) => res.json({ areFriends: !!row }));
});

// ─── Cinema REST routes ────────────────────────────────────────────────────
app.get("/cinema/search", async (req: any, res: any) => {
  const q = (req.query.q as string || "").trim();
  if (!q) return void res.status(400).json({ error: "Missing query" });
  try {
    const data = await ytSearch(q);
    const resultado = (data.videos || []).slice(0, 12).map((video: any) => ({
      type: "video",
      videoId: video.videoId,
      url: video.url,
      title: video.title,
      description: video.description || "",
      thumbnail: video.thumbnail || video.image || "",
      image: video.image || video.thumbnail || "",
      timestamp: video.timestamp || video.duration?.timestamp || "",
      duration: video.duration || null,
      author: video.author || null,
    }));
    return void res.json({ resultado });
  } catch (e: any) {
    return void res.status(502).json({ error: "Erro na busca. Tente novamente.", resultado: [] });
  }
});

app.get("/cinema/rooms", (_req, res) => {
  db.all(
    `SELECT r.*, (SELECT COUNT(*) FROM cinema_members m WHERE m.room_code = r.code) as member_count FROM cinema_rooms r WHERE r.is_public = 1 ORDER BY r.last_active DESC LIMIT 30`,
    [],
    (_e: any, rows: any[]) => res.json(rows || [])
  );
});

app.get("/cinema/invites/:username", (req, res) => {
  const un = req.params.username.toLowerCase().trim();
  db.all(
    `SELECT * FROM cinema_invites WHERE to_user = ? AND status = 'pending' ORDER BY created_at DESC LIMIT 20`,
    [un],
    (_e: any, rows: any[]) => res.json(rows || [])
  );
});

app.get("/cinema/room/:code", (req, res) => {
  const code = req.params.code.toUpperCase().trim();
  db.get(`SELECT * FROM cinema_rooms WHERE code = ?`, [code], (_e: any, room: any) => {
    if (!room) return void res.status(404).json({ error: "Sala não encontrada" });
    db.all(`SELECT username, is_muted FROM cinema_members WHERE room_code = ?`, [code], (_e2: any, members: any[]) => {
      const users: any[] = [];
      res.json({ ...room, members: members || [], users });
    });
  });
});

// ─── Sticker REST routes ───────────────────────────────────────────────────
app.get("/stickers/:username", (req: any, res: any) => {
  const username = req.params.username.toLowerCase().trim();
  db.all(
    `SELECT * FROM user_stickers WHERE username = ? ORDER BY is_favorite DESC, created_at DESC`,
    [username],
    (_err: any, rows: any[]) => res.json(rows || [])
  );
});

app.post("/stickers", (req: any, res: any) => {
  const { username, sticker_url, pack_name, source_path } = req.body;
  if (!username || !sticker_url) return void res.status(400).json({ error: "Campos obrigatórios" });
  const ts = new Date().toISOString();
  db.run(
    `INSERT INTO user_stickers (username, sticker_url, source_path, pack_name, is_favorite, created_at) VALUES (?, ?, ?, ?, 0, ?)`,
    [username.toLowerCase().trim(), sticker_url, source_path || null, pack_name || "Meus Stickers", ts],
    function(this: any, err: any) {
      if (err) return void res.status(500).json({ error: err.message });
      res.json({ ok: true, id: this.lastID });
    }
  );
});

app.post("/stickers/:id/favorite", (req: any, res: any) => {
  const id = parseInt(req.params.id);
  const { username } = req.body;
  if (!username) return void res.status(400).json({ error: "username obrigatório" });
  db.get(
    `SELECT is_favorite FROM user_stickers WHERE id = ? AND username = ?`,
    [id, username.toLowerCase().trim()],
    (_err: any, row: any) => {
      if (!row) return void res.status(404).json({ error: "Sticker não encontrado" });
      const newVal = row.is_favorite ? 0 : 1;
      db.run(`UPDATE user_stickers SET is_favorite = ? WHERE id = ?`, [newVal, id], (err: any) => {
        if (err) return void res.status(500).json({ error: err.message });
        res.json({ ok: true, is_favorite: newVal });
      });
    }
  );
});

app.delete("/stickers/:id", (req: any, res: any) => {
  const id = parseInt(req.params.id);
  const username = (req.body?.username || req.query?.username || "").toString().toLowerCase().trim();
  db.run(`DELETE FROM user_stickers WHERE id = ? AND username = ?`, [id, username], (err: any) => {
    if (err) return void res.status(500).json({ error: err.message });
    res.json({ ok: true });
  });
});

// ── Block / Unblock ────────────────────────────────────────────────────────
app.post("/block", (req, res) => {
  const { blocker, blocked } = req.body;
  if (!blocker || !blocked) return void res.status(400).json({ error: "Campos obrigatórios" });
  const now = new Date().toISOString();
  db.run(`INSERT OR IGNORE INTO user_blocks (blocker, blocked, created_at) VALUES (?, ?, ?)`,
    [blocker.toLowerCase(), blocked.toLowerCase(), now],
    (err: any) => {
      if (err) return void res.status(500).json({ error: err.message });
      res.json({ ok: true });
    });
});

app.post("/unblock", (req, res) => {
  const { blocker, blocked } = req.body;
  if (!blocker || !blocked) return void res.status(400).json({ error: "Campos obrigatórios" });
  db.run(`DELETE FROM user_blocks WHERE blocker = ? AND blocked = ?`,
    [blocker.toLowerCase(), blocked.toLowerCase()],
    (err: any) => {
      if (err) return void res.status(500).json({ error: err.message });
      res.json({ ok: true });
    });
});

app.get("/blocks/:username", (req, res) => {
  const username = req.params.username.toLowerCase().trim();
  db.all(`SELECT blocked FROM user_blocks WHERE blocker = ?`, [username], (_err: any, rows: any[]) => {
    res.json((rows || []).map((r: any) => r.blocked));
  });
});

// ── Report ─────────────────────────────────────────────────────────────────
app.post("/report", (req, res) => {
  const { reporter, reported } = req.body;
  if (!reporter || !reported) return void res.status(400).json({ error: "Campos obrigatórios" });
  const now = new Date().toISOString();
  db.all(
    `SELECT * FROM messages WHERE (s = ? AND r = ?) OR (s = ? AND r = ?) ORDER BY time DESC LIMIT 50`,
    [reporter.toLowerCase(), reported.toLowerCase(), reported.toLowerCase(), reporter.toLowerCase()],
    (_err: any, msgs: any[]) => {
      db.run(
        `INSERT INTO reports (reporter, reported, messages_json, created_at, status) VALUES (?, ?, ?, ?, 'pending')`,
        [reporter.toLowerCase(), reported.toLowerCase(), JSON.stringify(msgs || []), now],
        (err: any) => {
          if (err) return void res.status(500).json({ error: err.message });
          res.json({ ok: true });
        }
      );
    }
  );
});

// ── Ban status check ───────────────────────────────────────────────────────
app.get("/ban-status/:username", async (req, res) => {
  const ban = await getBanStatus(req.params.username.toLowerCase().trim());
  res.json({ banned: !!ban, ban: ban || null });
});

// ── Appeal ─────────────────────────────────────────────────────────────────
app.post("/appeal", (req, res) => {
  const { username, text } = req.body;
  if (!username || !text?.trim()) return void res.status(400).json({ error: "Campos obrigatórios" });
  const un = username.toLowerCase().trim();
  const now = new Date().toISOString();
  db.get(`SELECT * FROM user_bans WHERE username = ?`, [un], (_err: any, ban: any) => {
    if (!ban) return void res.status(400).json({ error: "Usuário não está banido" });
    db.get(`SELECT * FROM appeals WHERE username = ? AND status = 'pending'`, [un], (_err2: any, existing: any) => {
      if (existing) return void res.status(400).json({ error: "Você já tem uma apelação pendente" });
      db.run(`INSERT INTO appeals (username, appeal_text, created_at, status) VALUES (?, ?, ?, 'pending')`,
        [un, text.trim(), now],
        (err: any) => {
          if (err) return void res.status(500).json({ error: err.message });
          res.json({ ok: true });
        }
      );
    });
  });
});

// ── Admin routes ───────────────────────────────────────────────────────────
app.get("/admin/users", requireVerified, (_req, res) => {
  db.all(`SELECT u.*, ub.ban_type, ub.expires_at, ub.banned_by, ub.reason as ban_reason, ub.id as ban_id
    FROM users u LEFT JOIN user_bans ub ON u.username = ub.username ORDER BY u.created_at DESC`,
    [],
    (_err: any, rows: any[]) => {
      res.json((rows || []).map((r: any) => { const { password, ...safe } = r; return safe; }));
    });
});

app.post("/admin/ban", requireVerified, (req, res) => {
  const { target, reason, ban_type } = req.body;
  if (!target) return void res.status(400).json({ error: "target obrigatório" });
  const adminUser = req.adminUser.username;
  const un = target.toLowerCase().trim();
  if (un === adminUser) return void res.status(400).json({ error: "Não pode banir a si mesmo" });
  const now = new Date().toISOString();
  let expires_at: string | null = null;
  let actualType = ban_type || "permanent";
  if (ban_type === "24h") {
    expires_at = new Date(Date.now() + 24 * 60 * 60 * 1000).toISOString();
    actualType = "temporary";
  }
  db.run(`DELETE FROM user_bans WHERE username = ?`, [un], () => {
    db.run(
      `INSERT INTO user_bans (username, reason, ban_type, expires_at, banned_by, created_at) VALUES (?, ?, ?, ?, ?, ?)`,
      [un, reason || "", actualType, expires_at, adminUser, now],
      function (this: any, err: any) {
        if (err) return void res.status(500).json({ error: err.message });
        if (onlineUsers.has(un)) onlineUsers.get(un)!.forEach((sid) => io.to(sid).emit("force_logout", { reason: "banned" }));
        res.json({ ok: true });
      }
    );
  });
});

app.post("/admin/unban", requireVerified, (req, res) => {
  const { target } = req.body;
  if (!target) return void res.status(400).json({ error: "target obrigatório" });
  db.run(`DELETE FROM user_bans WHERE username = ?`, [target.toLowerCase().trim()], (err: any) => {
    if (err) return void res.status(500).json({ error: err.message });
    res.json({ ok: true });
  });
});

app.post("/admin/delete-account", requireVerified, (req, res) => {
  const { target } = req.body;
  if (!target) return void res.status(400).json({ error: "target obrigatório" });
  const un = target.toLowerCase().trim();
  if (un === req.adminUser.username) return void res.status(400).json({ error: "Não pode deletar a si mesmo" });
  const idx = _usersCache.findIndex((u: any) => u.username === un);
  if (idx !== -1) _usersCache.splice(idx, 1);
  db.run(`DELETE FROM users WHERE username = ?`, [un], (err: any) => {
    if (err) return void res.status(500).json({ error: err.message });
    if (onlineUsers.has(un)) onlineUsers.get(un)!.forEach((sid) => io.to(sid).emit("force_logout", { reason: "deleted" }));
    res.json({ ok: true });
  });
});

app.get("/admin/reports", requireVerified, (_req, res) => {
  db.all(`SELECT * FROM reports ORDER BY created_at DESC`, [], (_err: any, rows: any[]) => {
    const users = getUsers();
    res.json((rows || []).map((r: any) => {
      const rU = users.find((u: any) => u.username === r.reporter);
      const dU = users.find((u: any) => u.username === r.reported);
      return {
        ...r,
        reporter_dn: rU?.display_name || r.reporter,
        reporter_avatar: rU?.avatar || "",
        reported_dn: dU?.display_name || r.reported,
        reported_avatar: dU?.avatar || "",
      };
    }));
  });
});

app.post("/admin/reports/:id/action", requireVerified, (req, res) => {
  const { status } = req.body;
  db.run(`UPDATE reports SET status = ? WHERE id = ?`, [status, req.params.id], (err: any) => {
    if (err) return void res.status(500).json({ error: err.message });
    res.json({ ok: true });
  });
});

app.get("/admin/appeals", requireVerified, (_req, res) => {
  db.all(`SELECT a.*, ub.ban_type, ub.reason as ban_reason, ub.banned_by
    FROM appeals a LEFT JOIN user_bans ub ON a.username = ub.username ORDER BY a.created_at DESC`, [],
    (_err: any, rows: any[]) => {
      const users = getUsers();
      res.json((rows || []).map((r: any) => {
        const u = users.find((u: any) => u.username === r.username);
        return { ...r, display_name: u?.display_name || r.username, avatar: u?.avatar || "" };
      }));
    });
});

app.post("/admin/appeals/:id/action", requireVerified, (req, res) => {
  const { action, admin_note } = req.body;
  const adminUser = req.adminUser.username;
  db.get(`SELECT * FROM appeals WHERE id = ?`, [req.params.id], (_err: any, appeal: any) => {
    if (!appeal) return void res.status(404).json({ error: "Apelação não encontrada" });
    const newStatus = action === "unban" ? "approved" : "denied";
    db.run(`UPDATE appeals SET status = ?, admin_note = ? WHERE id = ?`,
      [newStatus, admin_note || "", req.params.id], (err: any) => {
        if (err) return void res.status(500).json({ error: err.message });
        if (action === "unban") {
          db.run(`DELETE FROM user_bans WHERE username = ?`, [appeal.username], () => {});
        } else {
          const now = new Date().toISOString();
          db.run(`DELETE FROM user_bans WHERE username = ?`, [appeal.username], () => {
            db.run(`INSERT INTO user_bans (username, reason, ban_type, expires_at, banned_by, created_at) VALUES (?, ?, 'permanent', NULL, ?, ?)`,
              [appeal.username, "Banimento permanente — recurso negado", adminUser, now], () => {});
          });
        }
        res.json({ ok: true });
      });
  });
});

// ── Push Notification Endpoints ────────────────────────────────────────────
app.get("/api/push/vapid-key", (_req: any, res: any) => {
  res.setHeader("Cache-Control", "no-store");
  if (!pushConfigured) {
    return void res.status(503).json({ publicKey: "", configured: false, error: "Push notifications are not configured" });
  }
  res.json({ publicKey: VAPID_PUBLIC_KEY, configured: true });
});

app.post("/api/push/subscribe", async (req: any, res: any) => {
  const { username, subscription } = req.body;
  if (!username || !subscription?.endpoint) return void res.status(400).json({ error: "Missing params" });
  const authenticatedUsername = String(req.headers["x-username"] || "").toLowerCase().trim();
  const requestedUsername = String(username).toLowerCase().trim();
  if (!authenticatedUsername || authenticatedUsername !== requestedUsername) {
    return void res.status(401).json({ error: "x-username header must match the subscription owner" });
  }
  if (!pushConfigured) {
    return void res.status(503).json({ error: "Push notifications are not configured" });
  }
  try {
    const userCheck = await pool.query("SELECT 1 FROM users WHERE username = $1 LIMIT 1", [requestedUsername]);
    if (userCheck.rows.length === 0) return void res.status(404).json({ error: "User not found" });
    await pool.query(
      `INSERT INTO push_subscriptions (username, endpoint, subscription, created_at, updated_at)
       VALUES ($1, $2, $3, NOW(), NOW())
       ON CONFLICT (endpoint) DO UPDATE SET username=$1, subscription=$3, updated_at=NOW()`,
      [requestedUsername, subscription.endpoint, JSON.stringify(subscription)]
    );
    res.json({ ok: true });
  } catch (e: any) {
    console.error("[push/subscribe]", e.message);
    res.status(500).json({ error: e.message });
  }
});

app.delete("/api/push/unsubscribe", async (req: any, res: any) => {
  const { endpoint } = req.body;
  if (!endpoint) return void res.status(400).json({ error: "Missing endpoint" });
  const authenticatedUsername = String(req.headers["x-username"] || "").toLowerCase().trim();
  if (!authenticatedUsername) return void res.status(401).json({ error: "x-username header required" });
  await pool.query("DELETE FROM push_subscriptions WHERE endpoint = $1 AND username = $2", [endpoint, authenticatedUsername]).catch(() => {});
  res.json({ ok: true });
});

// Called by the Service Worker's pushsubscriptionchange handler when the
// browser rotates push keys — the SW can't relay through a client tab
// because the app might be closed. The server looks up the user by the old
// endpoint and replaces the subscription in-place.
app.post("/api/push/resubscribe", async (req: any, res: any) => {
  const { oldEndpoint, subscription } = req.body;
  if (!oldEndpoint || !subscription?.endpoint) return void res.status(400).json({ error: "Missing params" });
  try {
    // Find the username that owned the old subscription
    const existing = await pool.query(
      "SELECT username FROM push_subscriptions WHERE endpoint = $1",
      [oldEndpoint]
    );
    if (existing.rows.length === 0) return void res.status(404).json({ error: "Unknown subscription" });
    const username = existing.rows[0].username;
    // Delete old, upsert new
    await pool.query("DELETE FROM push_subscriptions WHERE endpoint = $1", [oldEndpoint]);
    await pool.query(
      `INSERT INTO push_subscriptions (username, endpoint, subscription, created_at, updated_at)
       VALUES ($1, $2, $3, NOW(), NOW())
       ON CONFLICT (endpoint) DO UPDATE SET username=$1, subscription=$3, updated_at=NOW()`,
      [username, subscription.endpoint, JSON.stringify(subscription)]
    );
    res.json({ ok: true, username });
  } catch (e: any) {
    console.error("[push/resubscribe]", e.message);
    res.status(500).json({ error: e.message });
  }
});

// Status endpoint: returns subscription count without sending anything
app.get("/api/push/status", async (req: any, res: any) => {
  const username = (req.headers["x-username"] as string || "").toLowerCase().trim();
  if (!username) return void res.status(400).json({ error: "x-username header required" });
  const subCheck = await pool.query(
    "SELECT endpoint, created_at FROM push_subscriptions WHERE username = $1",
    [username]
  ).catch(() => ({ rows: [] }));
  res.json({ subscriptions: subCheck.rows.length, vapidConfigured: pushConfigured });
});

// Test endpoint: sends a real push to the authenticated user so they can verify the whole chain
app.post("/api/push/test", async (req: any, res: any) => {
  const username = (req.headers["x-username"] as string || "").toLowerCase().trim();
  if (!username) return void res.status(400).json({ error: "x-username header required" });
  if (!pushConfigured) return void res.status(503).json({ ok: false, error: "Push notifications are not configured" });

  // Check how many subscriptions exist
  const subCheck = await pool.query(
    "SELECT endpoint, created_at FROM push_subscriptions WHERE username = $1",
    [username]
  ).catch(() => ({ rows: [] }));

  if (subCheck.rows.length === 0) {
    return void res.status(404).json({
      ok: false,
      error: "Nenhuma assinatura push cadastrada para este usuário. Ative as notificações nas configurações primeiro.",
      subscriptions: 0
    });
  }

  const result = await sendPushToUser(username, {
    type: "test",
    title: "🔔 REDSPACE",
    body: "Notificação de teste — funcionou!",
    tag: "push-test",
    url: "/"
  });

  res.json({
    ok: result.sent > 0,
    subscriptions: subCheck.rows.length,
    sent: result.sent,
    errors: result.errors,
    endpoints: subCheck.rows.map((r: any) => ({ endpoint: r.endpoint.substring(0, 80) + "...", registered: r.created_at }))
  });
});

app.get("/api/healthz", (_req, res) => {
  res.json({ ok: true });
});

export { databaseReady, server };
