#!/usr/bin/env node

// Health AI Daily — self-contained cloud CLI. No runtime git.
// Commands: version | doctor | collect | collect-git | prepare | validate |
//           publish-all | publish | render-feed-doc | notify | state-show

import { readFile, writeFile, mkdir } from 'fs/promises';
import { existsSync, readFileSync, writeFileSync, mkdirSync, chmodSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';
import { execFile, execFileSync } from 'child_process';
import { createHash, createHmac } from 'crypto';

const VERSION = '0.5.0';
const APP_DIR = dirname(fileURLToPath(import.meta.url));
const DEFAULT_DATA_DIR = '/data/ai-daily';
const DINGTALK_API = process.env.DINGTALK_API_BASE || 'https://api.dingtalk.com';

const CENTRAL_FEEDS = {
  x: 'https://raw.githubusercontent.com/zarazhangrui/follow-builders/main/feed-x.json',
  podcasts: 'https://raw.githubusercontent.com/zarazhangrui/follow-builders/main/feed-podcasts.json',
  blogs: 'https://raw.githubusercontent.com/zarazhangrui/follow-builders/main/feed-blogs.json',
};
const PROMPT_KEYS = [
  'summarize_podcast', 'summarize_tweets', 'summarize_blogs', 'digest_intro', 'translate',
];

const X_API_BASE = process.env.X_API_BASE || 'https://api.x.com/2';
const X_LOOKBACK_HOURS = 24;
const X_MAX_TWEETS_PER_USER = 3;
const SEEN_MAX_AGE_DAYS = 7;

const FEED_FETCH_ATTEMPTS = 3;
const FEED_FETCH_BACKOFF_MS = 60000;
const FEED_FETCH_TIMEOUT_SEC = 20;
const TRANSCRIPT_INLINE_MAX = 4000;
const TRANSCRIPT_HEAD_SNIPPET = 500;

const MAX_DOC_CHARS = 50000;
const MAX_NOTIFY_CHARS = 4000;

const FEED_BEGIN = '=== AI-DAILY-CUSTOM-FEED-BASE64-BEGIN ===';
const FEED_END = '=== AI-DAILY-CUSTOM-FEED-BASE64-END ===';

const STATUS_MARKER = '__HTTP_STATUS__:';

// -- env ---------------------------------------------------------------------

function dataDir() {
  return process.env.AI_DAILY_DATA_DIR || DEFAULT_DATA_DIR;
}

function loadEnv() {
  const path = join(dataDir(), '.env');
  const out = {};
  if (!existsSync(path)) return out;
  for (const line of readFileSync(path, 'utf-8').split('\n')) {
    if (/^\s*#/.test(line)) continue;
    const m = line.match(/^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*?)\s*$/);
    if (m) out[m[1]] = m[2].replace(/^["']|["']$/g, '').trim();
  }
  return out;
}

function makeConfig() {
  const file = loadEnv();
  return {
    file,
    get(key, fallback = '') {
      let value = file[key] || process.env[key] || '';
      if (value.startsWith('<待填:')) value = '';
      return value || fallback;
    },
  };
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// -- HTTP --------------------------------------------------------------------

function runCurl(args, stdin = '') {
  return new Promise((resolve) => {
    const child = execFile('curl', args, { maxBuffer: 64 * 1024 * 1024 }, (err, stdout) => {
      if (err && !stdout) return resolve({ ok: false, status: 0, body: err.message });
      const out = String(stdout);
      const idx = out.lastIndexOf(STATUS_MARKER);
      let status = 0;
      let body = out;
      if (idx !== -1) {
        status = parseInt(out.slice(idx + STATUS_MARKER.length).trim(), 10) || 0;
        body = out.slice(0, idx).replace(/\n$/, '');
      }
      resolve({ ok: status >= 200 && status < 300, status, body });
    });
    child.stdin.end(stdin);
  });
}

function curlJSON(method, url, { headers = {}, body, timeoutSec = 30 } = {}) {
  const hdrs = body === undefined
    ? { ...headers }
    : { 'Content-Type': 'application/json', ...headers };
  const args = [
    '-sS', '--max-time', String(timeoutSec),
    '-o', '/dev/stdout',
    '-w', `\n${STATUS_MARKER}%{http_code}`,
    '-X', method,
  ];
  for (const [k, v] of Object.entries(hdrs)) args.push('-H', `${k}: ${v}`);
  if (body !== undefined) args.push('--data-binary', '@-');
  args.push(url);
  return runCurl(args, body === undefined ? '' : body);
}

function curlText(url, timeoutSec = 25) {
  return runCurl([
    '-sSL', '--max-time', String(timeoutSec),
    '-o', '/dev/stdout',
    '-w', `\n${STATUS_MARKER}%{http_code}`,
    url,
  ]);
}

function parseJSONSafe(text) {
  try { return JSON.parse(text); } catch { return null; }
}

async function retry(fn, { attempts = 3, delayMs = 1000, label = 'op' } = {}) {
  let lastErr;
  for (let i = 1; i <= attempts; i++) {
    try {
      return await fn();
    } catch (err) {
      lastErr = err;
      console.error(`[${label}] attempt ${i}/${attempts} failed: ${err.message}`);
      if (i < attempts) await sleep(delayMs);
    }
  }
  throw lastErr;
}

async function fetchCentralJSON(url) {
  return retry(async () => {
    const res = await curlText(url, FEED_FETCH_TIMEOUT_SEC);
    if (!res.ok) {
      throw new Error(`HTTP ${res.status || 'error'}: ${String(res.body).slice(0, 300)}`);
    }
    const data = parseJSONSafe(res.body);
    if (!data) throw new Error('invalid JSON');
    return data;
  }, { attempts: FEED_FETCH_ATTEMPTS, delayMs: FEED_FETCH_BACKOFF_MS, label: 'feed' });
}

// -- state backend -----------------------------------------------------------
// Priority: local file (tests/manual) > Aliyun OSS > generic HTTP KV.
// OSS object shape: { version, oauth:{refreshToken,...}, collector:{seenTweets}, lastRun }

function stateBackend(cfg) {
  const file = cfg.get('AI_DAILY_STATE_FILE');
  if (file) return { type: 'file', path: file };

  const bucket = cfg.get('OSS_STATE_BUCKET');
  if (bucket) {
    const id = cfg.get('OSS_STATE_ACCESS_KEY_ID');
    const secret = cfg.get('OSS_STATE_ACCESS_KEY_SECRET');
    if (!id || !secret) {
      return { type: 'invalid', reason: 'OSS_STATE_BUCKET set but OSS_STATE_ACCESS_KEY_ID/SECRET missing' };
    }
    return {
      type: 'oss',
      bucket,
      endpoint: cfg.get('OSS_STATE_ENDPOINT', 'oss-cn-hangzhou.aliyuncs.com')
        .replace(/^https?:\/\//, '')
        .replace(/\/+$/, ''),
      key: cfg.get('OSS_STATE_KEY', 'health-ai-daily/state.json').replace(/^\/+/, ''),
      id,
      secret,
      sts: cfg.get('OSS_STATE_SECURITY_TOKEN'),
    };
  }

  const url = cfg.get('AI_DAILY_STATE_URL');
  if (url) return { type: 'http', url, token: cfg.get('AI_DAILY_STATE_TOKEN') };
  return { type: 'none' };
}

function ossObjectUrl(b) {
  const path = b.key.split('/').map(encodeURIComponent).join('/');
  return `https://${b.bucket}.${b.endpoint}/${path}`;
}

function ossHeaders(b, method, body) {
  const date = new Date().toUTCString();
  const md5 = body ? createHash('md5').update(body).digest('base64') : '';
  const type = body ? 'application/json' : '';
  const headers = { Date: date };
  let canonicalHeaders = '';
  if (b.sts) {
    headers['x-oss-security-token'] = b.sts;
    canonicalHeaders = `x-oss-security-token:${b.sts}\n`;
  }
  const stringToSign = `${method}\n${md5}\n${type}\n${date}\n${canonicalHeaders}/${b.bucket}/${b.key}`;
  const signature = createHmac('sha1', b.secret).update(stringToSign).digest('base64');
  headers.Authorization = `OSS ${b.id}:${signature}`;
  if (body) {
    headers['Content-Type'] = type;
    headers['Content-MD5'] = md5;
  }
  return headers;
}

async function ossGet(b) {
  const res = await curlJSON('GET', ossObjectUrl(b), { headers: ossHeaders(b, 'GET') });
  if (res.status === 404) return null;
  if (!res.ok) throw new Error(`OSS GET failed (HTTP ${res.status}): ${res.body.slice(0, 300)}`);
  const data = parseJSONSafe(res.body);
  if (!data) throw new Error('OSS state is not valid JSON');
  return data;
}

async function ossPut(b, body) {
  const res = await curlJSON('PUT', ossObjectUrl(b), {
    headers: ossHeaders(b, 'PUT', body),
    body,
  });
  if (!res.ok) throw new Error(`OSS PUT failed (HTTP ${res.status}): ${res.body.slice(0, 300)}`);
  return true;
}

async function httpGetState(b) {
  const headers = b.token ? { Authorization: `Bearer ${b.token}` } : {};
  const res = await curlJSON('GET', b.url, { headers });
  if (res.status === 404) return null;
  if (!res.ok) throw new Error(`state GET failed (HTTP ${res.status}): ${res.body.slice(0, 300)}`);
  const data = parseJSONSafe(res.body);
  if (!data) throw new Error('state endpoint returned invalid JSON');
  return data;
}

async function httpPutState(b, body) {
  const headers = b.token ? { Authorization: `Bearer ${b.token}` } : {};
  const res = await curlJSON('PUT', b.url, { headers, body });
  if (!res.ok) throw new Error(`state PUT failed (HTTP ${res.status}): ${res.body.slice(0, 300)}`);
  return true;
}

function normalizeState(raw) {
  const oauth = raw?.oauth || raw || {};
  return {
    version: 1,
    oauth: {
      refreshToken: oauth.refreshToken || '',
      corpId: oauth.corpId || null,
      unionId: oauth.unionId || '',
      updatedAt: oauth.updatedAt || null,
    },
    collector: {
      seenTweets: raw?.collector?.seenTweets || raw?.seenTweets || {},
    },
    docs: raw?.docs || {},
    lastRun: raw?.lastRun || null,
  };
}

async function loadState(cfg, backend) {
  let raw = null;
  if (backend.type === 'file') {
    if (existsSync(backend.path)) {
      const text = await readFile(backend.path, 'utf-8');
      raw = parseJSONSafe(text);
      if (!raw) throw new Error(`State file is not valid JSON: ${backend.path}`);
    }
  } else if (backend.type === 'oss') {
    raw = await ossGet(backend);
  } else if (backend.type === 'http') {
    raw = await httpGetState(backend);
  } else if (backend.type === 'invalid') {
    throw new Error(backend.reason);
  }

  const state = normalizeState(raw);
  const bootstrap = cfg.get('DINGTALK_REFRESH_TOKEN');
  if (!state.oauth.refreshToken && bootstrap) state.oauth.refreshToken = bootstrap;
  return state;
}

function pruneSeen(state) {
  const cutoff = Date.now() - SEEN_MAX_AGE_DAYS * 24 * 60 * 60 * 1000;
  const pruned = {};
  for (const [id, ts] of Object.entries(state.collector.seenTweets || {})) {
    if (Number(ts) > cutoff) pruned[id] = ts;
  }
  state.collector.seenTweets = pruned;
}

async function saveState(cfg, backend, state) {
  pruneSeen(state);
  const body = JSON.stringify(state, null, 2) + '\n';

  if (backend.type === 'file') {
    mkdirSync(dirname(backend.path), { recursive: true });
    writeFileSync(backend.path, body, { mode: 0o600 });
    try { chmodSync(backend.path, 0o600); } catch { /* best effort */ }
    return { persisted: true, backend: 'file' };
  }

  if (backend.type === 'oss' || backend.type === 'http') {
    try {
      await retry(
        () => (backend.type === 'oss' ? ossPut(backend, body) : httpPutState(backend, body)),
        { attempts: 3, delayMs: 2000, label: 'state' }
      );
      return { persisted: true, backend: backend.type };
    } catch (err) {
      const fallback = join(dataDir(), 'state.json');
      mkdirSync(dataDir(), { recursive: true });
      writeFileSync(fallback, body, { mode: 0o600 });
      return { persisted: false, backend: backend.type, error: err.message };
    }
  }

  if (backend.type === 'invalid') throw new Error(backend.reason);

  mkdirSync(dataDir(), { recursive: true });
  writeFileSync(join(dataDir(), 'state.json'), body, { mode: 0o600 });
  return { persisted: false, backend: 'none', error: 'no persistent state backend configured' };
}

// -- DingTalk OAuth ----------------------------------------------------------

async function refreshUserToken(cfg, refreshToken) {
  const clientId = cfg.get('DINGTALK_CLIENT_ID');
  const clientSecret = cfg.get('DINGTALK_CLIENT_SECRET');
  if (!clientId || !clientSecret) {
    throw new Error('DINGTALK_CLIENT_ID / DINGTALK_CLIENT_SECRET missing');
  }
  const res = await curlJSON('POST', `${DINGTALK_API}/v1.0/oauth2/userAccessToken`, {
    body: JSON.stringify({
      clientId,
      clientSecret,
      grantType: 'refresh_token',
      refreshToken,
    }),
  });
  const data = parseJSONSafe(res.body) || {};
  if (!res.ok || !data.accessToken) {
    throw new Error(
      `userAccessToken(refresh) failed (HTTP ${res.status}): ` +
      `${data.message || data.code || res.body.slice(0, 300)}`
    );
  }
  return data;
}

async function acquireUserToken(cfg, backend, state) {
  const candidates = [];
  if (state.oauth.refreshToken) {
    candidates.push({ source: 'state', token: state.oauth.refreshToken });
  }
  const bootstrap = cfg.get('DINGTALK_REFRESH_TOKEN');
  if (bootstrap && bootstrap !== state.oauth.refreshToken) {
    candidates.push({ source: 'env', token: bootstrap });
  }
  if (candidates.length === 0) {
    throw new Error(
      'No DingTalk refresh token: state is empty and DINGTALK_REFRESH_TOKEN is unset. ' +
      'Run cloud/oauth-login.mjs locally once.'
    );
  }

  let lastErr = null;
  for (const candidate of candidates) {
    try {
      const data = await refreshUserToken(cfg, candidate.token);
      state.oauth.refreshToken = data.refreshToken || candidate.token;
      state.oauth.corpId = data.corpId || state.oauth.corpId;
      state.oauth.updatedAt = new Date().toISOString();
      const saved = await saveState(cfg, backend, state);
      return {
        accessToken: data.accessToken,
        source: candidate.source,
        statePersisted: saved.persisted,
        stateError: saved.error || null,
        stateBackend: saved.backend,
      };
    } catch (err) {
      lastErr = err;
      console.error(`[auth] refresh via ${candidate.source} failed: ${err.message}`);
    }
  }
  throw new Error(`DingTalk token refresh failed: ${lastErr?.message || 'unknown error'}`);
}

// -- collector ---------------------------------------------------------------

function normalizeAccounts(raw, source) {
  const list = Array.isArray(raw) ? raw : raw?.accounts;
  if (!Array.isArray(list)) {
    throw new Error(`${source} must be an array or { accounts: [...] }`);
  }
  return list
    .filter((a) => a && a.handle)
    .map((a) => ({ name: a.name || a.handle, handle: String(a.handle).replace(/^@/, '') }));
}

function loadAccounts(path = process.env.AI_DAILY_ACCOUNTS_PATH || join(APP_DIR, 'accounts.json')) {
  if (!existsSync(path)) throw new Error(`accounts.json not found: ${path}`);
  return normalizeAccounts(JSON.parse(readFileSync(path, 'utf-8')), path);
}

function loadPrompts() {
  const path = process.env.AI_DAILY_PROMPTS_PATH || join(APP_DIR, 'prompts.json');
  if (!existsSync(path)) throw new Error(`prompts.json not found: ${path}`);
  const prompts = JSON.parse(readFileSync(path, 'utf-8'));
  for (const key of PROMPT_KEYS) {
    if (!prompts[key]) throw new Error(`prompts.json is missing "${key}"`);
  }
  return prompts;
}

async function fetchXUsers(accounts, bearer, errors) {
  const handles = accounts.map((a) => a.handle);
  const userMap = {};
  for (let i = 0; i < handles.length; i += 100) {
    const batch = handles.slice(i, i + 100);
    const url = `${X_API_BASE}/users/by?usernames=${batch.map(encodeURIComponent).join(',')}` +
      '&user.fields=name,description';
    const res = await curlJSON('GET', url, {
      headers: { Authorization: `Bearer ${bearer}` },
    });
    if (res.status === 401) {
      errors.push('X API: 401 unauthorized (check COLLECTOR_X_BEARER_TOKEN)');
      return userMap;
    }
    if (!res.ok) {
      errors.push(`X API: user lookup failed (HTTP ${res.status})`);
      continue;
    }
    const data = parseJSONSafe(res.body) || {};
    for (const user of data.data || []) {
      userMap[user.username.toLowerCase()] = {
        id: user.id,
        name: user.name,
        description: user.description || '',
      };
    }
    for (const err of data.errors || []) {
      errors.push(`X API: user not found: ${err.value || err.detail}`);
    }
  }
  return userMap;
}

async function fetchX(accounts, bearer, state, errors) {
  const results = [];
  const cutoff = new Date(Date.now() - X_LOOKBACK_HOURS * 60 * 60 * 1000);
  const userMap = await fetchXUsers(accounts, bearer, errors);
  if (Object.keys(userMap).length === 0) return results;

  for (const account of accounts) {
    const user = userMap[account.handle.toLowerCase()];
    if (!user) continue;

    const url = `${X_API_BASE}/users/${user.id}/tweets?max_results=5` +
      '&tweet.fields=created_at,public_metrics,referenced_tweets,note_tweet' +
      '&exclude=retweets,replies' +
      `&start_time=${encodeURIComponent(cutoff.toISOString())}`;
    const res = await curlJSON('GET', url, {
      headers: { Authorization: `Bearer ${bearer}` },
    });

    if (res.status === 429) {
      errors.push('X API: rate limited; skipped remaining accounts');
      break;
    }
    if (res.status === 401) {
      errors.push('X API: 401 unauthorized; skipped remaining accounts');
      break;
    }
    if (!res.ok) {
      errors.push(`X API: @${account.handle} failed (HTTP ${res.status})`);
      continue;
    }

    const data = parseJSONSafe(res.body) || {};
    const newTweets = [];
    for (const tweet of data.data || []) {
      if (state.collector.seenTweets[tweet.id]) continue;
      if (newTweets.length >= X_MAX_TWEETS_PER_USER) break;
      newTweets.push({
        id: tweet.id,
        text: tweet.note_tweet?.text || tweet.text,
        createdAt: tweet.created_at,
        url: `https://x.com/${account.handle}/status/${tweet.id}`,
        likes: tweet.public_metrics?.like_count || 0,
        retweets: tweet.public_metrics?.retweet_count || 0,
        replies: tweet.public_metrics?.reply_count || 0,
        isQuote: (tweet.referenced_tweets || []).some((r) => r.type === 'quoted'),
        quotedTweetId: (tweet.referenced_tweets || []).find((r) => r.type === 'quoted')?.id || null,
      });
      state.collector.seenTweets[tweet.id] = Date.now();
    }

    if (newTweets.length === 0) continue;
    results.push({
      source: 'x',
      name: account.name,
      handle: account.handle,
      bio: user.description,
      tweets: newTweets,
    });
    await sleep(200);
  }
  return results;
}

async function cmdCollect(cfg, args) {
  const bearer = cfg.get('COLLECTOR_X_BEARER_TOKEN');
  if (!bearer) throw new Error('COLLECTOR_X_BEARER_TOKEN is not set');
  const accounts = loadAccounts();
  if (accounts.length === 0) throw new Error('accounts.json has no accounts');

  const backend = stateBackend(cfg);
  const state = await loadState(cfg, backend);
  const errors = [];
  const x = await fetchX(accounts, bearer, state, errors);

  const feed = {
    generatedAt: new Date().toISOString(),
    lookbackHours: X_LOOKBACK_HOURS,
    x,
    stats: {
      xBuilders: x.length,
      totalTweets: x.reduce((sum, a) => sum + a.tweets.length, 0),
    },
    errors: errors.length > 0 ? errors : undefined,
  };

  const out = arg(args, '--out') || join(dataDir(), 'custom-feed.json');
  await mkdir(dirname(out), { recursive: true });
  await writeFile(out, JSON.stringify(feed, null, 2));

  const saved = await saveState(cfg, backend, state);
  console.log(JSON.stringify({
    status: 'ok',
    out,
    stats: feed.stats,
    errors: feed.errors || [],
    stateBackend: saved.backend,
    statePersisted: saved.persisted,
    stateError: saved.error || null,
  }, null, 2));
}

// -- github collector --------------------------------------------------------
// The custom X feed lives in a public GitHub repo, shaped exactly like the
// central feeds, so the digest can fetch it over raw HTTP. Accounts config and
// dedup state live in the same repo and travel with the feed commit.

function shanghaiStamp(date = new Date()) {
  return new Intl.DateTimeFormat('sv-SE', {
    timeZone: 'Asia/Shanghai',
    year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', hour12: false,
  }).format(date);
}

function gitExec(args) {
  try {
    return execFileSync('git', args, {
      encoding: 'utf-8',
      stdio: ['ignore', 'pipe', 'pipe'],
    });
  } catch (err) {
    const stderr = String(err.stderr || '').trim();
    throw new Error(`git ${gitSubcommand(args)} failed: ${stderr || err.message}`);
  }
}

function gitSubcommand(args) {
  for (let i = 0; i < args.length; i += 1) {
    if (args[i] === '-C' || args[i] === '-c') {
      i += 1;
      continue;
    }
    if (!args[i].startsWith('-')) return args[i];
  }
  return 'command';
}

function isLocalRepo(repo) {
  return /^(file:\/\/|\/|\.{1,2}\/)/.test(String(repo || '').trim());
}

function collectorRepo(repoRaw, token) {
  const raw = String(repoRaw || '').trim();
  if (!raw) throw new Error('COLLECTOR_FEED_REPO is empty');
  if (isLocalRepo(raw)) return { url: raw, local: true };

  const hostPath = raw
    .replace(/^https?:\/\//, '')
    .replace(/^ssh:\/\/git@/, '')
    .replace(/^git@/, '');
  const withGit = hostPath.endsWith('.git') ? hostPath : `${hostPath}.git`;
  if (!token) throw new Error('COLLECTOR_GIT_TOKEN is not set');
  return {
    url: `https://x-access-token:${encodeURIComponent(token)}@${withGit}`,
    local: false,
  };
}

function collectorLinks(repoRaw, branch) {
  const cleaned = String(repoRaw || '').trim()
    .replace(/^https?:\/\//, '')
    .replace(/^ssh:\/\/git@/, '')
    .replace(/^git@/, '')
    .replace(/\.git$/, '');
  const gh = cleaned.match(/^github\.com\/([^/]+)\/([^/]+)$/i);
  if (!gh) return { feedUrl: String(repoRaw || ''), rawUrl: null };
  return {
    feedUrl: `https://github.com/${gh[1]}/${gh[2]}/blob/${branch}/feed-custom-x.json`,
    rawUrl: `https://raw.githubusercontent.com/${gh[1]}/${gh[2]}/${branch}/feed-custom-x.json`,
  };
}

function syncCollectorRepo(repoUrl, branch) {
  const dir = join(dataDir(), 'feed-repo');
  mkdirSync(dirname(dir), { recursive: true });
  if (!existsSync(join(dir, '.git'))) {
    gitExec(['clone', '--depth', '1', '--branch', branch, repoUrl, dir]);
    return dir;
  }
  gitExec(['-C', dir, 'remote', 'set-url', 'origin', repoUrl]);
  gitExec(['-C', dir, 'fetch', '--depth', '1', 'origin', branch]);
  gitExec(['-C', dir, 'checkout', '-B', branch, `origin/${branch}`]);
  gitExec(['-C', dir, 'reset', '--hard', `origin/${branch}`]);
  return dir;
}

function loadRepoAccounts(dir) {
  const path = join(dir, 'config', 'accounts.json');
  if (!existsSync(path)) throw new Error(`accounts.json not found in feed repo: ${path}`);
  return normalizeAccounts(JSON.parse(readFileSync(path, 'utf-8')), path);
}

function readCollectorState(dir) {
  const path = join(dir, 'state.json');
  if (!existsSync(path)) return { seenTweets: {} };
  const raw = JSON.parse(readFileSync(path, 'utf-8'));
  return { seenTweets: raw?.seenTweets || {} };
}

function pruneSeenTweets(seen) {
  const cutoff = Date.now() - SEEN_MAX_AGE_DAYS * 24 * 60 * 60 * 1000;
  const out = {};
  for (const [id, ts] of Object.entries(seen || {})) {
    if (Number(ts) > cutoff) out[id] = ts;
  }
  return out;
}

async function writeCollectorStatus(text) {
  const path = join(dataDir(), 'collector-status.txt');
  await mkdir(dirname(path), { recursive: true });
  await writeFile(path, text.endsWith('\n') ? text : `${text}\n`);
  return path;
}

async function cmdCollectGit(cfg) {
  const title = '### 健康 AI Daily 定制采集';
  let gitToken = '';
  try {
    const bearer = cfg.get('COLLECTOR_X_BEARER_TOKEN');
    if (!bearer) throw new Error('COLLECTOR_X_BEARER_TOKEN is not set');
    gitToken = cfg.get('COLLECTOR_GIT_TOKEN');

    const repoRaw = cfg.get('COLLECTOR_FEED_REPO', 'github.com/cchli2026/mz-daily.git');
    const branch = cfg.get('COLLECTOR_FEED_BRANCH', 'main');
    const repo = collectorRepo(repoRaw, gitToken);
    const dir = syncCollectorRepo(repo.url, branch);

    const accounts = loadRepoAccounts(dir);
    if (accounts.length === 0) throw new Error('feed repo config/accounts.json has no accounts');

    const pendingSeen = { ...readCollectorState(dir).seenTweets };
    const errors = [];
    const x = await fetchX(accounts, bearer, { collector: { seenTweets: pendingSeen } }, errors);
    if (x.length === 0 && errors.length > 0) {
      throw new Error(`X collection failed: ${errors.slice(0, 3).join(' | ')}`);
    }

    const feed = {
      generatedAt: new Date().toISOString(),
      lookbackHours: X_LOOKBACK_HOURS,
      x,
      stats: {
        xBuilders: x.length,
        totalTweets: x.reduce((sum, a) => sum + a.tweets.length, 0),
      },
      errors: errors.length > 0 ? errors : undefined,
    };

    await writeFile(join(dir, 'feed-custom-x.json'), JSON.stringify(feed, null, 2) + '\n');
    await writeFile(
      join(dir, 'state.json'),
      JSON.stringify({ seenTweets: pruneSeenTweets(pendingSeen) }, null, 2) + '\n'
    );

    gitExec(['-C', dir, 'add', 'feed-custom-x.json', 'state.json']);
    let pushed = false;
    let commit = null;
    try {
      gitExec(['-C', dir, 'diff', '--cached', '--quiet']);
    } catch {
      gitExec(['-C', dir,
        '-c', 'user.name=ai-daily-collector',
        '-c', 'user.email=ai-daily-collector@users.noreply.github.com',
        'commit', '-m', `update feed-custom-x.json + state.json @ ${feed.generatedAt}`,
      ]);
      gitExec(['-C', dir, 'push', 'origin', branch]);
      pushed = true;
      commit = gitExec(['-C', dir, 'rev-parse', '--short', 'HEAD']).trim();
    }

    const counts = new Map(x.map((entry) => [entry.handle.toLowerCase(), entry.tweets.length]));
    const perAccount = accounts.map(
      (account) => `@${account.handle}: ${counts.get(account.handle.toLowerCase()) || 0} 条`
    );
    const links = collectorLinks(repoRaw, branch);
    const summary = [
      `${title} — ${shanghaiStamp()}`,
      '',
      `账号 ${accounts.length} 个；新推文 ${feed.stats.totalTweets} 条（${feed.stats.xBuilders} 个账号）`,
      ...perAccount.map((line) => `- ${line}`),
      ...(errors.length
        ? ['', `警告 ${errors.length} 条：`, ...errors.slice(0, 5).map((err) => `- ${err}`)]
        : []),
      '',
      `推送：${pushed ? `成功（${commit}）` : '无变化'}`,
      `Feed：${links.feedUrl}`,
    ].join('\n');
    const statusFile = await writeCollectorStatus(summary);

    console.log(JSON.stringify({
      status: 'ok',
      stats: feed.stats,
      errors: feed.errors || [],
      accounts: accounts.length,
      pushed,
      commit,
      feedUrl: links.feedUrl,
      rawUrl: links.rawUrl,
      statusFile,
    }, null, 2));
  } catch (err) {
    const message = String(err.message).split(gitToken || '\u0000').join('***');
    await writeCollectorStatus(`${title}失败 — ${shanghaiStamp()}\n\n${message}`).catch(() => {});
    throw new Error(message);
  }
}

// -- prepare -----------------------------------------------------------------

function mergeBuilders(base, extra) {
  const byHandle = new Map();
  for (const builder of base) {
    byHandle.set(String(builder.handle).toLowerCase(), {
      ...builder,
      tweets: [...(builder.tweets || [])],
    });
  }
  for (const builder of extra) {
    const key = String(builder.handle).toLowerCase();
    const current = byHandle.get(key);
    if (!current) {
      byHandle.set(key, { ...builder, tweets: [...(builder.tweets || [])] });
      continue;
    }
    const ids = new Set(current.tweets.map((t) => t.id));
    for (const tweet of builder.tweets || []) {
      if (!ids.has(tweet.id)) {
        current.tweets.push(tweet);
        ids.add(tweet.id);
      }
    }
    if (!current.bio && builder.bio) current.bio = builder.bio;
    if (!current.name && builder.name) current.name = builder.name;
  }
  return [...byHandle.values()];
}

async function offloadTranscripts(podcasts) {
  const dir = join(dataDir(), 'tmp', 'transcripts');
  let ensured = false;
  const out = [];
  for (const episode of podcasts) {
    const transcript = typeof episode.transcript === 'string' ? episode.transcript : null;
    if (transcript && transcript.length > TRANSCRIPT_INLINE_MAX) {
      if (!ensured) {
        await mkdir(dir, { recursive: true });
        ensured = true;
      }
      const guid = (episode.guid || episode.url || String(out.length))
        .replace(/[^a-zA-Z0-9._-]/g, '_')
        .slice(0, 120);
      const path = join(dir, `${guid}.txt`);
      await writeFile(path, transcript);
      const { transcript: _drop, ...rest } = episode;
      out.push({
        ...rest,
        transcriptPath: path,
        transcriptChars: transcript.length,
        transcriptHead: transcript.slice(0, TRANSCRIPT_HEAD_SNIPPET),
      });
    } else {
      out.push(episode);
    }
  }
  return out;
}

async function cmdPrepare(cfg) {
  const prompts = loadPrompts();
  const errors = [];

  let config = { language: 'zh', frequency: 'daily', delivery: { method: 'stdout' } };
  const configPath = join(dataDir(), 'config.json');
  if (existsSync(configPath)) {
    try {
      config = { ...config, ...JSON.parse(await readFile(configPath, 'utf-8')) };
    } catch (err) {
      errors.push(`Could not read config.json: ${err.message}`);
    }
  }

  const [feedX, feedPodcasts, feedBlogs] = await Promise.all([
    fetchCentralJSON(CENTRAL_FEEDS.x).catch((err) => {
      errors.push(`Could not fetch tweet feed: ${err.message}`);
      return null;
    }),
    fetchCentralJSON(CENTRAL_FEEDS.podcasts).catch((err) => {
      errors.push(`Could not fetch podcast feed: ${err.message}`);
      return null;
    }),
    fetchCentralJSON(CENTRAL_FEEDS.blogs).catch((err) => {
      errors.push(`Could not fetch blog feed: ${err.message}`);
      return null;
    }),
  ]);

  const centralOk = [feedX, feedPodcasts, feedBlogs].filter(Boolean).length;

  let custom = { builders: 0, tweets: 0, path: null };
  const customPath = cfg.get('AI_DAILY_CUSTOM_FEED_PATH') || join(dataDir(), 'custom-feed.json');
  let customBuilders = [];
  if (existsSync(customPath)) {
    try {
      const customFeed = JSON.parse(await readFile(customPath, 'utf-8'));
      if (!Array.isArray(customFeed.x)) throw new Error('missing x[] array');
      customBuilders = customFeed.x;
      custom = {
        builders: customFeed.stats?.xBuilders ?? customFeed.x.length,
        tweets: customFeed.stats?.totalTweets ??
          customFeed.x.reduce((sum, b) => sum + (b.tweets || []).length, 0),
        path: customPath,
      };
    } catch (err) {
      errors.push(`Could not read custom feed ${customPath}: ${err.message}`);
    }
  } else {
    errors.push(`Custom feed missing at ${customPath} (collect may have failed); continuing without it`);
  }

  const x = mergeBuilders(feedX?.x || [], customBuilders);
  const podcasts = await offloadTranscripts(feedPodcasts?.podcasts || []);
  const blogs = feedBlogs?.blogs || [];

  const stats = {
    podcastEpisodes: podcasts.length,
    xBuilders: x.length,
    totalTweets: x.reduce((sum, b) => sum + (b.tweets || []).length, 0),
    blogPosts: blogs.length,
    feedGeneratedAt:
      feedX?.generatedAt || feedPodcasts?.generatedAt || feedBlogs?.generatedAt || null,
  };
  const totalItems = stats.podcastEpisodes + stats.totalTweets + stats.blogPosts;

  if (centralOk === 0 && custom.tweets === 0) {
    console.log(JSON.stringify({
      status: 'error',
      message: 'All central feeds failed and no custom tweets were collected',
      errors,
    }, null, 2));
    process.exitCode = 1;
    return;
  }

  const output = {
    status: 'ok',
    generatedAt: new Date().toISOString(),
    config: {
      language: config.language || 'zh',
      frequency: config.frequency || 'daily',
      delivery: config.delivery || { method: 'stdout' },
    },
    podcasts,
    x,
    blogs,
    stats,
    prompts,
    errors: errors.length > 0 ? errors : undefined,
  };

  const tmpDir = join(dataDir(), 'tmp');
  await mkdir(tmpDir, { recursive: true });
  const feedPath = join(tmpDir, 'feed.json');
  await writeFile(feedPath, JSON.stringify(output, null, 2));

  const snapshot = { ...output };
  delete snapshot.prompts;
  const snapshotPath = join(dataDir(), 'last-feed-snapshot.json');
  await writeFile(snapshotPath, JSON.stringify(snapshot, null, 2));

  console.log(JSON.stringify({
    status: 'ok',
    feedPath,
    snapshotPath,
    stats,
    centralFeedsOk: centralOk,
    customFeed: custom,
    totalItems,
    errors: errors.length > 0 ? errors : undefined,
  }, null, 2));
}

// -- validate ----------------------------------------------------------------

async function cmdValidate(cfg, args) {
  try {
    const digestPath = arg(args, '--digest') || join(dataDir(), 'last-digest.txt');
    const snapshotPath = cfg.get('AI_DAILY_SNAPSHOT_PATH') ||
      join(dataDir(), 'last-feed-snapshot.json');

    if (!existsSync(snapshotPath)) {
      console.error(`[validate] snapshot not found: ${snapshotPath} — skipping`);
      return;
    }
    if (!existsSync(digestPath)) {
      console.error(`[validate] digest not found: ${digestPath} — skipping`);
      return;
    }

    const snapshot = JSON.parse(await readFile(snapshotPath, 'utf-8'));
    const digest = await readFile(digestPath, 'utf-8');

    const tweets = [];
    for (const builder of snapshot.x || []) {
      for (const tweet of builder.tweets || []) {
        if (tweet.url) tweets.push(tweet.url);
      }
    }
    const podcasts = (snapshot.podcasts || []).map((p) => p.url).filter(Boolean);
    const blogs = (snapshot.blogs || []).map((b) => b.url).filter(Boolean);
    const missing = (urls) => urls.filter((u) => !digest.includes(u));

    const report = (label, urls) => {
      if (urls.length === 0) return;
      console.error(`[validate] missing ${label} URLs (${urls.length}):`);
      for (const url of urls) console.error(`  ${url}`);
    };

    const mt = missing(tweets);
    const mp = missing(podcasts);
    const mb = missing(blogs);
    console.error(`[validate] feed: ${tweets.length} tweets, ${podcasts.length} podcasts, ${blogs.length} blogs`);
    console.error(`[validate] present: ${tweets.length - mt.length}, ${podcasts.length - mp.length}, ${blogs.length - mb.length}`);
    report('tweet', mt);
    report('podcast', mp);
    report('blog', mb);
    if (mt.length + mp.length + mb.length === 0) {
      console.error('[validate] all feed URLs are present');
    } else {
      console.error('[validate] review missing URLs; intentional exclusions are acceptable');
    }
  } catch (err) {
    console.error(`[validate] error: ${err.message} — continuing`);
  }
}

// -- docs --------------------------------------------------------------------

function shanghaiDate(date = new Date()) {
  return new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Asia/Shanghai',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).format(date);
}

function docConfig(cfg) {
  const workspaceId = cfg.get('DINGTALK_WORKSPACE_ID');
  return {
    workspaceId,
    digest: {
      key: 'digest',
      workspaceId,
      folderId: cfg.get('DINGTALK_DIGEST_FOLDER_ID'),
      name: cfg.get('DINGTALK_DIGEST_DOC_NAME') || shanghaiDate(),
    },
    archive: {
      key: 'archive',
      workspaceId,
      folderId: cfg.get('DINGTALK_ARCHIVE_FOLDER_ID'),
      name: cfg.get('DINGTALK_ARCHIVE_DOC_NAME') || 'AI-DAILY-CUSTOM-FEED',
    },
  };
}

function authHeaders(accessToken) {
  return { 'x-acs-dingtalk-access-token': accessToken };
}

async function resolveOperatorId(cfg, accessToken, state) {
  const fixed = cfg.get('DINGTALK_OPERATOR_UNION_ID');
  if (fixed) return fixed;
  if (state.oauth.unionId) return state.oauth.unionId;

  const res = await curlJSON('GET', `${DINGTALK_API}/v1.0/contact/users/me`, {
    headers: authHeaders(accessToken),
  });
  const data = parseJSONSafe(res.body) || {};
  if (!res.ok || !data.unionId) {
    throw new Error(
      `get unionId failed (HTTP ${res.status}): ` +
      `${data.message || data.code || res.body.slice(0, 200)}; ` +
      'grant Contact.User.Read or set DINGTALK_OPERATOR_UNION_ID'
    );
  }
  state.oauth.unionId = data.unionId;
  return data.unionId;
}

function nodeToDoc(node) {
  const nodeId = node.nodeId || node.dentryUuid || node.id;
  return {
    name: node.name,
    nodeId,
    docKey: node.docKey || nodeId,
    url: node.url || node.docUrl || `https://alidocs.dingtalk.com/i/nodes/${nodeId}`,
  };
}

async function listFolderNodes(target, accessToken, operatorId) {
  const nodes = [];
  let nextToken = '';
  for (let page = 0; page < 20; page += 1) {
    const params = new URLSearchParams({
      parentNodeId: target.folderId,
      operatorId,
      maxResults: '50',
    });
    if (nextToken) params.set('nextToken', nextToken);
    const res = await curlJSON('GET', `${DINGTALK_API}/v2.0/wiki/nodes?${params}`, {
      headers: authHeaders(accessToken),
    });
    const data = parseJSONSafe(res.body) || {};
    if (!res.ok) {
      throw new Error(
        `list folder failed (HTTP ${res.status}): ` +
        `${data.message || data.code || res.body.slice(0, 200)}`
      );
    }
    nodes.push(...(data.nodes || []));
    nextToken = data.nextToken || '';
    if (!nextToken) break;
  }
  return nodes;
}

async function createDoc(target, accessToken, operatorId) {
  const res = await curlJSON(
    'POST',
    `${DINGTALK_API}/v1.0/doc/workspaces/${encodeURIComponent(target.workspaceId)}/docs`,
    {
      headers: authHeaders(accessToken),
      body: JSON.stringify({
        name: target.name,
        docType: 'DOC',
        operatorId,
        parentNodeId: target.folderId,
      }),
    }
  );
  const data = parseJSONSafe(res.body) || {};
  if (!res.ok || data.success === false) {
    const message = data.message || data.code || res.body.slice(0, 300);
    const err = new Error(`create doc failed (HTTP ${res.status}): ${message}`);
    err.status = res.status;
    err.code = data.code || '';
    throw err;
  }
  return data;
}

async function ensureDoc(target, accessToken, operatorId, state) {
  if (!target.workspaceId) throw new Error('DINGTALK_WORKSPACE_ID is missing');
  if (!target.folderId) throw new Error(`DingTalk folder id missing for ${target.key}`);

  state.docs = state.docs || {};
  const cached = state.docs[target.key];
  if (cached && cached.name === target.name && cached.folderId === target.folderId) {
    return { ...cached, created: false };
  }

  const remember = (doc, created) => {
    const info = { ...doc, name: target.name, folderId: target.folderId, createdAt: new Date().toISOString() };
    state.docs[target.key] = info;
    return { ...info, created };
  };

  let nodes = await listFolderNodes(target, accessToken, operatorId);
  const found = nodes.find((node) => node.name === target.name);
  if (found) return remember(nodeToDoc(found), false);

  try {
    const data = await createDoc(target, accessToken, operatorId);
    const nodeId = data.nodeId || data.dentryUuid;
    if (!nodeId) throw new Error('create doc returned no nodeId');
    return remember({
      nodeId,
      docKey: data.docKey || nodeId,
      url: data.url || `https://alidocs.dingtalk.com/i/nodes/${nodeId}`,
    }, true);
  } catch (err) {
    const conflict = /nameConflict|名称冲突/i.test(`${err.code} ${err.message}`);
    if (!conflict) throw err;
    nodes = await listFolderNodes(target, accessToken, operatorId);
    const existing = nodes.find((node) => node.name === target.name);
    if (!existing) throw err;
    return remember(nodeToDoc(existing), false);
  }
}

async function overwriteDoc(docKey, content, accessToken) {
  const res = await curlJSON(
    'POST',
    `${DINGTALK_API}/v2.0/doc/me/suites/documents/${encodeURIComponent(docKey)}/overwriteContent`,
    {
      headers: authHeaders(accessToken),
      body: JSON.stringify({ dataType: 'markdown', content }),
    }
  );
  const data = parseJSONSafe(res.body) || {};
  if (!res.ok || data.success === false) {
    throw new Error(
      `overwriteContent failed (HTTP ${res.status}): ` +
      `${data.message || data.code || res.body.slice(0, 300)}`
    );
  }
  return data;
}

function compactFeed(feed, maxText) {
  const x = (feed.x || []).map((builder) => ({
    source: builder.source,
    name: builder.name,
    handle: builder.handle,
    bio: builder.bio,
    tweets: (builder.tweets || []).map((tweet) => ({
      ...tweet,
      text: maxText ? String(tweet.text || '').slice(0, maxText) : tweet.text,
    })),
  }));
  return {
    generatedAt: feed.generatedAt,
    lookbackHours: feed.lookbackHours,
    x,
    stats: feed.stats,
    errors: feed.errors,
  };
}

function buildFeedDoc(feed) {
  const variants = [
    { feed, wrap: false },
    { feed, wrap: false, minify: true },
    { feed: compactFeed(feed, 1500), minify: true },
    { feed: compactFeed(feed, 500), minify: true, dropErrors: true },
  ];

  for (const variant of variants) {
    let payload = variant.feed;
    if (variant.dropErrors) payload = { ...payload, errors: undefined };
    const json = variant.minify ? JSON.stringify(payload) : JSON.stringify(payload, null, 2);
    const base64 = Buffer.from(json, 'utf-8').toString('base64');
    const wrapped = base64.match(/.{1,76}/g)?.join('\n') || base64;
    const sha256 = createHash('sha256').update(json).digest('hex');
    const content = [
      '# 健康 AI Daily — 定制采集归档',
      '',
      `生成时间：${feed.generatedAt || 'unknown'}`,
      `账号：${feed.stats?.xBuilders ?? 0}；推文：${feed.stats?.totalTweets ?? 0}`,
      `sha256：${sha256}`,
      '',
      '以下为机器读取区，请勿编辑。',
      '',
      FEED_BEGIN,
      '```',
      wrapped,
      '```',
      FEED_END,
      '',
    ].join('\n');

    if (Buffer.byteLength(content, 'utf-8') <= MAX_DOC_CHARS) {
      return { content, sha256, jsonBytes: Buffer.byteLength(json, 'utf-8'), compacted: variant !== variants[0] };
    }
  }

  throw new Error(
    `Custom feed is too large to archive in a DingTalk doc (limit ${MAX_DOC_CHARS} chars)`
  );
}

async function publishFeedBuilt(target, built, accessToken, operatorId, state, warnings) {
  if (!built) return { published: false, reason: 'no feed data' };
  if (!target.folderId) {
    warnings.push('DINGTALK_ARCHIVE_FOLDER_ID is not set; custom feed was not archived');
    return { published: false, reason: 'no archive folder configured' };
  }
  try {
    const doc = await ensureDoc(target, accessToken, operatorId, state);
    await overwriteDoc(doc.docKey, built.content, accessToken);
    return {
      published: true,
      created: doc.created,
      docKey: doc.docKey,
      url: doc.url,
      chars: built.content.length,
      sha256: built.sha256,
      compacted: built.compacted,
    };
  } catch (err) {
    warnings.push(`custom feed publish failed: ${err.message}`);
    return { published: false, reason: err.message };
  }
}

async function cmdPublishAll(cfg, args) {
  const dryRun = args.includes('--dry-run');
  const digestPath = arg(args, '--digest') || join(dataDir(), 'last-digest.txt');
  const feedPath = arg(args, '--feed') || join(dataDir(), 'custom-feed.json');
  const warnings = [];

  if (!existsSync(digestPath)) throw new Error(`digest not found: ${digestPath}`);
  const digest = await readFile(digestPath, 'utf-8');
  if (!digest.trim()) throw new Error(`digest is empty: ${digestPath}`);
  if (digest.length > MAX_DOC_CHARS) {
    throw new Error(`digest too long: ${digest.length} chars > ${MAX_DOC_CHARS}; condense it first`);
  }

  const docs = docConfig(cfg);
  if (!docs.workspaceId) throw new Error('DINGTALK_WORKSPACE_ID is missing');
  if (!docs.digest.folderId) throw new Error('DINGTALK_DIGEST_FOLDER_ID is missing');
  if (
    docs.archive.folderId === docs.digest.folderId &&
    docs.archive.name === docs.digest.name
  ) {
    throw new Error('archive and digest targets point to the same document');
  }

  let feed = null;
  if (existsSync(feedPath)) {
    try {
      feed = JSON.parse(await readFile(feedPath, 'utf-8'));
    } catch (err) {
      warnings.push(`custom feed is not valid JSON: ${err.message}`);
    }
  } else {
    warnings.push(`custom feed not found: ${feedPath}`);
  }

  let feedBuilt = null;
  if (feed) {
    try {
      feedBuilt = buildFeedDoc(feed);
    } catch (err) {
      warnings.push(`custom feed archive skipped: ${err.message}`);
    }
  }

  if (dryRun) {
    console.log(JSON.stringify({
      status: 'ok',
      dryRun: true,
      digestTarget: {
        workspaceId: docs.workspaceId,
        folderId: docs.digest.folderId,
        name: docs.digest.name,
      },
      digestChars: digest.length,
      archiveTarget: docs.archive.folderId
        ? {
            workspaceId: docs.archive.workspaceId,
            folderId: docs.archive.folderId,
            name: docs.archive.name,
          }
        : null,
      feedChars: feedBuilt?.content.length || 0,
      feedCompacted: feedBuilt?.compacted || false,
      warnings: warnings.length ? warnings : undefined,
    }, null, 2));
    return;
  }

  const backend = stateBackend(cfg);
  if (backend.type === 'none') {
    throw new Error(
      'No persistent state backend: personal OAuth refresh tokens rotate on every ' +
      'refresh, so cloud runs require OSS_STATE_* or AI_DAILY_STATE_URL.'
    );
  }
  if (backend.type === 'invalid') throw new Error(backend.reason);

  const state = await loadState(cfg, backend);
  const auth = await acquireUserToken(cfg, backend, state);
  const operatorId = await resolveOperatorId(cfg, auth.accessToken, state);

  const digestDoc = await ensureDoc(docs.digest, auth.accessToken, operatorId, state);
  await overwriteDoc(digestDoc.docKey, digest, auth.accessToken);

  const feedResult = await publishFeedBuilt(
    docs.archive, feedBuilt, auth.accessToken, operatorId, state, warnings
  );

  state.lastRun = {
    at: new Date().toISOString(),
    digestDocKey: digestDoc.docKey,
    digestDocUrl: digestDoc.url,
    digestDocName: docs.digest.name,
    digestChars: digest.length,
    archiveDocKey: feedResult.docKey || null,
    archiveDocUrl: feedResult.url || null,
    feedChars: feedBuilt?.content.length || 0,
    feedPublished: feedResult.published,
    version: VERSION,
  };
  const finalSave = await saveState(cfg, backend, state);
  const statePersisted = auth.statePersisted && finalSave.persisted;

  console.log(JSON.stringify({
    status: 'ok',
    version: VERSION,
    digestDoc: {
      name: docs.digest.name,
      docKey: digestDoc.docKey,
      url: digestDoc.url,
      created: digestDoc.created,
      chars: digest.length,
    },
    archiveDoc: feedResult.published
      ? {
          name: docs.archive.name,
          docKey: feedResult.docKey,
          url: feedResult.url,
          created: feedResult.created,
          chars: feedResult.chars,
        }
      : null,
    feedPublished: feedResult.published,
    authSource: auth.source,
    stateBackend: finalSave.backend,
    statePersisted,
    stateError: auth.stateError || finalSave.error || null,
    warnings: warnings.length ? warnings : undefined,
  }, null, 2));
}

async function cmdPublish(cfg, args) {
  const dryRun = args.includes('--dry-run');
  const which = arg(args, '--doc', 'digest');
  if (!['digest', 'archive', 'feed'].includes(which)) {
    throw new Error('--doc must be digest or archive');
  }
  const filePath = arg(args, '--file');
  if (!filePath) throw new Error('Usage: publish --doc digest|archive --file <path>');
  if (!existsSync(filePath)) throw new Error(`file not found: ${filePath}`);

  const docs = docConfig(cfg);
  const target = which === 'digest' ? docs.digest : docs.archive;
  const name = arg(args, '--name');
  if (name) target.name = name;

  let content;
  if (which === 'digest') {
    content = await readFile(filePath, 'utf-8');
  } else {
    const feed = JSON.parse(await readFile(filePath, 'utf-8'));
    content = buildFeedDoc(feed).content;
  }
  if (!content.trim()) throw new Error('content is empty');
  if (content.length > MAX_DOC_CHARS) {
    throw new Error(`content too long: ${content.length} chars > ${MAX_DOC_CHARS}`);
  }

  if (dryRun) {
    console.log(JSON.stringify({
      status: 'ok',
      dryRun: true,
      doc: which,
      target: { workspaceId: target.workspaceId, folderId: target.folderId, name: target.name },
      chars: content.length,
    }, null, 2));
    return;
  }

  const backend = stateBackend(cfg);
  if (backend.type === 'none' || backend.type === 'invalid') {
    throw new Error(backend.reason || 'No persistent state backend configured');
  }
  const state = await loadState(cfg, backend);
  const auth = await acquireUserToken(cfg, backend, state);
  const operatorId = await resolveOperatorId(cfg, auth.accessToken, state);
  const doc = await ensureDoc(target, auth.accessToken, operatorId, state);
  await overwriteDoc(doc.docKey, content, auth.accessToken);
  const saved = await saveState(cfg, backend, state);
  console.log(JSON.stringify({
    status: 'ok',
    doc: which,
    target: { folderId: target.folderId, name: target.name },
    docKey: doc.docKey,
    url: doc.url,
    created: doc.created,
    chars: content.length,
    statePersisted: auth.statePersisted && saved.persisted,
    stateError: auth.stateError || saved.error || null,
  }, null, 2));
}

async function cmdRenderFeedDoc(args) {
  const file = arg(args, '--file') || join(dataDir(), 'custom-feed.json');
  if (!existsSync(file)) throw new Error(`feed not found: ${file}`);
  const feed = JSON.parse(await readFile(file, 'utf-8'));
  const built = buildFeedDoc(feed);
  console.log(built.content);
  console.error(`[render-feed-doc] chars=${built.content.length} compacted=${built.compacted}`);
}

// -- notify ------------------------------------------------------------------

function webhookSignedUrl(webhookUrl, secret) {
  if (!secret) return webhookUrl;
  const timestamp = Date.now();
  const sign = createHmac('sha256', secret)
    .update(`${timestamp}\n${secret}`)
    .digest('base64');
  const sep = webhookUrl.includes('?') ? '&' : '?';
  return `${webhookUrl}${sep}timestamp=${timestamp}&sign=${encodeURIComponent(sign)}`;
}

async function cmdNotify(cfg, args) {
  let text = arg(args, '--text');
  if (!text) {
    const file = arg(args, '--file');
    if (file) text = await readFile(file, 'utf-8');
  }
  if (!text && args.includes('--stdin')) {
    const chunks = [];
    for await (const chunk of process.stdin) chunks.push(chunk);
    text = Buffer.concat(chunks).toString('utf-8');
  }
  if (!text || !text.trim()) throw new Error('empty notification (use --text, --file, or --stdin)');
  if (text.length > MAX_NOTIFY_CHARS) text = `${text.slice(0, MAX_NOTIFY_CHARS)}\n…(truncated)`;

  const webhookUrl = cfg.get('DINGTALK_WEBHOOK_URL');
  if (!webhookUrl) throw new Error('DINGTALK_WEBHOOK_URL is not set');
  const title = arg(args, '--title', '健康 AI Daily');

  const res = await curlJSON('POST', webhookSignedUrl(webhookUrl, cfg.get('DINGTALK_WEBHOOK_SECRET')), {
    body: JSON.stringify({
      msgtype: 'markdown',
      markdown: { title, text },
    }),
  });
  const data = parseJSONSafe(res.body) || {};
  if (!res.ok || data.errcode !== 0) {
    throw new Error(
      `webhook failed (HTTP ${res.status}, errcode=${data.errcode}): ` +
      `${data.errmsg || res.body.slice(0, 300)}`
    );
  }
  console.log(JSON.stringify({
    status: 'ok', channel: 'webhook', title, chars: text.length, taskId: data.task_id || null,
  }, null, 2));
}

// -- diagnostics -------------------------------------------------------------

async function cmdStateShow(cfg) {
  const backend = stateBackend(cfg);
  if (backend.type === 'none') {
    console.log(JSON.stringify({ backend: 'none' }, null, 2));
    return;
  }
  if (backend.type === 'invalid') throw new Error(backend.reason);
  const state = await loadState(cfg, backend);
  console.log(JSON.stringify({
    backend: backend.type,
    oauth: {
      refreshToken: state.oauth.refreshToken ? '***' : '',
      corpId: state.oauth.corpId,
      unionId: state.oauth.unionId ? '***' : '',
      updatedAt: state.oauth.updatedAt,
    },
    docs: state.docs || {},
    seenTweets: Object.keys(state.collector.seenTweets).length,
    lastRun: state.lastRun,
  }, null, 2));
}

async function cmdDoctor(cfg, args) {
  const scope = arg(args, '--scope', 'all');
  if (!['all', 'collector'].includes(scope)) {
    throw new Error('--scope must be all or collector');
  }
  const fatal = [];
  const warnings = [];
  const checks = { version: VERSION, scope, appDir: APP_DIR };

  if (scope === 'collector') {
    const repo = cfg.get('COLLECTOR_FEED_REPO', 'github.com/cchli2026/mz-daily.git');
    checks.xBearer = Boolean(cfg.get('COLLECTOR_X_BEARER_TOKEN'));
    checks.gitToken = Boolean(cfg.get('COLLECTOR_GIT_TOKEN'));
    checks.feedRepo = repo;
    checks.feedBranch = cfg.get('COLLECTOR_FEED_BRANCH', 'main');
    checks.webhook = Boolean(cfg.get('DINGTALK_WEBHOOK_URL'));
    checks.accounts = 'repo:config/accounts.json';
    try {
      execFileSync('git', ['--version'], { stdio: 'ignore' });
      checks.git = true;
    } catch {
      checks.git = false;
    }

    if (!checks.xBearer) fatal.push('COLLECTOR_X_BEARER_TOKEN missing');
    if (!checks.gitToken && !isLocalRepo(repo)) fatal.push('COLLECTOR_GIT_TOKEN missing');
    if (!checks.webhook) fatal.push('DINGTALK_WEBHOOK_URL missing');
    if (!checks.git) fatal.push('git not available');

    console.log(JSON.stringify({
      status: fatal.length ? 'error' : 'ok',
      ready: fatal.length === 0,
      checks,
      fatal,
      warnings,
    }, null, 2));
    if (fatal.length) process.exitCode = 1;
    return;
  }

  try {
    checks.prompts = Object.keys(loadPrompts()).length;
  } catch (err) {
    fatal.push(err.message);
  }
  try {
    checks.accounts = loadAccounts().length;
  } catch (err) {
    fatal.push(err.message);
  }

  checks.dingtalkClient = Boolean(cfg.get('DINGTALK_CLIENT_ID') && cfg.get('DINGTALK_CLIENT_SECRET'));
  checks.refreshTokenBootstrap = Boolean(cfg.get('DINGTALK_REFRESH_TOKEN'));
  checks.webhook = Boolean(cfg.get('DINGTALK_WEBHOOK_URL'));
  checks.xBearer = Boolean(cfg.get('COLLECTOR_X_BEARER_TOKEN'));

  const docs = docConfig(cfg);
  checks.dingtalkWorkspace = docs.workspaceId || null;
  checks.digestFolderId = docs.digest.folderId || null;
  checks.digestDocName = docs.digest.name;
  checks.archiveFolderId = docs.archive.folderId || null;
  checks.archiveDocName = docs.archive.name;
  checks.operatorUnionId = cfg.get('DINGTALK_OPERATOR_UNION_ID') ? '***' : null;

  const backend = stateBackend(cfg);
  checks.stateBackend = backend.type;

  if (!checks.dingtalkClient) fatal.push('DINGTALK_CLIENT_ID/SECRET missing');
  if (checks.accounts === 0) fatal.push('accounts.json has no accounts');
  if (!docs.workspaceId) fatal.push('DINGTALK_WORKSPACE_ID missing');
  if (!docs.digest.folderId) fatal.push('DINGTALK_DIGEST_FOLDER_ID missing');
  if (!checks.webhook) fatal.push('DINGTALK_WEBHOOK_URL missing');
  if (backend.type === 'none') fatal.push('No persistent state backend (OSS_STATE_* or AI_DAILY_STATE_URL)');
  if (backend.type === 'invalid') fatal.push(backend.reason);
  if (!checks.xBearer) warnings.push('COLLECTOR_X_BEARER_TOKEN missing; custom collection will fail');
  if (!docs.archive.folderId) warnings.push('DINGTALK_ARCHIVE_FOLDER_ID missing; custom feed will not be archived');
  if (!checks.refreshTokenBootstrap) warnings.push('No bootstrap refresh token; relying on the state backend');
  if (!checks.operatorUnionId) warnings.push('DINGTALK_OPERATOR_UNION_ID not set; resolved via Contact.User.Read on first publish');

  console.log(JSON.stringify({
    status: fatal.length ? 'error' : 'ok',
    ready: fatal.length === 0,
    checks,
    fatal,
    warnings,
  }, null, 2));
  if (fatal.length) process.exitCode = 1;
}

// -- CLI ---------------------------------------------------------------------

function arg(args, name, fallback = null) {
  const index = args.indexOf(name);
  return index !== -1 && args[index + 1] !== undefined ? args[index + 1] : fallback;
}

async function main() {
  const [command, ...args] = process.argv.slice(2);
  const cfg = makeConfig();

  switch (command) {
    case 'version':
      console.log(JSON.stringify({ name: 'ai-daily', version: VERSION }, null, 2));
      return;
    case 'doctor':
      await cmdDoctor(cfg, args);
      return;
    case 'collect':
      await cmdCollect(cfg, args);
      return;
    case 'collect-git':
      await cmdCollectGit(cfg);
      return;
    case 'prepare':
      await cmdPrepare(cfg);
      return;
    case 'validate':
      await cmdValidate(cfg, args);
      return;
    case 'publish-all':
      await cmdPublishAll(cfg, args);
      return;
    case 'publish':
      await cmdPublish(cfg, args);
      return;
    case 'render-feed-doc':
      await cmdRenderFeedDoc(args);
      return;
    case 'notify':
      await cmdNotify(cfg, args);
      return;
    case 'state-show':
      await cmdStateShow(cfg);
      return;
    default:
      console.log(JSON.stringify({
        status: 'error',
        message: 'Usage: ai-daily.mjs <version|doctor|collect|collect-git|prepare|validate|publish-all|publish|render-feed-doc|notify|state-show>',
      }));
      process.exitCode = 1;
  }
}

main().catch((err) => {
  console.log(JSON.stringify({ status: 'error', message: err.message }));
  process.exit(1);
});
