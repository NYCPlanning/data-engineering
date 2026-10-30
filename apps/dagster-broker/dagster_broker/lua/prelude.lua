-- Shared helpers, spliced into every script after its shebang line (see dagster_broker/store.py).
-- ARGV[1] is always the key prefix. Time is always the server's TIME, never the app's.
-- Standalone Valkey only: scripts compute key names, which cluster mode would forbid.
local P = ARGV[1]

local function now_ms()
  local t = redis.call('TIME')
  return tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
end

-- zset members are zero-padded ids, so equal scores (same priority) sort FIFO
local function pad(id) return string.format('%016d', tonumber(id)) end
local function ikey(id) return P .. 'item:' .. id end
local function attkey(id) return P .. 'att:' .. id end
local function evkey(id) return P .. 'ev:' .. id end
local function qkey(sig) return P .. 'q:' .. sig end
local function qtkey(sig) return P .. 'qt:' .. sig end
local function qrkey(sig, run) return P .. 'qr:' .. redis.sha1hex(sig .. '\0' .. run) end
local LEASES = P .. 'leases'
local SIGS = P .. 'sigs'

local function index_queued(id, sig, run, prio, t)
  local m = pad(id)
  redis.call('ZADD', qkey(sig), -prio, m)
  redis.call('ZADD', qtkey(sig), t, m)
  if run then redis.call('ZADD', qrkey(sig, run), -prio, m) end
  redis.call('SADD', SIGS, sig)
end

local function unindex_queued(id, sig, run)
  local m = pad(id)
  redis.call('ZREM', qkey(sig), m)
  redis.call('ZREM', qtkey(sig), m)
  if run then redis.call('ZREM', qrkey(sig, run), m) end
  if redis.call('ZCARD', qkey(sig)) == 0 then redis.call('SREM', SIGS, sig) end
end

-- the fencing token is (id, attempt, worker_id); only a claimed item can match it
local function fence(k, attempt, worker)
  if redis.call('EXISTS', k) == 0 then return 'noitem' end
  local v = redis.call('HMGET', k, 'status', 'attempt', 'worker_id')
  if v[1] ~= 'claimed' or v[2] ~= attempt or v[3] ~= worker then return 'stale' end
  return nil
end

local function touch_worker(worker, caps_json, t, ttl)
  local wk = P .. 'worker:' .. worker
  if caps_json then redis.call('HSET', wk, 'caps', caps_json) end
  redis.call('HSETNX', wk, 'first_seen', t)
  redis.call('HSET', wk, 'last_seen', t)
  redis.call('EXPIRE', wk, ttl)
end

-- leave the claimed state: drop the lease and the holder
local function release(id, k)
  redis.call('ZREM', LEASES, pad(id))
  redis.call('HDEL', k, 'worker_id', 'lease_until')
end

-- a terminal status: everything about the item now expires after ttl
local function finish(id, k, status, t, ttl)
  redis.call('HSET', k, 'status', status, 'finished_at', t, 'updated_at', t)
  local idem = redis.call('HGET', k, 'idempotency_key')
  redis.call('EXPIRE', k, ttl)
  redis.call('EXPIRE', attkey(id), ttl)
  redis.call('EXPIRE', evkey(id), ttl)
  if idem then redis.call('EXPIRE', P .. 'idem:' .. idem, ttl) end
end

-- back to queued after a failed or expired attempt (keeps its original FIFO position)
local function requeue(id, k, t)
  local v = redis.call('HMGET', k, 'sig', 'run_id', 'priority')
  redis.call('HSET', k, 'status', 'queued', 'queued_at', t, 'updated_at', t)
  index_queued(id, v[1], v[2], tonumber(v[3]), t)
end

-- after attempt n ends without success: queued, dead, or cancelled
local function after_attempt(id, k)
  local v = redis.call('HMGET', k, 'cancel_requested', 'attempt', 'max_attempts')
  if v[1] == '1' then return 'cancelled' end
  if tonumber(v[2]) >= tonumber(v[3]) then return 'dead' end
  return 'queued'
end
