#!lua flags=allow-oom
-- The claim: match + pick + take + lease in one script, so it is atomic by construction.
-- allow-oom: claiming doesn't grow memory, and workers must be able to drain a full store.
-- ARGV: prefix, worker_id, caps (JSON list), affinity run_ids (JSON list), lease_ms, worker_ttl
local worker, caps_json, lease_ms, wttl = ARGV[2], ARGV[3], tonumber(ARGV[5]), tonumber(ARGV[6])
local t = now_ms()

local have = {}
for _, c in ipairs(cjson.decode(caps_json)) do have[c] = true end
local eligible = {}
for _, sig in ipairs(redis.call('SMEMBERS', SIGS)) do
  local ok = true
  for _, c in ipairs(cjson.decode(sig)) do
    if not have[c] then ok = false; break end
  end
  if ok then eligible[#eligible + 1] = sig end
end

-- best = lowest (score, member): highest priority, then oldest enqueue, then id
local best_m, best_s
local function consider(key)
  local r = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
  if r[1] then
    local s = tonumber(r[2])
    if best_m == nil or s < best_s or (s == best_s and r[1] < best_m) then
      best_m, best_s = r[1], s
    end
  end
end
for _, run in ipairs(cjson.decode(ARGV[4])) do
  for _, sig in ipairs(eligible) do consider(qrkey(sig, run)) end
end
if not best_m then
  for _, sig in ipairs(eligible) do consider(qkey(sig)) end
end

touch_worker(worker, caps_json, t, wttl)
if not best_m then return false end

local id = member_id(best_m)
local k = ikey(id)
local v = redis.call('HMGET', k, 'status', 'attempt', 'sig', 'run_id')
if v[1] ~= 'queued' then
  return redis.error_reply('dagster-broker invariant: queued index holds item ' .. id ..
    ' with status ' .. tostring(v[1]))
end
local attempt = tonumber(v[2]) + 1
-- backstop: an (item, attempt) is logged once; a second claim of it fails loudly
if redis.call('HEXISTS', attkey(id), attempt) == 1 then
  return redis.error_reply('dagster-broker invariant: attempt ' .. attempt .. ' of item ' ..
    id .. ' already claimed')
end
unindex_queued(best_m, v[3], v[4])
local lease_until = t + lease_ms
redis.call('HSET', k, 'status', 'claimed', 'attempt', attempt, 'worker_id', worker,
  'lease_until', lease_until, 'claimed_at', t, 'updated_at', t)
redis.call('ZADD', LEASES, lease_until, id)
redis.call('HSET', attkey(id), attempt, worker)
return redis.call('HGETALL', k)
