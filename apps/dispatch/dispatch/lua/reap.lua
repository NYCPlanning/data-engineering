#!lua flags=allow-oom
-- Requeue, kill or cancel items whose lease has expired. Atomic, so it can't interleave
-- with a heartbeat or complete; safe to run from any number of replicas at once.
-- ARGV: prefix, limit, ttl
local t = now_ms()
local out = {}
local expired = redis.call('ZRANGE', LEASES, '-inf', '(' .. t, 'BYSCORE', 'LIMIT', 0, tonumber(ARGV[2]))
for _, m in ipairs(expired) do
  local id = tonumber(m)
  local k = ikey(id)
  local v = redis.call('HMGET', k, 'status', 'lease_until', 'attempt', 'worker_id')
  if v[1] ~= 'claimed' then
    redis.call('ZREM', LEASES, m)  -- stale index entry; nothing to reap
  elseif tonumber(v[2]) < t then
    local status = after_attempt(id, k)
    release(id, k)
    redis.call('HSET', k, 'error', 'lease expired (attempt ' .. v[3] .. ', worker ' .. v[4] .. ')')
    redis.call('HSET', attkey(id), v[3] .. ':outcome', 'expired')
    if status == 'queued' then requeue(id, k, t) else finish(id, k, status, t, tonumber(ARGV[3])) end
    out[#out + 1] = {id, tonumber(v[3]), status}
  end
end
return out
