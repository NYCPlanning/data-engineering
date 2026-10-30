#!lua flags=allow-oom
-- ARGV: prefix, id, attempt, worker_id, lease_ms, worker_ttl
local id, attempt, worker = ARGV[2], ARGV[3], ARGV[4]
local k = ikey(id)
local bad = fence(k, attempt, worker)
if bad then return {bad} end
local t = now_ms()
local lease_until = t + tonumber(ARGV[5])
redis.call('HSET', k, 'lease_until', lease_until, 'updated_at', t)
redis.call('ZADD', LEASES, lease_until, pad(id))
touch_worker(worker, nil, t, tonumber(ARGV[6]))
return {'ok', lease_until, redis.call('HGET', k, 'cancel_requested')}
