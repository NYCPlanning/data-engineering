#!lua flags=allow-oom
-- Retryable failures requeue until max_attempts (then dead); non-retryable ones are failed.
-- ARGV: prefix, id, attempt, worker_id, error ('' = none), retryable (1/0), ttl
local id, attempt, worker = ARGV[2], ARGV[3], ARGV[4]
local k = ikey(id)
local bad = fence(k, attempt, worker)
if bad then return {bad} end
local t = now_ms()
local status = after_attempt(id, k)
if status == 'queued' and ARGV[6] ~= '1' then status = 'failed' end
release(id, k)
if ARGV[5] ~= '' then redis.call('HSET', k, 'error', ARGV[5]) else redis.call('HDEL', k, 'error') end
redis.call('HSET', attkey(id), attempt .. ':outcome',
  status == 'cancelled' and 'cancelled' or 'failed')
if status == 'queued' then requeue(id, k, t) else finish(id, k, status, t, tonumber(ARGV[7])) end
return {'ok', redis.call('HGETALL', k)}
