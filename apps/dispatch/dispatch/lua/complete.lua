#!lua flags=allow-oom
-- ARGV: prefix, id, attempt, worker_id, result (JSON), ttl
local id, attempt, worker = ARGV[2], ARGV[3], ARGV[4]
local k = ikey(id)
local bad = fence(k, attempt, worker)
if bad then return {bad} end
local t = now_ms()
release(id, k)
redis.call('HSET', k, 'result', ARGV[5])
redis.call('HDEL', k, 'error')
redis.call('HSET', attkey(id), attempt .. ':outcome', 'done')
finish(id, k, 'done', t, tonumber(ARGV[6]))
return {'ok', redis.call('HGETALL', k)}
