#!lua flags=allow-oom
-- Cancel a queued item now; ask the holder of a claimed one to stop (it sees cancel on its
-- next heartbeat). ARGV: prefix, id, ttl
local id = ARGV[2]
local k = ikey(id)
local v = redis.call('HMGET', k, 'status', 'sig', 'run_id', 'enqueued_at')
if not v[1] then return {'noitem'} end
if v[1] == 'queued' then
  unindex_queued(qmember(id, v[4]), v[2], v[3])
  redis.call('HSET', k, 'cancel_requested', 1)
  finish(id, k, 'cancelled', now_ms(), tonumber(ARGV[3]))
elseif v[1] == 'claimed' then
  redis.call('HSET', k, 'cancel_requested', 1, 'updated_at', now_ms())
else
  return {'terminal', redis.call('HGETALL', k)}
end
return {'ok', redis.call('HGETALL', k)}
