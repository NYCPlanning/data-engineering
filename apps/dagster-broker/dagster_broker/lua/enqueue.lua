#!lua
-- No allow-oom flag: when Valkey is at maxmemory this is refused with an OOM error before
-- it runs, so a full store rejects new work cleanly instead of half-writing it.
-- ARGV: prefix, idempotency_key ('' = none), sig, run_id ('' = none), priority,
--       then field/value pairs for the item hash.
local idem, sig, run, prio = ARGV[2], ARGV[3], ARGV[4], tonumber(ARGV[5])
if idem ~= '' then
  local existing = redis.call('GET', P .. 'idem:' .. idem)
  if existing and redis.call('EXISTS', ikey(existing)) == 1 then
    return {0, redis.call('HGETALL', ikey(existing))}
  end
end
if run == '' then run = nil end
local t = now_ms()
local id = redis.call('INCR', P .. 'seq')
local k = ikey(id)
local fields = {}
for i = 6, #ARGV do fields[#fields + 1] = ARGV[i] end
redis.call('HSET', k, 'id', id, 'status', 'queued', 'attempt', 0, 'cancel_requested', 0,
  'enqueued_at', t, 'queued_at', t, 'updated_at', t, unpack(fields))
if idem ~= '' then redis.call('SET', P .. 'idem:' .. idem, id) end
index_queued(id, sig, run, prio, t)
return {1, redis.call('HGETALL', k)}
