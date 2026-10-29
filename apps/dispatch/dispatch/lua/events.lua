#!lua
-- Append events (e.g. Pipes messages) for the current attempt only. No allow-oom: a full
-- store refuses them up front. Over max_events the batch is refused, never trimmed.
-- ARGV: prefix, id, attempt, worker_id, max_events, then kind/data pairs
local id, attempt, worker = ARGV[2], ARGV[3], ARGV[4]
local k = ikey(id)
local bad = fence(k, attempt, worker)
if bad then return {bad} end
local ek = evkey(id)
local n = (#ARGV - 5) / 2
if redis.call('XLEN', ek) + n > tonumber(ARGV[5]) then return {'full'} end
local ids = {}
for i = 6, #ARGV, 2 do
  ids[#ids + 1] = redis.call('XADD', ek, '*', 'attempt', attempt, 'worker_id', worker,
    'kind', ARGV[i], 'data', ARGV[i + 1])
end
return {'ok', ids}
