#!lua
-- Append events (e.g. Pipes messages) for the current attempt only.
-- ARGV: prefix, id, attempt, worker_id, then kind/data pairs
local id, attempt, worker = ARGV[2], ARGV[3], ARGV[4]
local k = ikey(id)
local bad = fence(k, attempt, worker)
if bad then return {bad} end
local ids = {}
for i = 5, #ARGV, 2 do
  ids[#ids + 1] = redis.call('XADD', evkey(id), '*', 'attempt', attempt, 'worker_id', worker,
    'kind', ARGV[i], 'data', ARGV[i + 1])
end
return {'ok', ids}
