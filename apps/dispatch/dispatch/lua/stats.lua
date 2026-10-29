#!lua flags=no-writes
-- Read-only counts. ARGV: prefix, caps (JSON list, or '' for all), min_age_ms
-- demand = queued items a worker with these caps could claim, queued at least min_age ago.
local filter = nil
if ARGV[2] ~= '' then
  filter = {}
  for _, c in ipairs(cjson.decode(ARGV[2])) do filter[c] = true end
end
local cutoff = now_ms() - tonumber(ARGV[3])
local queued, demand = 0, 0
for _, sig in ipairs(redis.call('SMEMBERS', SIGS)) do
  local n = redis.call('ZCARD', qkey(sig))
  queued = queued + n
  local ok = true
  if filter then
    for _, c in ipairs(cjson.decode(sig)) do
      if not filter[c] then ok = false; break end
    end
  end
  if ok then demand = demand + redis.call('ZCOUNT', qtkey(sig), '-inf', cutoff) end
end
return {queued, redis.call('ZCARD', LEASES), demand}
