-- Copyright (C) 2016 Pau Carré Cardona - All Rights Reserved
-- You may use, distribute and modify this code under the
-- terms of the Apache License v2.0 (http://www.apache.org/licenses/LICENSE-2.0.txt).

--
-- Reader and writer to store information thanks to redis
--

local torchFolder = require('paths').thisfile('../..')
package.path = string.format("%s;%s/?.lua", os.getenv("LUA_PATH"), torchFolder)

local redis = require 'redis'
local redisClient = nil

local tiefvision_redis_io = {}
function tiefvision_redis_io.read(fileName)
  print("REDIS READ", fileName)
  -- local file = filePath(fileName)
  -- if not paths.filep(file) then
  --   return nil
  -- end
  --
  -- return torch.load(file)
end

function tiefvision_redis_io.write(fileName, data)
  local t0 = os.clock()
  print("WRITE CALLED", os.date("%c"))

  -- ########################################################################### 596.176684 seconds for first element
  -- print("FILE OPEN", os.date("%c"))
  -- local fileName = "/tmp/redis" .. string.sub(math.random(), 3)
  --
  -- print("LOOPING", os.date("%c"))
  -- for i = 1, data:size()[1] do
  --   if i % 1000 == 0 then
  --      print("LOOPING " .. i .. "/" .. data:size()[1], os.date("%c"))
  --   end
  --
  --   local file = io.open(fileName, "w")
  --
  --   for e = 1, data:size()[2] do
  --     local redisProtocol = toRedisProtocol("HSET", i, e, data[i][e])
  --     file:write(redisProtocol)
  --   end
  --
  --   file:close()
  --   os.execute("cat " .. fileName .. " | redis-cli --pipe -h " .. tiefvision_redis_io.host .. " -p " .. tiefvision_redis_io.port .. " 1> /dev/null")
  -- end
  --
  -- os.remove(fileName)

  -- ###########################################################################

  print("LOOPING", os.date("%c"))
  for i = 1, data:size()[1] do
    if i % 1000 == 0 then
       print("LOOPING " .. i .. "/" .. data:size()[1], os.date("%c"))
    end

    tiefvision_redis_io.redisClient:pipeline(function(client)
      client:del(tostring(i))
      client:rpush(tostring(i), data[i])
    end)
  end

  print("WRITE FINISHED", os.date("%c"), os.clock() - t0)
end

function toRedisProtocol(...)
  local args = {...}
  local argsLength = #args

  local redisProtocol = "*" .. argsLength .. "\r\n"
  for i = 1, argsLength do
    local arg = tostring(args[i])

    redisProtocol = redisProtocol .. "$" .. #arg .. "\r\n"
    redisProtocol = redisProtocol .. arg .. "\r\n"
  end

  return redisProtocol
end

local factory = {}
setmetatable(factory, { __call = function(_, host, port)
  tiefvision_redis_io.host = host
  tiefvision_redis_io.port = port or 6379

  tiefvision_redis_io.redisClient = redis.connect(
    tiefvision_redis_io.host,
    tiefvision_redis_io.port
  )

  return tiefvision_redis_io
end })

return factory
