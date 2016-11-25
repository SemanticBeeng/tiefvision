-- Copyright (C) 2016 Pau Carré Cardona - All Rights Reserved
-- You may use, distribute and modify this code under the
-- terms of the Apache License v2.0 (http://www.apache.org/licenses/LICENSE-2.0.txt).

--
-- Default configuration file
--

local torchFolder = require('paths').thisfile('.')
package.path = string.format("%s;%s/?.lua", os.getenv("LUA_PATH"), torchFolder)

local config = {}

config.databaseHost = "tiefvision-001.elzafy.0001.use1.cache.amazonaws.com"
config.database = require("0-tiefvision-commons/io/tiefvision_redis_io")(config.databaseHost)

return config
