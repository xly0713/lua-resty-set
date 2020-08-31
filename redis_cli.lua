local redis = require "resty.redis"

local ngx_log = ngx.log
local ngx_ERR = ngx.ERR
local string_find = string.find
local setmetatable = setmetatable

local _M = {
    _VERSION = '0.01'
}

local mt = { __index = _M }

function _M.new(opts)
    local opts = opts or {}
    local self = {
        host = opts.host or "127.0.0.1",
        port = opts.port or 6379,
        timeout = opts.timeout or 5000, --5 second
        auth_passwd = opts.auth_passwd,
        db_index = opts.db_index, --可选(eval执行lua脚本内部处理select命令)
        max_idle_time = opts.max_idle_time or 60000, --60 seconds
        pool_size = opts.pool_size or 100
    }

    return setmetatable(self, mt)
end

function _M.exec(self, func)
    local red = redis.new()
    red:set_timeout(self.timeout)

    local id = self.host .. ":" .. self.port
    local ok, err = red:connect(self.host, self.port)
    if not ok then
        ngx_log(ngx_ERR, "failed to connect: ", id, ", err: ", err)
        return nil, err
    end

    local reused_times, err = red:get_reused_times()
    if err then
        return nil, err
    elseif reused_times == 0 then
        local ok, err = red:auth(self.auth_passwd)
        if err and err ~= "ERR Client sent AUTH, but no password is set" then
            ngx_log(ngx_ERR, "failed to auth: ", id, ", err: ", err)
            return nil, err
        end
    end

    if self.db_index then
        local ok, err = red:select(self.db_index)
        if err then
            ngx_log(ngx_ERR, "failed to select db: ", id, ", err: ", err)
            return nil, err
        end
    end

    local res, err = func(red)
    if res == false and (err and string_find(err, "WRONGTYPE", 1, true)) then
        --ngx_log(ngx_ERR, "wrong type")
        res, err = nil, nil
    end

    if not err then
        local ok, errmsg = red:set_keepalive(self.max_idle_time, self.pool_size)
        if not ok then
            ngx_log(ngx_ERR, "failed to set_keepalive: ", id, ", err: ", errmsg)
            red:close()
        end
    end

    return res, err
end

return _M
