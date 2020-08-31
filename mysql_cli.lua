local mysql = require "resty.mysql"

local ngx_log = ngx.log
local ngx_ERR = ngx.ERR

local ipairs = ipairs

local _M = {
    _VERSION = "0.01"
}

local mt = { __index = _M }

function _M.new(opts)
    local opts = opts or {}
    local self = {
        host = opts.host or "127.0.0.1",
        port = opts.timeout or 3306,
        user = opts.user,            --必传
        password = opts.password,    --必传
        timeout = opts.timeout or 5000,
        max_idle_time = opts.max_idle_time or 60000,
        pool_size = opts.pool_size or 100,
    }

    return setmetatable(self, mt)
end

function _M.exec(self, dbname, str_sql)
    local db, err = mysql:new()
    if not db then
        ngx_log(ngx_ERR, "failed to instantiate mysql: ", err)
        return nil, err
    end

    db:set_timeout(self.timeout)

    local ok, err, errcode, sqlstate = db:connect{
        host = self.host,
        port = self.port,
        user = self.user,
        password = self.password,
        database = dbname,
        charset = "utf8mb4",
        max_packet_size = 5242880,  -- 5M
    }

    if not ok then
        ngx_log(ngx_ERR, "failed to connect: ", err, ": ", errcode, " ", sqlstate)
        return nil, err
    end

    --ngx_log(ngx_ERR, "connected to mysql.")
    local res, err, errcode, sqlstate = db:query(str_sql)
    if err then
        ngx_log(ngx_ERR, "failed to query: ", err, ": ", errcode, " ", sqlstate, ", sql: ", str_sql)
        return nil, err
    end

    local ok, err = db:set_keepalive(self.max_idle_time, self.pool_size)
    if not ok then
        db:close()
    end

    return res, nil
end

function _M.exec_transaction(self, dbname, arr_str_sql)
    local db, err = mysql:new()
    if not db then
        ngx_log(ngx_ERR, "failed to instantiate mysql: ", err)
        return nil, err
    end

    db:set_timeout(self.timeout)

    local ok, err, errcode, sqlstate = db:connect{
        host = self.host,
        port = self.port,
        user = self.user,
        password = self.password,
        database = dbname,
        charset = "utf8mb4",
        max_packet_size = 5242880,  -- 5M
    }

    if not ok then
        ngx_log(ngx_ERR, "failed to connect: ", err, ": ", errcode, " ", sqlstate)
        return nil, err
    end

    local res, err, errcode, sqlstate = db:query("START TRANSACTION")  --开始事务
    if err then
        ngx_log(ngx_ERR, "failed to 'START TRANSACTION': ", err, ": ", errcode, " ", sqlstate)
        return nil, err
    end

    local result = {}
    for _, str_sql in ipairs(arr_str_sql) do
        res, err, errcode, sqlstate = db:query(str_sql)
        if err then
            ngx_log(ngx_ERR, "ROLLBACK, err: ", err, ", str_sql: ", str_sql)

            db:query("ROLLBACK")  --回滚事务
            return nil, err
        end

        result[#result + 1] = res
    end

    res, err, errcode, sqlstate = db:query("COMMIT")  --提交事务
    if err then
        ngx_log(ngx_ERR, "failed to 'COMMIT': ", err, ": ", errcode, " ", sqlstate)
        return nil, err
    end

    local ok, err = db:set_keepalive(self.max_idle_time, self.pool_size)
    if not ok then
        db:close()
    end

    return result
end

return _M
