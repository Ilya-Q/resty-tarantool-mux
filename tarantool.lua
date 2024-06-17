local mp = require 'MessagePack'
local se = require 'ngx.semaphore'

local M = {}

local t_insert = table.insert
local t_clear = table.clear
if not t_clear then
	t_clear = function(t)
		for i=1,#t do
			t[i] = nil
		end
	end
end

local co_yield = coroutine.yield
local co_status = coroutine.status

-- a few linked list helper functions
local function li_remove(li, node)
	if node.prev then
		node.prev.next = node.next
	else
		li.head = node.next
	end
	if node.next then
		node.next.prev = node.prev
	else
		li.tail = node.prev
	end
end

local function li_append(li, node)
	if li.tail then
		li.tail.next, li.tail, node.prev, node.next = node, node, li.tail, nil
	else
		li.head, li.tail, node.next, node.prev = node, node, nil, nil
	end
end

local function li_prepend(li, node)
	if li.head then
		li.head.prev, li.head, node.next, node.prev = node, node, li.head, nil
	else
		li.head, li.tail, node.next, node.prev = node, node, nil, nil
	end
end

local methods = {}
local req_methods = {}
local rpool_methods = {}
local conn_methods = {}

function req_methods:complete(response)
	assert(self.state == 'taken', "can't complete request in state " .. self.state)
	self.response, self.state, self.err = response, 'done', false
	self.sema:post()
end

function req_methods:fail(err)
	assert(self.state == 'taken', "can't complete request in state " .. self.state)
	self.response, self.state, self.err = err, 'done', true
	self.sema:post()
end

function req_methods:cancel()
	assert(self.state ~= 'free', "can't cancel request in state " .. self.state)
	if self.conn then
		self.conn:ground(self.sync)
	end
	if self.state == 'ready' then
		li_remove(self.pool.reqs.ready, self)
	end
	self.state, self.conn, self.sync, self.body, self.code, self.err = 'free', nil, nil, nil, nil, nil
	li_append(self.pool.reqs.free, self)
	self.pool.sema:post()
end

function req_methods:await(timeout)
	local ok, err = self.sema:wait(timeout)
	local req_err, body = self.err, self.response
	self:cancel()
	if not ok then
		if err == 'timeout' then
			error("request timed out")
		else
			error("req semaphore error: " .. err)
		end
	end
	if not req_err then
		return body
	else
		-- TODO: second arg?
		error("tarantool error: " .. body)
	end
end

function rpool_methods:take(conn)
	local ret = self.reqs.ready.tail
	if ret then
		li_remove(self.reqs.ready, ret)
		ret.state, ret.conn = 'taken', conn
	end
	return ret
end

function rpool_methods:put(code, body, timeout)
--	ngx.log(ngx.INFO, 'putting req')
	local ok, err = self.sema:wait(timeout)
	if not ok then
		error("could not wait for a free request: " .. err)
	end
	local ret = assert(self.reqs.free.tail, "rpool free semaphore desync")
	li_remove(self.reqs.free, ret)
	ret.code, ret.body = code, body
	li_append(self.reqs.ready, ret)
--	ngx.log(ngx.INFO, 'put req, self.reqs.ready.tail = ', self.reqs.ready.tail)
	return ret
end

local function new_rpool(size)
	assert(type(size) == 'number' and size > 0 and size <= 0xFFFFFFFF, "invalid request pool size")
	local pool = {
		sema = se:new(),
		size = size,
		reqs = {
			free = {head = nil, tail = nil},
			ready = {head = nil, tail = nil},
		},
	}
	for i=1, size do
		local cur = {
			sema = se:new(),
			-- TODO: guard
			pool = pool,
			conn = nil,
			sync = nil,
			code = nil,
			body = nil,
			err = nil,
			response = nil,
			state = 'free',
		}
		for name, method in pairs(req_methods) do
			cur[name] = method
		end
		li_append(pool.reqs.free, cur)
	end
	for name, method in pairs(rpool_methods) do
		pool[name] = method
	end
	pool.sema:post(size)
	return pool
end

local function write(conn)
	local p, s = conn.pool, conn.sock
	local buf = {}
	while true do
		local req = p:take(conn)
		while req do
--			ngx.log(ngx.INFO, "writing req")
			local sync = conn.next_sync
			conn.inflight[sync] = req
			-- TODO: un-leak abstraction
			req.sync = sync
			-- TODO: schema
			while conn.inflight[conn.next_sync] or conn.grounded[conn.next_sync] do
				-- TODO: limit the number of iterations
				conn.next_sync = (conn.next_sync + 1) % 0xFFFFFFFF
			end
			local header = mp.pack({ [0x00] = req.code, [0x01] = sync })
			-- TODO: reuse table
			t_insert(buf, { mp.pack(#header + #req.body), header, req.body })
			req = p:take(conn)
		end
		if #buf > 0 then
			ngx.log(ngx.INFO, string.format("sending %s requests", #buf))
			assert(s:send(buf))
			t_clear(buf)
		end
		ngx.sleep(conn.collect_interval)
	end
end


-- reading n bytes from a socket seems like it should be easy
-- but because OpenResty won't let you disable timeouts
-- it's actually surprisingly annoying due to possible partial reads
local function read_n(sock, n, prefix)
	if n == 0 then
		return prefix or ''
	end
	local data, err, partial = sock:receive(n)
	if not data then
		if err ~= 'timeout' then
			error("socket read error: " .. err)
		end
		if prefix then
			prefix = prefix .. partial
		end
		return read_n(sock, n - #partial, prefix and (#prefix > 0 and prefix or nil))
	end
	if prefix then
		return prefix .. data
	else
		return data
	end
end

local function read(conn)
	local p, s = conn.pool, conn.sock
	while true do
		local bu
		local len = mp.unpack(read_n(s, 5))
		local resp = read_n(s, len)
		local it = mp.unpacker(resp)

		local _, header = it()
		local code, sync, schema_id = header[0x00], header[0x01], header[0x05]
		
		local req = conn.inflight[sync]
		if req then
			local _, body = it()
			if code == 0 then
				req:complete(body[0x30])
			else
				-- TODO: preserve error code?
				req:fail(body[0x31])
			end				
		elseif conn.grounded[sync] then
			conn.grounded[sync] = nil
			ngx.log(ngx.WARN, ("Response for grounded request with sync number %s received from %s"):format(sync, conn.name))
		else
			ngx.log(ngx.WARN, ("Stray response from %s with sync number %s, unsafe to proceed"):format(conn.name, sync))
			error("stray response")
		end
	end
end

function conn_methods:_connect()
	if self.state ~= 'idle' then
		return false, "not idle"
	end
	self.state = 'connecting'
	local ok, err = self.sock:connect(self.host, self.port)
	if not ok then
		return false, "socket connection error: " .. err
	end
	local greeting, err = self.sock:receive(128)
	if not greeting then
		return false, "error reading greeting: " .. err
	end
	-- TODO: auth
	ngx.log(ngx.INFO, ("connected to tarantool %s at %s:%s, greeting: %s"):format(self.name, self.host, self.port, greeting:sub(1, 63)))
	self.reader, self.writer = ngx.thread.spawn(read, self), ngx.thread.spawn(write, self)
	self.state = 'connected'
	return true
end

function conn_methods:_cleanup()
	if self.state ~= 'connected' then
		return false, "not connected"
	end
	self.state = 'cleanup'
	if self.reader and co_status(self.reader) ~= 'dead' then
		local ok, err = ngx.thread.kill(self.reader)
		if not ok then
			return false, "reader kill error: " .. err
		end
	end
	if self.writer and co_status(self.writer) ~= 'dead' then
		local ok, err = ngx.thread.kill(self.writer)
		if not ok then
			return false, "writer kill error: " .. err
		end
	end
	local ok, err = self.sock:close()
	if not ok then
		return false, "socket close error: " .. err
	end
	for sync, req in pairs(self.inflight) do
		req:fail("connection died")
		ngx.log(ngx.INFO, ("failed in-flight request %s during cleanup"):format(sync))
	end
	self.next_sync = 0
	self.state = 'idle'
	self.grounded = {}
	return true
end

function conn_methods:ground(sync)
	local req = self.inflight[sync]
	if not req then
		return false, "no in-flight request with sync=" .. sync
	end
	self.grounded[sync] = true
	self.inflight[sync] = nil
	return true
end

local function monitor(premature, conn)
	conn.sock = ngx.socket.tcp()
	while conn.state ~= 'dead' do
		local ok, err = conn:_connect()
		if ok then
			local ok, err = ngx.thread.wait(conn.reader, conn.writer)
			if not ok then
				ngx.log(ngx.ERR, ("tarantool %s at %s:%s: reader/writer error: %s"):format(conn.name, conn.host, conn.port, err))
			else
				ngx.log(ngx.ERR, ("tarantool %s at %s:%s: reader/writer stopped prematurely"):format(conn.name, conn.host, conn.port, err))
			end
			local ok, err = conn:_cleanup()
			if not ok then
				-- if the cleanup fails, the connection is too screwed to recover
				error(("couldn't cleanup connection for tarantool %s at %s:%s"):format(conn.name, conn.host, conn.port))
			end
		else
			ngx.log(ngx.ERR, ("failed to connect to tarantool %s at %s:%s: %s"):format(conn.name, conn.host, conn.port, err))
		end
		ngx.sleep(conn.reconnect_interval)
	end
end

local function new_conn(pool, opts)
	local conn = {
		name = assert(opts.name, "name required"),
		host = assert(opts.host, "host required"),
		port = assert(opts.port, "port required"),
		reconnect_interval = opts.reconnect_interval or 1,
		collect_interval = opts.collect_interval or 0.01,
		pool = pool,
		state = 'idle',
		next_sync = 0,
		inflight = {},
		grounded = {},
	}
	for name, method in pairs(conn_methods) do
		conn[name] = method
	end
	ngx.timer.at(0, monitor, conn)
	return conn
end


function methods:_request(code, body, opts)
	local timeout = opts.timeout or self.timeout
	local deadline = ngx.time() + timeout
	local req = self.pool:put(code, body, timeout)
	return req:await(deadline - ngx.time())
end

function methods:call(name, opts, ...)
	assert(type(name) == 'string', 'function name required for call')
	local ret = self:_request(0x0A, mp.pack({ [0x22] = name, [0x21] = {...} }), opts)
	return unpack(ret)
end

local function register_client(t, opts)
	local pool = new_rpool(opts.concurrency or 1000)
	local conn = new_conn(pool, opts)
	local client = { pool = pool, conn = conn, timeout = opts.timeout or 5 }
	for name, method in pairs(methods) do
		client[name] = method
	end
	return rawset(t, opts.name, client)
end

return setmetatable({}, { __call = register_client, __newindex = function() error("Attempt to modify the 'tarantool' table", 2) end })
