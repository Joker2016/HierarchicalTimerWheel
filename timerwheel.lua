local TIMERWHEELNUM = 10
local TIMERSLOTNUM = 512

local HOUR = 1
local MINUTE = 2
local SECONDS = 3
local FRAME = 4

local HOURCOUNT = 24
local MINUTECOUNT = 60
local SECONDSCOUNT = 60
local FRAMECOUNT = 100

local CANCELED = 1
local READY = 2
local FOREVER = -1

local utime = require "usertime"

local TICK = function() 
    return utime.getmillisecond() / 1000
end

local TIME_ELAPSED = 1

local function get_seconds(hour, minute, seconds, frame)
    return hour * SECONDSCOUNT * MINUTECOUNT  + minute * SECONDSCOUNT + seconds + frame / FRAMECOUNT
end

local function to_seconds(src, src_unit)
    if src_unit == SECONDS then
        return src
    elseif src_unit == MINUTE then
        return src * SECONDSCOUNT
    elseif src_unit == HOUR then
        return src * SECONDSCOUNT * MINUTECOUNT
    elseif src_unit == FRAME then
        return src / FRAMECOUNT
    end
end

local function seconds_to(src, dst_unit)
    if dst_unit == SECONDS then
        return src
    elseif dst_unit == MINUTE then
        return src / SECONDSCOUNT
    elseif dst_unit == HOUR then
        return src / SECONDSCOUNT / MINUTECOUNT
    elseif dst_unit == FRAME then
        return src * FRAMECOUNT
    end    
end

local function to_time(src, src_unit, dst_unit)
    local seconds = to_seconds(src, src_unit)
    return seconds_to(seconds, dst_unit)
end

------------------------------------------------------------------------------------
-- timer id
------------------------------------------------------------------------------------
local _TIMERIDGENERATE = math.random(1, 0xFFFFFF)
local function _GenerateTimerId()
    _TIMERIDGENERATE = (_TIMERIDGENERATE + 1) % 0xFFFFFF
    return _TIMERIDGENERATE
end

local timerIdmt = {}
timerIdmt.__index = timerIdmt

function timerIdmt:init(wheel, slot)
    self.id = _GenerateTimerId()
    self:update_wheel_slot(wheel, slot)
end

function timerIdmt:update_wheel_slot(wheel, slot)
    self.wheel = wheel
    self.slot = slot
    self.tick = TICK()

    if not self.wheel or self.wheel < 0 or self.wheel > TIMERWHEELNUM then
        assert(false, "====> TIMER <===== cannot find wheel")
    end

    if not self.slot or self.slot < 0 or self.slot > TIMERSLOTNUM then
        assert(false, "====> TIMER <===== cannot find slot")
    end
end

function timerIdmt:get()
    return self.id
end


local TimerId = {}
function TimerId.new(...)
    local o = {}
    setmetatable(o, timerIdmt)
    o:init(...)
    return o
end

------------------------------------------------------------------------------------
-- timer obj
------------------------------------------------------------------------------------
local timerObjmt = {}
timerObjmt.__index = timerObjmt

function timerObjmt:init(create_time, rounds, callback, interval_rounds, interval_offset, times, remainder, rremainder)
    self.timer_id = nil
    self.create_time = create_time
    self.rounds = rounds
    self.callback = callback
    self.status = READY
    self.times = times or 1
    self.remainder = remainder or 0

    self.rescheduleRounds = interval_rounds or 0
    self.rescheduleOffset = interval_offset or 0
    self.rescheduleRemainder = rremainder or 0
end

function timerObjmt:canceled()
    return self.status == CANCELED
end

function timerObjmt:decrement()
    self.rounds = self.rounds -1
end

function timerObjmt:get_offset()
    return self.rescheduleOffset
end

function timerObjmt:ready()
    return self.rounds == 0 and self.remainder <= 0
end

function timerObjmt:reset()
    self.status = READY
    self.rounds = self.rescheduleRounds
    self.remainder = self.rescheduleRemainder
end

function timerObjmt:cancel()
    self.status = CANCELED
end

function timerObjmt:can_reuse()
    return self.times >= 1 or self.times == FOREVER
end

function timerObjmt:can_lower()
    return self.remainder > 0 and self.rounds == 0
end

function timerObjmt:deal()
    local ok, msg = pcall(self.callback)
    if not ok then
        --Log.error("====> TIMER <==== callback error:", msg)
        print("====> TIMER <==== callback error:", msg)
    else
        if self.times ~= FOREVER then
            self.times = self.times -1
        end
    end
end

function timerObjmt:destroy()
    self.timer_id = nil
    self.callback = nil
end

local TimerObj = {}
function TimerObj.new(...) 
    local o = {}
    setmetatable(o, timerObjmt)
    o:init(...)
    return o
end

------------------------------------------------------------------------------------------------
-- timer slot
------------------------------------------------------------------------------------------------
local timerSlotmt = {}
timerSlotmt.__index = timerSlotmt

function timerSlotmt:init(time_unit)
    self.time_unit = time_unit
    self.timers = {}
end

function timerSlotmt:add_delay_timer(create_time, rounds, callback, remainder, slot_idx)
    print("add_delay_timer  ", self.time_unit, create_time, rounds, callback, remainder, slot_idx)
    local timer = TimerObj.new(create_time, rounds, callback, nil, nil, nil, remainder or 0)
    local key = TimerId.new(self.time_unit, slot_idx or 0)
    self.timers[key] = timer
    timer.timer_id = key
    print("=============================", timer.timer_id)
    return key
end

function timerSlotmt:add_repeat_timer(create_time, rounds, callback, interval_rounds, interval_offset, times, remainder, rremainder, slot_idx)
    local timer = TimerObj.new(create_time, rounds, callback, interval_rounds, interval_offset, times, remainder, rremainder)
    local key = TimerId.new(self.time_unit, slot_idx or 0)
    self.timers[key] = timer
    timer.timer_id = key
    return key
end

function timerSlotmt:add_timer(timer, slot_idx)
    timer.timer_id:update_wheel_slot(self.time_unit, slot_idx)
    self.timers[timer.timer_id] = timer
end

function timerSlotmt:process()
    local to_remove = {}
    local to_reschedule = {}
    for key, timer in pairs(self.timers) do
        if timer:canceled() then
            table.insert(to_remove, key)
        elseif timer:ready() then
            timer:deal()
            table.insert(to_remove, key)
            if timer:can_reuse() then
                table.insert(to_reschedule, timer)
            end
        else
            timer:decrement()
        end
    end

    for i = 1, #to_remove do
        self.timers[to_remove[i]] = nil
    end
    return to_reschedule
end

function timerSlotmt:hprocess()
    --print("111111111111111111111111111111111111", self.time_unit)
    local to_remove = {}
    local to_reschedule = {}
    local to_lower = {}
    for key, timer in pairs(self.timers) do
        if timer:canceled() then
            print("111111111111111111111111111111111111")
            table.insert(to_remove, key)
        elseif timer:ready() then
            print("22222222222222222222222222222222222222")
            timer:deal()
            table.insert(to_remove, key)
            if timer:can_reuse() then
                table.insert(to_reschedule, timer)
            end
        elseif timer:can_lower() then
            print("333333333333333333333333333333333333333")
            table.insert(to_lower, timer)
            table.insert(to_remove, key)
        else
            print("44444444444444444444444444444444444")
            timer:decrement()
        end
    end

    for i = 1, #to_remove do
        self.timers[to_remove[i]] = nil
    end
    return to_reschedule, to_lower
end

function timerSlotmt:cancel_id(timer_id)
    self.timers[timer_id] = nil
end

function timerSlotmt:clear()
    for _, timer in pairs(self.timers) do
        timer:cancel()
    end
end

local TimerSlot = {}
function TimerSlot.new(...)
    local o = {}
    setmetatable(o, timerSlotmt)
    o:init(...)
    return o
end

----------------------------------------------------------------------------------
-- timer wheel
----------------------------------------------------------------------------------
local timerWheelmt = {}
timerWheelmt.__index = timerWheelmt

function timerWheelmt:init(wheelSize, resolution, time_unit)
    self.time_unit = time_unit
    self.wheelSize = wheelSize
    self.resolution = resolution
    self.wheels = {}
    for i = 1, wheelSize do 
        print ("____________________________", i)
        table.insert(self.wheels, TimerSlot.new(time_unit))
    end

    self.cursor = 0
    self.pre = nil
    self.next = nil
end

function timerWheelmt:delay_exec(delay, callback)
    local offset = math.floor(delay / self.resolution)
    local rounds = math.floor(offset / self.wheelSize)
    local slot_idx = self:idx(self.cursor + offset)
    self.wheels[slot_idx]:add_delay_timer(TICK(), rounds, callback, nil, slot_idx)
end

function timerWheelmt:repeat_exec(delay, interval, times, callback)
    local offset = math.floor(delay / self.resolution)
    local rounds = math.floor(offset / self.wheelSize)
    local interval_offset = math.floor(interval / self.resolution)
    local interval_rounds = math.floor(interval / self.wheelSize)
    local slot_idx = self:idx(self.cursor + offset)
    self.wheels[slot_idx]:add_repeat_timer(TICK(), rounds, callback, interval_rounds, interval_offset, times, nil, nil, slot_idx)
end

function timerWheelmt:repeat_forever_exec(delay, interval, callback)
    self:repeat_exec(delay, interval, FOREVER, callback)
end

function timerWheelmt:add_hierarchical_delay_timer(offset, rounds, remainder, callback)
    local slot_idx = self:idx(self.cursor + offset)
    return self.wheels[slot_idx]:add_delay_timer(TICK(), rounds, callback, remainder, slot_idx)
end

function timerWheelmt:add_hierarchical_repeat_timer(offset, rounds, remainder, callback, roffset, rrounds, rremainder, times)
    local slot_idx = self:idx(self.cursor + offset)
    return self.wheels[slot_idx]:add_repeat_timer(TICK(), rounds, callback, rrounds, roffset, times, remainder, rremainder, slot_idx)
end

function timerWheelmt:reschedule(timer)
    timer.reset()
    local slot_idx = self:idx(self.cursor + timer:get_offset())
    self.wheels[slot_idx]:add_timer(timer, slot_idx)
end

function timerWheelmt:idx(cursor)
    return math.floor(cursor % self.wheelSize) + 1
end

function timerWheelmt:get_now()
    return TICK()
end

function timerWheelmt:tick()
    while (true) 
    do
        local slot = self.wheels[self.cursor + 1]
        local to_reschedule = slot:process()
        for i = 1, #to_reschedule do
            self.reschedule(to_reschedule[i])
        end
        --time.sleep(to_seconds(self.resolution, self.time_unit))
        self.cursor = (self.cursor + 1) % self.wheelSize
    end
end

function Sleep(n)
   os.execute("sleep " .. n)
end

function timerWheelmt:wait()
    --print(to_seconds(self.resolution * TIME_ELAPSED, self.time_unit), self.resolution, TICK())
    Sleep(to_seconds(self.resolution * TIME_ELAPSED, self.time_unit))
    --time.sleep(to_seconds(self.resolution * TIME_ELAPSED, self.time_unit))
end

function timerWheelmt:update_cursor(timerWheel)
    local old_cursor = self.cursor
    self.cursor = (self.cursor + 1) % self.wheelSize
    if old_cursor + 1 == self.wheelSize and self.pre then
        self.pre:update_cursor(timerWheel)
        print("222=========================================", timerWheel, self.time_unit)
        self.pre:expire(timerWheel)
    end
end

function timerWheelmt:expire(timerWheel)
    local slot = self.wheels[self.cursor + 1]
    local to_reschedule, to_lower = slot:hprocess()
    for i = 1, #to_reschedule do
        self:hreschedule(to_reschedule[i], timerWheel)
    end
    if #to_lower > 0 and self.next then
        for i = 1, #to_lower do
            timerWheel:lower_timer(to_lower[i])
        end
    elseif #to_lower > 0 and not self.next then
        assert(false, "====> TIMER <==== cannot lower timer")
    end
end

function timerWheelmt:hreschedule(timer, timerWheel)
    timer:reset()
    timerWheel:reschedule(timer)
end

function timerWheelmt:hadd_timer(timer, offset)
    if not offset then
        offset = timer:get_offset()
    end
    local slot_idx = self:idx(self.cursor + offset)
    self.wheels[slot_idx]:add_timer(timer, slot_idx)
end

function timerWheelmt:clear()
    for i = 1, #self.wheels do
        self.wheels[i]:clear()
    end
end

local TimerWheel = {}
function TimerWheel.new(...)
    local o = {}
    setmetatable(o, timerWheelmt)
    o:init(...)
    return o
end

-------------------------------------------------------------------------------------------
-- timer wheel chain
-------------------------------------------------------------------------------------------
local timerWheelChainmt = {}
timerWheelChainmt.__index = timerWheelChainmt

function timerWheelChainmt:init(timerWheel)
    self.timerWheel = timerWheel

    self.hourWheel = TimerWheel.new(HOURCOUNT, 1, HOUR)
    self.minuteWheel = TimerWheel.new(MINUTECOUNT, 1, MINUTE)
    self.secondsWheel = TimerWheel.new(SECONDSCOUNT, 1, SECONDS)
    self.frameWheel = TimerWheel.new(FRAMECOUNT, 1, FRAME)

    self.head = self.hourWheel
    self.tail = self.frameWheel

    self.hourWheel.next = self.minuteWheel
    self.minuteWheel.pre = self.hourWheel
    self.minuteWheel.next = self.secondsWheel
    self.secondsWheel.pre = self.minuteWheel
    self.secondsWheel.next = self.frameWheel
    self.frameWheel.pre = self.secondsWheel
end

function timerWheelChainmt:tick()
    while (true)
    do
        self.tail:expire(self.timerWheel)
        self.tail:wait()
        self.tail:update_cursor(self.timerWheel)
        --return
    end
end

local TimerWheelChain = {}
function TimerWheelChain.new(...)
    local o = {}
    setmetatable(o, timerWheelChainmt)
    o:init(...)
    return o
end

-------------------------------------------------------------------------------------------
-- hirerarchical timer wheel
-------------------------------------------------------------------------------------------
local hierarchicalTimerWheelmt = {}
hierarchicalTimerWheelmt.__index = hierarchicalTimerWheelmt

function hierarchicalTimerWheelmt:init()
    self.chain = TimerWheelChain.new(self)
end

function hierarchicalTimerWheelmt:lower_timer(timer)
    self:_reuse(timer.remainder, timer)
end

function hierarchicalTimerWheelmt:reschedule(timer)
    self:_reuse(timer:get_offset(), timer)
end

function hierarchicalTimerWheelmt:_reuse(delay, timer)
    local dhour, hour_remainder, dminute, minute_remainder, dseconds, seconds_remainder, dframes ,_ = self:calc_param(delay)
    if dhour ~= 0 then
        local rounds = math.floor(dhour / 24)
        if rounds == dhour / 24 then
            rounds = rounds - 1
        end
        timer.remainder = hour_remainder
        timer.rounds = rounds
        self.chain.hourWheel:hadd_timer(timer, dhour)
    elseif dminute ~= 0 then
        timer.remainder = minute_remainder
        timer.rounds = 0
        self.chain.minuteWheel:hadd_timer(timer, dminute)
    elseif dseconds ~= 0 then
        timer.remainder = seconds_remainder
        timer.rounds = 0
        self.chain.secondsWheel:hadd_timer(timer, dseconds)
    elseif dframes ~= 0 then
        timer.remainder = 0
        timer.rounds = 0
        self.chain.frameWheel:hadd_timer(timer, dframes)
    else
        timer.remainder = 0
        timer.rounds = 0
        self.chain.frameWheel:hadd_timer(timer, 1)
    end
end

function hierarchicalTimerWheelmt:get_time()
    local hour = self.chain.hourWheel.cursor
    local minute = self.chain.minuteWheel.cursor
    local seconds = self.chain.secondsWheel.cursor
    local frames = self.chain.frameWheel.cursor

    return get_seconds(hour, minute, seconds, frames)
end

function hierarchicalTimerWheelmt:calc_param(delay)
    local now = self:get_time()
    print("=================", now)

    local delay_hour = delay + now
    local hour = math.floor(to_time(delay_hour, SECONDS, HOUR))
    local delay_minutes = delay_hour - hour * MINUTECOUNT * SECONDSCOUNT
    local minute = math.max(0, math.floor(to_time(delay_minutes, SECONDS, MINUTE)))
    local delay_seconds = delay_minutes - minute * SECONDSCOUNT
    local seconds = math.max(0, math.floor(to_time(delay_seconds, SECONDS, SECONDS)))
    local delay_frames = delay_seconds -seconds
    local frames = math.max(0, math.floor(to_time(delay_frames, SECONDS, FRAME)))

    print("==================", delay_hour, hour, delay_minutes, minute, delay_seconds, seconds, delay_frames, frames)

    local cur_hour = self.chain.hourWheel.cursor
    local cur_minute = self.chain.minuteWheel.cursor
    local cur_seconds = self.chain.secondsWheel.cursor
    local cur_frames = self.chain.frameWheel.cursor

    print("=================", hour - cur_hour, delay_hour - get_seconds(hour, 0, 0, 0),
            minute - cur_minute, delay_minutes - get_seconds(0, minute, 0, 0),
            seconds - cur_seconds, delay_seconds - get_seconds(0, 0, seconds, 0),
            frames - cur_frames, 0)

    return hour - cur_hour, delay_hour - get_seconds(hour, 0, 0, 0),
            minute - cur_minute, delay_minutes - get_seconds(0, minute, 0, 0),
            seconds - cur_seconds, delay_seconds - get_seconds(0, 0, seconds, 0),
            frames - cur_frames, 0
end

function hierarchicalTimerWheelmt:delay_exec(delay, callback)
    local dhour, hour_remainder, dminute, minute_remainder, dseconds, seconds_remainder, dframes ,_ = self:calc_param(delay)
    if dhour ~= 0 then
        local rounds = math.floor(dhour / 24)
        if rounds == dhour / 24 then
            rounds = rounds - 1
        end
        return self.chain.hourWheel:add_hierarchical_delay_timer(dhour, rounds, hour_remainder, callback)
    elseif dminute ~= 0 then
        return self.chain.minuteWheel:add_hierarchical_delay_timer(dminute, 0, minute_remainder, callback)
    elseif dseconds ~= 0 then
        return self.chain.secondsWheel:add_hierarchical_delay_timer(dseconds, 0, seconds_remainder, callback)
    elseif dframes ~= 0 then
        return self.chain.frameWheel:add_hierarchical_delay_timer(dframes, 0, 0, callback)
    else
        return self.chain.frameWheel:add_hierarchical_delay_timer(1, 0, 0, callback)
    end
end

function hierarchicalTimerWheelmt:repeat_exec(delay, interval, times, callback)
    local dhour, hour_remainder, dminute, minute_remainder, dseconds, seconds_remainder, dframes ,_ = self:calc_param(delay)
    local rounds = 0
    local remainder = 0

    if dhour ~= 0 then
        offset = dhour
        local rounds = math.floor(dhour / 24)
        if rounds == dhour / 24 then
            rounds = rounds - 1
        end
        remainder = hour_remainder
    elseif dminute ~= 0 then
        offset = dminute
        remainder = minute_remainder
    elseif dseconds ~= 0 then
        offset = dseconds
        remainder = seconds_remainder
    elseif dframes ~= 0 then
        offset = dframes
    else
        offset = 1
    end

    if dhour ~= 0 then
        return self.chain.hourWheel:add_hierarchical_repeat_timer(offset, rounds, remainder, callback, interval, 0, 0, times)
    elseif dminute ~= 0 then
        return self.chain.minuteWheel:add_hierarchical_repeat_timer(offset, rounds, remainder, callback, interval, 0, 0, times)
    elseif dseconds ~= 0 then
        return self.chain.secondsWheel:add_hierarchical_repeat_timer(offset, rounds, remainder, callback, interval, 0, 0, times)
    else
        return self.chain.frameWheel:add_hierarchical_repeat_timer(offset, rounds, remainder, callback, interval, 0, 0, times)
    end
end

function hierarchicalTimerWheelmt:repeat_forever_exec(delay, interval, callback)
    self:repeat_exec(delay, interval, FOREVER, callback)
end

function hierarchicalTimerWheelmt:clear()
    self.chain.hourWheel:clear()
    self.chain.minuteWheel:clear()
    self.chain.secondsWheel:clear()
    self.chain.frameWheel:clear()
end

function hierarchicalTimerWheelmt:cancel_timer(timer_id)
    local wheel = timer_id.wheel
    local slot = timer_id.slot
    if wheel == HOUR and 0 < slot and slot < #self.chain.hourWheel.wheels then
        self.chain.hourWheel.wheels[slot]:cancel_id(timer_id)
    elseif wheel == MINUTE and 0 < slot and slot < #self.chain.minuteWheel.wheels then
        self.chain.minuteWheel.wheels[slot]:cancel_id(timer_id)
    elseif wheel == SECONDS and 0 < slot and slot < #self.chain.secondsWheel.wheels then
        self.chain.secondsWheel.wheels[slot]:cancel_id(timer_id)
    elseif wheel == FRAME and 0 < slot and slot < #self.chain.frameWheel.wheels then
        self.chain.frameWheel.wheels[slot]:cancel_id(timer_id)
    end
end

function hierarchicalTimerWheelmt:start()
    self.chain:tick()
end

local HierarchicalTimerWheel = {}
function HierarchicalTimerWheel.new(frame)
    local o = {}
    if frame > 0 and frame < 100 then
        FRAMECOUNT = frame
    end
    setmetatable(o, hierarchicalTimerWheelmt)
    o:init()
    return o
end

-------------------------------------------------------------------------------------------
-- test timer
-------------------------------------------------------------------------------------------
local wheel = HierarchicalTimerWheel.new(50)
local function cb(delay)
    print ("+++++++++++++++++++++++++++", wheel:get_time(), delay)
end

print ("start time", wheel:get_time())
wheel:delay_exec(0.1, function ()
    print ("+++++++++++++++++++++++++++", wheel:get_time(), 0.1)
end)
wheel:delay_exec(10, function ()
    print ("+++++++++++++++++++++++++++", wheel:get_time(), 10)
end)
b = wheel:delay_exec(20, function ()
    print ("+++++++++++++++++++++++++++", wheel:get_time(), 20)
end)
wheel:delay_exec(30, function ()
    print ("+++++++++++++++++++++++++++", wheel:get_time(), 30)
end)
wheel:delay_exec(40, function ()
    print ("+++++++++++++++++++++++++++", wheel:get_time(), 40)
end)
wheel:delay_exec(50, function ()
    print ("+++++++++++++++++++++++++++", wheel:get_time(), 50)
end)
wheel:delay_exec(60, function ()
    print ("+++++++++++++++++++++++++++", wheel:get_time(), 60)
end)
wheel:delay_exec(70, function ()
    print ("+++++++++++++++++++++++++++", wheel:get_time(), 70)
end)


wheel:delay_exec(100, function ()
    print ("+++++++++++++++++++++++++++", wheel:get_time(), 100)
end)
local a =  wheel:delay_exec(100, function ()
    print ("+++++++++++++++++++++++++++", wheel:get_time(), 101)
end)
wheel:delay_exec(100, function ()
    print ("+++++++++++++++++++++++++++", wheel:get_time(), 102)
end)

--[[
#wheel.repeat_exec(3700, 3700, 20,  function ()
    print "+++++++++++++++++++++++++++", wheel:get_time(), 25
end)

#a = wheel.delay_exec(3600, lambda: cb(102))
]]
repeat_timer = wheel:repeat_exec(25, 40, 200, function ()
    print ("+++++++++++++++++++++++++++", wheel:get_time(), "aaaaaaaa")
end)

wheel:delay_exec(10, function ()
    print ('cancel cb', a, a.wheel, a.slot)
    wheel:cancel_timer(b)
end)

wheel:start()










