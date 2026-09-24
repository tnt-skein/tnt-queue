--- Работник очереди без box: ожидание, когда очередь не принимает,
--- и сбой шага.
---
--- Сама работа идёт против настоящего рока на узле; здесь проверяется
--- то, чего на узле не увидеть глазами, — что работник именно ждёт,
--- а не опрашивает очередь без уступки, и сколько он ждёт.

local fiber = require('fiber')
local t = require('luatest')

local helper = dofile('test/helper.lua')

local worker = helper.worker

local g = t.group('tnt.queue.worker')

g.after_each(function()
    worker._set_source(nil)
    helper.journal.forget()
end)

g.after_all(function()
    helper.journal.release()
end)

--- Очередь-двойник: труба отвечает на `open` тем, что сказано.
---@param open function Что отвечает `open`
---@param name string|nil Имя очереди; по умолчанию «двойник»
---@return table queue
local function queue_of(open, name)
    return {
        name = name or 'двойник',
        counts = { idle_waits = 0, failures = 0, ack = 0, retry = 0, bury = 0, late = 0 },
        handling = {},
        tube = { open = open },
    }
end

--- Очередь-двойник с одним сообщением: труба заведена, `take` отдаёт его
--- один раз, а движения итога записываются в `seen`.
---@param envelope table Конверт
---@param name string|nil Имя очереди
---@return table queue
---@return table seen Движения итога по порядку
local function queue_with(envelope, name)
    local seen = {}
    local taken = false
    local queue = queue_of(function()
        return true
    end, name)

    queue.tube.take = function()
        if taken then
            -- Пустая труба уступает: у настоящей это ожидание задачи.
            fiber.sleep(0.01)

            return nil
        end

        taken = true

        return 7, envelope
    end

    queue.tube.ack = function()
        table.insert(seen, 'ack')
    end

    queue.tube.release = function(_, _, delay)
        table.insert(seen, ('release %s'):format(delay))
    end

    queue.tube.bury = function(_, _, _, reason)
        table.insert(seen, ('bury %s'):format(reason))
    end

    return queue, seen
end

--- Конверт для двойника.
---@param attempt integer|nil
---@return table
local function envelope_of(attempt)
    return {
        id = '01J0000000000000000000000X',
        name = 'двойник',
        body = 'тело',
        headers = { ['x-request-id'] = 'r-7' },
        attempt = attempt or 1,
        created = 0,
    }
end

--- Пускает получателя на одном сообщении и ждёт, пока он с ним кончит.
---@param queue table
---@param seen table
---@param handler function
---@param opts table|nil
---@return TntQueueConsumer
local function handled(queue, seen, handler, opts)
    worker._set_source({
        sleep = function()
            fiber.sleep(0)
        end,
        read_only = function()
            return false
        end,
    })

    local consumer = worker.start(queue, handler, helper.policy(opts))

    t.helpers.retrying({ timeout = 5 }, function()
        assert(#seen > 0, 'итог не записан')
    end)

    consumer:stop()

    return consumer
end

--- Пускает работника, пока он не уступит `rounds` раз, и останавливает.
---@param queue table
---@param read_only boolean
---@param rounds integer
---@return table seen Паузы в `slept`, ожидания записи в `waited`
local function run(queue, read_only, rounds)
    local seen = { slept = {}, waited = {} }

    ---@type TntQueueConsumer
    local consumer

    --- Уступка двойника: считает круги и останавливает работника.
    local function yielded(list, seconds)
        table.insert(list, seconds)

        if #seen.slept + #seen.waited >= rounds then
            consumer:stop()
        end

        fiber.sleep(0)
    end

    worker._set_source({
        sleep = function(seconds)
            yielded(seen.slept, seconds)
        end,
        wait_rw = function(timeout)
            yielded(seen.waited, timeout)
        end,
        read_only = function()
            return read_only
        end,
    })

    consumer = worker.start(queue, function() end, helper.policy())

    t.helpers.retrying({ timeout = 5 }, function()
        assert(consumer.workers == 0, 'работник не остановился')
    end)

    return seen
end

g.test_on_a_read_only_node_the_worker_waits_for_writes = function()
    local queue = queue_of(function()
        return false, 'узел только для чтения'
    end)

    local seen = run(queue, true, 2)

    t.assert_equals(seen.waited, { 0.5, 0.5 }, 'ждёт записи со сроком, а не опрашивает')
    t.assert_equals(seen.slept, {})
    t.assert_equals(queue.counts.idle_waits, 2)
end

g.test_while_the_queue_is_rising_the_worker_sleeps_short = function()
    local queue = queue_of(function()
        return false, 'WAITING'
    end)

    local seen = run(queue, false, 3)

    t.assert_equals(
        seen.slept,
        { 0.01, 0.01, 0.01 },
        'узел пишет, а очередь ещё нет: короткий сон'
    )
    t.assert_equals(seen.waited, {})
    t.assert_equals(queue.counts.idle_waits, 3)
end

g.test_a_broken_step_is_counted_logged_and_followed_by_a_pause = function()
    local queue = queue_of(function()
        error('труба развалилась', 0)
    end)

    local seen = run(queue, false, 2)

    t.assert_equals(
        seen.slept,
        { 0.1, 0.1 },
        'после сбоя работник спит, а не крутит цикл'
    )
    t.assert_equals(queue.counts.failures, 2)
    t.assert_equals(queue.counts.idle_waits, 0)

    local record = helper.journal.find('шаг работника очереди сорвался').record

    t.assert_equals(record.fields.destination, 'двойник')
    t.assert_str_contains(record.fields.err, 'труба развалилась')
end

g.test_workers_are_started_by_the_count_of_the_policy = function()
    local queue = queue_of(function()
        return false, 'WAITING'
    end)

    local consumer = worker.start(queue, function() end, helper.policy({ workers = 3 }))

    t.assert_equals(consumer.workers, 3)

    consumer:stop()

    t.helpers.retrying({ timeout = 5 }, function()
        assert(consumer.workers == 0, 'работники не кончились')
    end)
end

g.test_an_acknowledged_message_leaves_no_record_and_a_returned_one_is_debug = function()
    local queue, seen = queue_with(envelope_of(1))

    handled(queue, seen, function(message)
        if message.attempt == 1 then
            return nil, { retry_after = 0.25, message = 'подождите' }
        end
    end)

    local entry = helper.journal.find('сообщение возвращено на выдачу')

    t.assert_equals(seen, { 'release 0.25' })
    t.assert_equals(queue.counts.retry, 1)
    t.assert_equals(entry.line:match('^%u+'), 'DEBUG', 'повтор — debug')
    t.assert_equals(entry.record.fields.id, '01J0000000000000000000000X')
    t.assert_equals(entry.record.fields.attempt, 1)
    t.assert_equals(entry.record.fields.delay, 0.25)
    t.assert_equals(
        helper.journal.find('сообщение зарыто'),
        nil,
        'зарывать было нечего'
    )
end

g.test_a_buried_message_is_written_as_an_error = function()
    local queue, seen = queue_with(envelope_of(1))

    handled(queue, seen, function()
        return nil, { retriable = false, message = 'тело не прошло проверку' }
    end)

    local entry = helper.journal.find('сообщение зарыто')

    t.assert_equals(seen, { 'bury тело не прошло проверку' })
    t.assert_equals(queue.counts.bury, 1)
    t.assert_equals(entry.line:match('^%u+'), 'ERROR', 'зарытое — error')
    t.assert_equals(entry.record.fields.err, 'тело не прошло проверку')
    t.assert_equals(helper.journal.find('сообщение возвращено на выдачу'), nil)
end

g.test_a_raised_handler_is_written_with_a_traceback_of_the_handler = function()
    local queue, seen = queue_with(envelope_of(1))

    handled(queue, seen, function()
        error('деление на ноль')
    end)

    local entry = helper.journal.find('обработчик сообщения бросил')
    local traceback = entry.record.fields.traceback

    t.assert_equals(entry.line:match('^%u+'), 'ERROR')
    t.assert_str_contains(entry.record.fields.err, 'деление на ноль')

    -- Стек снят с места броска: первым кадром `error`, а не ловец пакета
    -- и не `xpcall`, который его позвал.
    t.assert_str_matches(traceback, "\nstack traceback:\n\t%[C%]: in function 'error'\n.*")
    t.assert_not_equals(
        traceback:find('worker_test.lua', 1, true),
        nil,
        'в стеке виден обработчик'
    )
end

g.test_the_context_of_the_sender_reaches_the_handler = function()
    local queue, seen = queue_with(envelope_of(1))
    local carried = {}

    handled(queue, seen, function()
        table.insert(carried, helper.context.get('request_id'))
    end)

    t.assert_equals(carried, { 'r-7' })
    t.assert_equals(seen, { 'ack' })
    t.assert_equals(queue.counts.ack, 1)
end

g.test_the_worker_carries_the_name_of_its_queue = function()
    local queue, seen = queue_with(envelope_of(1), 'имя_очереди')
    local names = {}

    handled(queue, seen, function()
        table.insert(names, fiber.self():name())
    end)

    t.assert_equals(names, { 'queue/имя_очереди/1' })
end

g.test_a_long_queue_name_is_cut_and_does_not_kill_the_worker = function()
    local name = string.rep('я', 200)
    local queue, seen = queue_with(envelope_of(1), name)

    handled(queue, seen, function() end)

    t.assert_equals(seen, { 'ack' }, 'длинное имя не мешает работнику')
end

g.test_without_box_the_worker_sleeps_instead_of_waiting_for_writes = function()
    local queue = queue_of(function()
        return false, 'CLOSED'
    end)
    local seen = { slept = {}, waited = {} }

    ---@type TntQueueConsumer
    local consumer

    -- Признак узла для чтения берётся умолчанием внешней зависимости: до `box.cfg` узел
    -- ещё никакой, и ждать записи у него нечего.
    worker._set_source({
        sleep = function(seconds)
            table.insert(seen.slept, seconds)

            if #seen.slept >= 2 then
                consumer:stop()
            end

            fiber.sleep(0)
        end,
        wait_rw = function(timeout)
            table.insert(seen.waited, timeout)
            fiber.sleep(0)
        end,
    })

    consumer = worker.start(queue, function() end, helper.policy())

    t.helpers.retrying({ timeout = 5 }, function()
        assert(consumer.workers == 0, 'работник не остановился')
    end)

    t.assert_equals(seen.slept, { 0.01, 0.01 })
    t.assert_equals(seen.waited, {}, 'записи не ждёт: узел ещё не настроен')
end
