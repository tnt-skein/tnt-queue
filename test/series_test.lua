--- Ряды очереди без узла: что растёт с каким счётчиком очереди и откуда
--- берётся глубина.
---
--- Очередь здесь — двойник с именем, счётчиками и `status()`: ряды судят
--- только по ним. Сами ряды, их роды и корзины ожидания — общие ряды
--- договора, и проверяет их общая часть (`tnt-message`). Что настоящая
--- очередь зовёт ряды на каждом движении, показывают проверки на узле
--- (`series_node_test.lua`).

local t = require('luatest')

local helper = dofile('test/helper.lua')

--- Что увидит сборщик: число ряда с такими метками и все наблюдения ряда.
local value, samples = helper.value, helper.samples

local g = t.group('tnt.queue.series')

local series = helper.series

--- Двойник очереди: имя, счётчики и глубина, которую отдаст `status()`.
---@param name string
---@param depth table|nil
---@return table
local function double(name, depth)
    return {
        name = name,
        counts = { sent = 0, ack = 0, retry = 0, bury = 0, late = 0, expired = 0, failures = 0 },
        status = function()
            return { depth = depth }
        end,
    }
end

g.test_each_counter_of_the_status_grows_its_row = function()
    local queue = double('series_counts')

    for _, what in ipairs({ 'sent', 'sent', 'ack', 'retry', 'bury', 'late', 'expired', 'failures' }) do
        series.count(queue, what)
    end

    t.assert_equals(queue.counts, { sent = 2, ack = 1, retry = 1, bury = 1, late = 1, expired = 1, failures = 1 })

    local destination = { destination = 'series_counts' }

    t.assert_equals(value('message_sent_total', destination), 2)
    t.assert_equals(value('message_expired_total', destination), 1)
    t.assert_equals(value('message_worker_failures_total', destination), 1)

    for _, outcome in ipairs({ 'ack', 'retry', 'bury', 'late' }) do
        t.assert_equals(
            value('message_handled_total', { destination = 'series_counts', outcome = outcome }),
            1,
            outcome
        )
    end
end

g.test_a_refused_send_is_counted_by_its_kind = function()
    series.refused('series_refused', { kind = 'conflict' })

    t.assert_equals(value('message_send_failures_total', { destination = 'series_refused', kind = 'conflict' }), 1)
end

g.test_the_depth_is_asked_of_each_watched_queue_at_the_collection = function()
    local full = double('series_full', { ready = 1, taken = 2, delayed = 3, dead = 4 })
    local closed = double('series_closed', nil)

    series.watch(full)
    series.watch(closed)

    -- Проверки соседних пакетов в том же процессе чистят реестр целиком;
    -- ряды очереди возвращает туда первое же наблюдение.
    series.count(full, 'sent')

    for state, count in pairs({ ready = 1, taken = 2, delayed = 3, dead = 4 }) do
        t.assert_equals(value('message_depth', { destination = 'series_full', state = state }), count)
    end

    for _, sample in ipairs(samples('message_depth')) do
        t.assert_not_equals(
            sample.label_pairs.destination,
            'series_closed',
            'не принимающая очередь глубины не знает'
        )
    end

    full.status = function()
        return { depth = { ready = 9, taken = 0, delayed = 0, dead = 0 } }
    end

    t.assert_equals(value('message_depth', { destination = 'series_full', state = 'ready' }), 9)
end

g.test_a_queue_nobody_holds_leaves_the_depth = function()
    series.watch(double('series_gone', { ready = 1, taken = 0, delayed = 0, dead = 0 }))
    series.count(double('series_gone'), 'sent')
    collectgarbage()
    collectgarbage()

    for _, sample in ipairs(samples('message_depth')) do
        t.assert_not_equals(sample.label_pairs.destination, 'series_gone')
    end
end
