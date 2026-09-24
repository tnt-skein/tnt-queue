--- Ряды очереди на временном узле с настоящим роком: отправка, итоги,
--- ожидание первой выдачи и глубина — так, как их видит сборщик.
---
--- Какой ряд растёт с каким счётчиком, проверено без узла
--- (`series_test.lua`); здесь — что настоящая работа очереди до рядов
--- доходит, а глубина берётся у рока на каждом сборе.

local t = require('luatest')

local helper = dofile('test/helper.lua')

local g = t.group('tnt.queue.series.node')

g.before_all(function()
    g.server = helper.start_node('series')
end)

g.after_all(function()
    helper.stop_node(g.server)
end)

g.test_a_queue_at_work_shows_in_its_rows = function()
    local seen = g.server:exec(function()
        local queue = require('tnt.queue')

        local rows = queue.declare('rows', { ttr = 5 })

        -- Очередь держит узел, а не только эта функция: глубину ряд
        -- спрашивает у очередей, которые кто-то держит.
        rawset(_G, 'rows_queue', rows)

        local done = {}
        local consumer = rows:consume(function(message)
            if message.body == 'со сбоем' and message.attempt == 1 then
                return nil, 'сервис недоступен'
            end

            table.insert(done, message.body)
        end, { backoff = { base = 0.05, max = 1 } })

        rows:send('сразу')
        rows:send('со сбоем')
        rows:send('позже', { delay = 0.3 })

        t.helpers.retrying({ timeout = 15 }, function()
            assert(#done == 3, 'сообщения не обработаны')
        end)

        consumer:stop()

        t.helpers.retrying({ timeout = 15 }, function()
            assert(consumer.workers == 0, 'работник не остановился')
        end)

        rows:send('лежит')
        rows:send('и это лежит')

        return rows:status().counts
    end)

    local function row(name, labels)
        labels.destination = 'rows'

        return helper.row(g.server, name, labels)
    end

    t.assert_equals(seen.sent, 5)
    t.assert_equals(row('message_sent_total', {}), 5)
    t.assert_equals(row('message_handled_total', { outcome = 'ack' }), 3)
    t.assert_equals(row('message_handled_total', { outcome = 'retry' }), 1)
    t.assert_equals(row('message_handled_total', { outcome = 'bury' }), nil)
    t.assert_equals(
        row('message_wait_seconds_count', {}),
        3,
        'повторная выдача в ожидание не идёт'
    )
    t.assert_ge(row('message_wait_seconds_sum', {}), 0.3, 'отсрочка — часть ожидания')
    t.assert_equals(row('message_depth', { state = 'ready' }), 2)
    t.assert_equals(row('message_depth', { state = 'taken' }), 0)
    t.assert_equals(row('message_depth', { state = 'delayed' }), 0)
    t.assert_equals(row('message_depth', { state = 'dead' }), 0)
end
