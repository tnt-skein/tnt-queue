--- Узел для чтения: ленивая загрузка рока, отказ парой и работник,
--- который ждёт записи, а не крутится.
---
--- Узел здесь свой, и на нём до первого объявления рока нет вовсе:
--- `require('queue')` на узле для чтения подменяет вызов `box.cfg`,
--- печатает стек в stdout и отказывает исключением.

local t = require('luatest')

local helper = dofile('test/helper.lua')

local g = t.group('tnt.queue.readonly')

g.before_all(function()
    g.server = helper.start_node('readonly')
end)

g.after_all(function()
    helper.stop_node(g.server)
end)

g.test_the_rock_is_not_loaded_on_a_read_only_node = function()
    local seen = g.server:exec(function()
        local clock = require('clock')
        local fiber = require('fiber')
        local queue = require('tnt.queue')

        box.cfg({ read_only = true })

        local lazy = queue.declare('lazy', { ttr = 5 })
        local handled = {}
        local consumer = lazy:consume(function(message)
            table.insert(handled, message.body)
        end)

        local loaded = package.loaded['queue'] ~= nil
        local closed = lazy:status().state
        local sent, refused = lazy:send('пока нельзя')
        local kicked, kick_err = lazy:kick(3)

        refused = assert(refused)
        kick_err = assert(kick_err)

        -- Тикер отмечает свои обороты временем процессора потока: работник,
        -- крутящий `take` без уступки, тратит его между оборотами тикера,
        -- а медленная машина и остановки процесса — нет. Число оборотов
        -- за окно зависело от загрузки машины: под соседними прогонами
        -- их выходило меньше, хотя работник уступал исправно.
        local last = clock.thread()
        local held = 0.0

        --- Отмечает оборот: самый долгий промежуток от прошлой отметки.
        local function tick()
            local now = clock.thread()

            held, last = math.max(held, now - last), now
        end

        local ticker = fiber.create(function()
            while true do
                fiber.sleep(0.01)
                tick()
            end
        end)

        local before = lazy:status().counts.idle_waits

        fiber.sleep(0.3)
        ticker:cancel()
        -- И хвост окна: работник, занявший поток до конца окна, виден здесь.
        tick()

        local waits = lazy:status().counts.idle_waits - before

        box.cfg({ read_only = false })

        -- Труба ещё не заведена, а в транзакции её не завести: DDL
        -- уступает, и уступка порвала бы транзакцию вызывающего.
        box.begin()

        local in_txn = assert(select(2, lazy:send('в транзакции')))

        box.rollback()

        t.helpers.retrying({ timeout = 15 }, function()
            assert(lazy:send('теперь можно') ~= nil, 'очередь не поднялась')
        end)

        t.helpers.retrying({ timeout = 15 }, function()
            assert(#handled == 1, 'сообщение не обработано')
        end)

        consumer:stop()

        return {
            loaded = loaded,
            closed = closed,
            sent = sent,
            kicked = kicked,
            kick_err = { kind = kick_err.kind, retriable = kick_err.retriable },
            refused = {
                kind = refused.kind,
                sent = refused.sent,
                retriable = refused.retriable,
                message = refused.message,
            },
            in_txn = { kind = in_txn.kind, message = in_txn.message },
            held = held,
            waits = waits,
            handled = handled,
            state = lazy:status().state,
        }
    end)

    t.assert_equals(seen.loaded, false, 'на узле для чтения рок не грузится')
    t.assert_equals(seen.closed, 'CLOSED', 'пока трубы нет, очередь закрыта')
    t.assert_equals(seen.sent, nil)
    t.assert_equals(seen.refused.kind, 'unreachable')
    t.assert_equals(seen.refused.sent, false)
    t.assert_equals(seen.refused.retriable, true)
    t.assert_equals(
        seen.refused.message,
        'очередь lazy не принимает: узел только для чтения'
    )
    t.assert_equals(seen.kicked, nil)
    t.assert_equals(
        seen.kick_err.kind,
        'unreachable',
        'зарытых на узле для чтения не вернуть'
    )
    t.assert_equals(seen.kick_err.retriable, true)
    t.assert_equals(seen.in_txn.kind, 'unreachable')
    t.assert_ge(
        helper.row(g.server, 'message_send_failures_total', { destination = 'lazy', kind = 'unreachable' }),
        2,
        'отказы отправки на узле для чтения видны в ряду'
    )
    t.assert_equals(
        seen.in_txn.message,
        'очередь lazy не принимает: труба ещё не заведена'
    )
    -- Граница вдали от обеих сторон: оборот тикера стоит доли миллисекунды
    -- процессора, а работник без уступки занял бы окно в 0,3 с.
    t.assert_lt(seen.held, 0.1, 'цикл событий крутится: работник не занял узел')
    t.assert_le(
        seen.waits,
        10,
        'работник ждёт записи, а не опрашивает очередь без уступки'
    )
    t.assert_equals(seen.handled, { 'теперь можно' })
    t.assert_equals(seen.state, 'RUNNING')
end

g.test_the_queue_stops_accepting_when_the_node_goes_read_only = function()
    local seen = g.server:exec(function()
        local fiber = require('fiber')
        local queue = require('tnt.queue')

        local rock = require('queue') --[[@as table]]
        local switched = queue.declare('switched', { ttr = 5 })

        switched:send('до перехода')

        local task_id = assert(switched.tube:take(0))

        box.cfg({ read_only = true })

        t.helpers.retrying({ timeout = 15 }, function()
            ---@diagnostic disable-next-line: undefined-field
            assert(rock.state() == 'WAITING', 'очередь ещё не в ожидании')
        end)

        local waiting = assert(select(2, switched:send('во время чтения')))
        local ack = select(2, pcall(switched.tube.ack, switched.tube, task_id))

        box.cfg({ read_only = false })

        -- Узел уже пишет, а очередь ещё нет: рок проводит её через
        -- WAITING и STARTUP, и новая очередь в это окно не заводится.
        local rising = queue.declare('rising', { ttr = 5 })
        local early = assert(select(2, rising:send('в окне подъёма')))

        t.helpers.retrying({ timeout = 15 }, function()
            ---@diagnostic disable-next-line: undefined-field
            assert(rock.state() == 'RUNNING', 'очередь не поднялась')
        end)

        fiber.sleep(0)

        return {
            waiting = { kind = waiting.kind, message = waiting.message, retriable = waiting.retriable },
            ack = tostring(ack),
            early = { kind = early.kind, message = early.message },
            works = rising:send('после подъёма') ~= nil,
        }
    end)

    t.assert_equals(seen.waiting.kind, 'unreachable')
    t.assert_equals(seen.waiting.retriable, true)
    t.assert_equals(seen.waiting.message, 'очередь switched не принимает: WAITING')
    t.assert_equals(
        seen.ack,
        'очередь switched в состоянии WAITING',
        'движение, которое не записалось, — исключение без места'
    )
    t.assert_equals(seen.early.kind, 'unreachable')
    t.assert_not_equals(
        seen.early.message:match('^очередь rising не принимает: (%u+)$'),
        nil,
        'в причине — состояние рока'
    )
    t.assert_equals(seen.works, true)
end
