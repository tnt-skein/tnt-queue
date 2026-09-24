--- Отказы отправки: род по тому, что бросил рок или box.
---
--- Узел здесь свой: проверка рвёт синхронную запись кворумом, которого
--- нет, и держать это рядом с обычными очередями незачем.

local t = require('luatest')

local helper = dofile('test/helper.lua')

--- Узел у каждой проверки свой: обе ломают рок так, что он этого
--- не переживает. Снесённый из-под него спейс трубы убивает его фибер
--- состояния на первой же смене ведущего, а смена ведущего нужна второй
--- проверке.
local broken = t.group('tnt.queue.failure.broken')
local conflict = t.group('tnt.queue.failure.conflict')

for name, g in pairs({ broken = broken, conflict = conflict }) do
    g.before_all(function()
        g.server = helper.start_node(name)
    end)

    g.after_all(function()
        helper.stop_node(g.server)
    end)
end

broken.test_a_raised_put_is_a_broken_send = function()
    local seen = broken.server:exec(function()
        local queue = require('tnt.queue')

        local gone = queue.declare('gone', { ttr = 5 })

        -- Спейс трубы унесли из-под рока: `put` бросает.
        box.space.gone:drop()

        -- Спейса зарытых тоже нет: возврат зарытых бросает.
        box.space.gone_dead:drop()

        local kicked, kick_err = gone:kick()
        local sent, err = gone:send('некуда')
        local with_id = assert(select(2, gone:send('некуда', { id = '01J0000000000000000000000X' })))

        kick_err = assert(kick_err)
        err = assert(err)
        local leaked = box.is_in_txn()

        -- В чужой транзакции откатывает вызывающий: отправка — её часть.
        box.begin()

        local inside = assert(select(2, gone:send('некуда')))
        local kept = box.is_in_txn()

        box.rollback()

        return {
            sent = sent,
            kicked = kicked,
            kick_err = { kind = kick_err.kind, retriable = kick_err.retriable },
            leaked = leaked,
            inside = inside.kind,
            kept = kept,
            err = { kind = err.kind, sent = err.sent, retriable = err.retriable, message = err.message },
            with_id = { retriable = with_id.retriable },
            counts = gone.counts,
        }
    end)

    t.assert_equals(seen.sent, nil)
    t.assert_equals(seen.err.kind, 'broken')
    t.assert_equals(
        seen.err.sent,
        true,
        'сообщение могло уйти: отказ пришёл после отправки'
    )
    t.assert_equals(
        seen.err.retriable,
        false,
        'повторять отправку без опознавателя нельзя'
    )
    t.assert_equals(
        seen.with_id.retriable,
        true,
        'с готовым опознавателем повтор узнаётся по нему'
    )
    t.assert_equals(seen.counts.sent, 0, 'несостоявшаяся отправка не считается')
    t.assert_equals(seen.kicked, nil)
    t.assert_equals(seen.kick_err.kind, 'broken', 'возврат зарытых отказывает парой')
    t.assert_equals(seen.kick_err.retriable, false, 'повторять возврат вслепую нечего')
    t.assert_equals(seen.leaked, false, 'транзакция рока не утекает вызывающему')
    t.assert_equals(seen.inside, 'broken')
    t.assert_equals(seen.kept, true, 'чужую транзакцию отправка не откатывает')
    t.assert_equals(
        helper.row(broken.server, 'message_send_failures_total', { destination = 'gone', kind = 'broken' }),
        3,
        'отказ отправки виден в ряду по роду, а отказ возврата зарытых — нет'
    )
end

conflict.test_a_synchronous_rollback_is_a_conflict = function()
    local seen = conflict.server:exec(function()
        local queue = require('tnt.queue')

        local rock = require('queue') --[[@as table]]
        local synced = queue.declare('synced', { ttr = 5 })

        box.ctl.promote()

        -- Смена ведущего проводит очередь через STARTUP даже на одиночном
        -- узле: пока она не RUNNING, отправка отвечает unreachable.
        t.helpers.retrying({ timeout = 15 }, function()
            ---@diagnostic disable-next-line: undefined-field
            assert(rock.state() == 'RUNNING', 'очередь не поднялась после promote')
        end)

        box.space.synced:alter({ is_sync = true })
        box.cfg({ replication_synchro_quorum = 2, replication_synchro_timeout = 0.2 })

        local sent, err = synced:send('без кворума')

        err = assert(err)

        box.space.synced:alter({ is_sync = false })

        -- Отправка за синхронной транзакцией, которая не собрала кворум:
        -- лимб откатывает всё, что встало за ней.
        local sync_orders = box.schema.space.create('sync_orders', { is_sync = true })

        sync_orders:create_index('primary', {})

        local waiter = require('fiber').create(function()
            pcall(function()
                sync_orders:insert({ 1 })
            end)
        end)

        require('fiber').sleep(0.05)

        local behind, rolled = synced:send('за синхронной')

        t.helpers.retrying({ timeout = 15 }, function()
            assert(waiter:status() == 'dead', 'синхронная транзакция ещё ждёт')
        end)

        rolled = assert(rolled)
        box.cfg({ replication_synchro_quorum = 1 })

        return {
            sent = sent,
            behind = behind,
            rolled = { kind = rolled.kind, retriable = rolled.retriable, message = rolled.message },
            err = { kind = err.kind, sent = err.sent, retriable = err.retriable, message = err.message },
            left = box.space.synced:count(),
            works = synced:send('с кворумом') ~= nil,
        }
    end)

    t.assert_equals(seen.sent, nil)
    t.assert_equals(
        seen.err.kind,
        'conflict',
        'откат синхронной транзакции — повторять всю транзакцию'
    )
    t.assert_equals(seen.err.sent, false)
    t.assert_equals(seen.err.retriable, false)
    t.assert_str_contains(seen.err.message, 'Quorum')
    t.assert_equals(seen.behind, nil)
    t.assert_equals(seen.rolled.kind, 'conflict', 'откат лимба — тоже конфликт')
    t.assert_equals(seen.rolled.retriable, false)
    t.assert_str_contains(seen.rolled.message, 'rollback')
    t.assert_equals(seen.left, 0, 'откачено — сообщения нет')
    t.assert_equals(seen.works, true)
    t.assert_equals(
        helper.row(conflict.server, 'message_send_failures_total', { destination = 'synced', kind = 'conflict' }),
        2
    )
end
