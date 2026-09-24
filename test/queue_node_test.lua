--- Очередь на временном узле с настоящим роком `queue`: отправка, выдача,
--- итог обработчика, ключ, транзакции, зарытые.
---
--- Двойника рока здесь нет нарочно: работа внутри чужая, и проверять её
--- двойником значило бы доказать, что мы правильно разговариваем сами
--- с собой. Каждая очередь берёт своё имя: рок держит трубы в спейсах
--- одного узла на весь набор.

local t = require('luatest')

local helper = dofile('test/helper.lua')

local g = t.group('tnt.queue.node')

g.before_all(function()
    g.server = helper.start_node()
end)

g.after_all(function()
    helper.stop_node(g.server)
end)

--- Исполняет тело на узле; первым аргументом идёт всё, что ему передали.
---@param body function
---@param args table|nil
---@return any
local function on_node(body, args)
    return g.server:exec(body, { args or {} })
end

g.test_send_returns_a_ulid_and_the_handler_gets_the_envelope = function()
    local seen = on_node(function()
        local context = require('tnt.context')
        local queue = require('tnt.queue')

        local mail = queue.declare('mail', { ttr = 5 })
        local taken = {}

        local consumer = mail:consume(function(message)
            table.insert(taken, {
                id = message.id,
                name = message.name,
                body = message.body,
                attempt = message.attempt,
                key = message.key,
                request_id = context.get('request_id'),
            })
        end, { workers = 2 })

        local sent = context.run({ request_id = 'r-7' }, function()
            return mail:send({ to = 'a@example.org' })
        end)

        t.helpers.retrying({ timeout = 15 }, function()
            assert(#taken == 1, 'сообщение не обработано')
        end)

        mail:send('без контекста')

        t.helpers.retrying({ timeout = 15 }, function()
            assert(#taken == 2, 'второе сообщение не обработано')
        end)

        local running = mail:status().workers

        consumer:stop()

        return {
            sent = sent,
            taken = taken,
            running = running,
            left = box.space.mail:count(),
            status = mail:status(),
        }
    end)

    t.assert_equals(#seen.sent, 26)
    t.assert_equals(seen.taken[1].id, seen.sent)
    t.assert_equals(seen.taken[1].name, 'mail')
    t.assert_equals(seen.taken[1].body, { to = 'a@example.org' })
    t.assert_equals(seen.taken[1].attempt, 1)
    t.assert_equals(seen.taken[1].key, nil)
    t.assert_equals(
        seen.taken[1].request_id,
        'r-7',
        'контекст отправителя доехал заголовком'
    )
    t.assert_equals(
        #seen.taken[2].request_id,
        26,
        'без контекста — свой опознаватель запроса'
    )
    t.assert_equals(seen.left, 0, 'подтверждённое уходит из трубы')
    t.assert_equals(seen.status.counts.sent, 2)
    t.assert_equals(seen.status.counts.ack, 2)
    t.assert_equals(seen.status.state, 'RUNNING')
    t.assert_equals(seen.status.ttr, 5)
    t.assert_equals(seen.status.depth, { ready = 0, taken = 0, delayed = 0, dead = 0 })
    t.assert_equals(seen.running, 2, 'работники получателя видны в состоянии')
end

g.test_a_failure_is_retried_with_a_backoff_and_then_buried = function()
    local seen = on_node(function()
        local clock = require('clock')
        local queue = require('tnt.queue')

        local flaky = queue.declare('flaky', { ttr = 5 })
        local attempts = {}

        local consumer = flaky:consume(function(message)
            table.insert(attempts, { attempt = message.attempt, at = clock.monotonic() })

            return nil, 'сервис недоступен'
        end, { max_attempts = 3, backoff = { base = 0.1, max = 1 } })

        local sent = flaky:send('работа')

        t.helpers.retrying({ timeout = 15 }, function()
            assert(box.space.flaky_dead:get(sent) ~= nil, 'сообщение не зарыто')
        end)

        consumer:stop()

        local dead = assert(box.space.flaky_dead:get(sent))

        return {
            attempts = attempts,
            dead = { id = dead[1], reason = dead[2], attempt = dead[3], buried = dead[4], message = dead[5] },
            gaps = { attempts[2].at - attempts[1].at, attempts[3].at - attempts[2].at },
            counts = flaky:status().counts,
            left = box.space.flaky:count(),
            now = clock.realtime(),
        }
    end)

    t.assert_equals(#seen.attempts, 3)
    t.assert_equals({ seen.attempts[1].attempt, seen.attempts[2].attempt, seen.attempts[3].attempt }, { 1, 2, 3 })
    t.assert_ge(seen.gaps[1], 0.09, 'первая отсрочка — 0,1 с')
    t.assert_ge(seen.gaps[2], 0.19, 'вторая отсрочка вдвое больше')
    t.assert_equals(seen.dead.reason, 'попыток 3 из 3: сервис недоступен')
    t.assert_equals(seen.dead.attempt, 3)
    t.assert_equals(seen.dead.message.body, 'работа')
    t.assert_almost_equals(seen.dead.buried, seen.now, 5)
    t.assert_equals(seen.left, 0, 'зарытое уходит из трубы')
    t.assert_equals(seen.counts.retry, 2)
    t.assert_equals(seen.counts.bury, 1)
end

g.test_an_unretriable_failure_is_buried_at_once_and_retry_after_is_the_delay = function()
    local seen = on_node(function()
        local clock = require('clock')
        local failure = require('tnt.storage.failure')
        local queue = require('tnt.queue')

        local strict = queue.declare('strict', { ttr = 5 })
        local attempts = 0

        local consumer = strict:consume(function(message)
            attempts = attempts + 1

            if message.body == 'негодное' then
                return nil, failure.new('rejected', 'тело не прошло проверку')
            end

            if message.attempt == 1 then
                return nil, { retry_after = 0.2, message = 'подождите' }
            end
        end)

        local invalid = strict:send('негодное')

        t.helpers.retrying({ timeout = 15 }, function()
            assert(box.space.strict_dead:get(invalid) ~= nil, 'негодное не зарыто')
        end)

        local buried = attempts
        local started = clock.monotonic()

        attempts = 0
        strict:send('позже')

        t.helpers.retrying({ timeout = 15 }, function()
            assert(attempts == 2 and box.space.strict:count() == 0, 'повтор не дошёл')
        end)

        consumer:stop()

        return {
            buried = buried,
            dead = assert(box.space.strict_dead:get(invalid)):totable(),
            waited = clock.monotonic() - started,
        }
    end)

    t.assert_equals(seen.buried, 1, 'retriable = false — зарыто с первой выдачи')
    t.assert_equals(seen.dead[2], 'тело не прошло проверку')
    t.assert_equals(seen.dead[3], 1)
    t.assert_ge(seen.waited, 0.19, 'retry_after отказа — отсрочка')
end

g.test_a_raised_handler_is_an_attempt_with_its_text = function()
    local seen = on_node(function()
        local queue = require('tnt.queue')

        local crash = queue.declare('crash', { ttr = 5 })

        local consumer = crash:consume(function()
            error('деление на ноль')
        end, { max_attempts = 2, backoff = { base = 0.05, max = 1 } })

        local sent = crash:send('бабах')

        t.helpers.retrying({ timeout = 15 }, function()
            assert(box.space.crash_dead:get(sent) ~= nil, 'упавшее не зарыто')
        end)

        consumer:stop()

        return { dead = assert(box.space.crash_dead:get(sent)):totable() }
    end)

    t.assert_equals(seen.dead[3], 2)
    t.assert_str_contains(seen.dead[2], 'попыток 2 из 2:')
    t.assert_str_contains(seen.dead[2], 'деление на ноль')
end

g.test_ttr_hands_the_message_over_and_the_late_outcome_is_counted = function()
    local seen = on_node(function()
        local fiber = require('fiber')
        local queue = require('tnt.queue')

        local slow = queue.declare('slow', { ttr = 0.2 })
        local attempts = {}

        local consumer = slow:consume(function(message)
            table.insert(attempts, message.attempt)

            if message.attempt == 1 then
                fiber.sleep(0.4)
            end
        end, { workers = 2 })

        slow:send('долгая работа')

        t.helpers.retrying({ timeout = 15 }, function()
            assert(#attempts == 2 and slow:status().counts.late == 1, 'опоздавшего итога нет')
        end)

        consumer:stop()

        return { attempts = attempts, counts = slow:status().counts, left = box.space.slow:count() }
    end)

    t.assert_equals(seen.attempts, { 1, 2 }, 'по ttr сообщение выдано снова со счётом 2')
    t.assert_equals(seen.counts.late, 1)
    t.assert_equals(seen.counts.ack, 1)
    t.assert_equals(seen.left, 0)
end

g.test_messages_of_one_key_go_one_by_one_even_with_two_workers = function()
    local seen = on_node(function()
        local fiber = require('fiber')
        local queue = require('tnt.queue')

        local ordered = queue.declare('ordered', { ttr = 5 })
        local marks = {}

        local consumer = ordered:consume(function(message)
            table.insert(marks, message.body .. '+')
            fiber.sleep(0.05)
            table.insert(marks, message.body .. '-')
        end, { workers = 2 })

        ordered:send('a1', { key = 'a' })
        ordered:send('a2', { key = 'a' })

        t.helpers.retrying({ timeout = 15 }, function()
            assert(#marks == 4, 'обработаны не оба')
        end)

        consumer:stop()

        return { marks = table.concat(marks, ' ') }
    end)

    t.assert_equals(seen.marks, 'a1+ a1- a2+ a2-')
end

g.test_sending_inside_a_box_transaction_is_part_of_it = function()
    local seen = on_node(function()
        local queue = require('tnt.queue')

        local txn = queue.declare('txn', { ttr = 5 })
        local orders = box.schema.space.create('orders', { if_not_exists = true })

        orders:create_index('primary', { if_not_exists = true })

        box.begin()
        orders:insert({ 1 })
        txn:send({ order = 1 })
        box.rollback()

        local after_rollback = { messages = box.space.txn:count(), orders = orders:count() }

        box.begin()
        orders:insert({ 2 })
        txn:send({ order = 2 })
        box.commit()

        return {
            after_rollback = after_rollback,
            after_commit = { messages = box.space.txn:count(), orders = orders:count() },
        }
    end)

    t.assert_equals(seen.after_rollback, { messages = 0, orders = 0 }, 'откат уносит и сообщение')
    t.assert_equals(seen.after_commit, { messages = 1, orders = 1 })
end

g.test_atomic_commits_the_handler_and_the_acknowledgement_together = function()
    local seen = on_node(function()
        local fiber = require('fiber')
        local queue = require('tnt.queue')

        local txn = queue.declare('txn', { ttr = 5 })
        local effects = box.schema.space.create('effects', { if_not_exists = true })

        effects:create_index('primary', { if_not_exists = true, parts = { { 1, 'string' } } })

        local consumer = txn:consume(function(message)
            effects:insert({ message.id, message.body.order })

            if message.body.order == 3 then
                fiber.sleep(0)
            end

            if message.body.order == 4 then
                return nil, 'записывать не будем'
            end
        end, { atomic = true, max_attempts = 2, backoff = { base = 0.05, max = 1 } })

        t.helpers.retrying({ timeout = 15 }, function()
            assert(box.space.txn:count() == 0, 'сообщение транзакции не обработано')
        end)

        local yielding = txn:send({ order = 3 })

        t.helpers.retrying({ timeout = 15 }, function()
            assert(box.space.txn_dead:get(yielding) ~= nil, 'уступившее не зарыто')
        end)

        local refused = txn:send({ order = 4 })

        t.helpers.retrying({ timeout = 15 }, function()
            assert(box.space.txn_dead:get(refused) ~= nil, 'отказавшее не зарыто')
        end)

        consumer:stop()

        return {
            effects = effects:count(),
            yielded = assert(box.space.txn_dead:get(yielding)):totable(),
            refused = assert(box.space.txn_dead:get(refused)):totable(),
            written = effects:get(refused),
            counts = txn:status().counts,
        }
    end)

    t.assert_equals(
        seen.effects,
        1,
        'запись обработчика и подтверждение зафиксированы вместе'
    )
    t.assert_str_contains(
        seen.yielded[2],
        'fiber yield',
        'уступка рвёт транзакцию и считается попыткой'
    )
    t.assert_equals(seen.refused[2], 'попыток 2 из 2: записывать не будем')
    t.assert_equals(seen.written, nil, 'отказ обработчика откатывает и его запись')
    t.assert_equals(seen.counts.ack, 1)
    t.assert_equals(seen.counts.bury, 2)
end

g.test_buried_messages_are_kicked_back_with_the_attempt_count_reset = function()
    local seen = on_node(function()
        local queue = require('tnt.queue')

        local kicked = queue.declare('kicked', { ttr = 5 })
        local attempts = {}
        local fail = true

        local consumer = kicked:consume(function(message)
            table.insert(attempts, message.attempt)

            if fail then
                return nil, 'пока нет'
            end
        end, { max_attempts = 1 })

        kicked:send('дело')
        kicked:send('второе дело')

        t.helpers.retrying({ timeout = 15 }, function()
            assert(box.space.kicked_dead:count() == 2, 'зарыты не оба')
        end)

        fail = false

        -- Без аргумента возвращается ровно одно.
        local one = kicked:kick()
        local left_after_one = box.space.kicked_dead:count()
        local moved, err = kicked:kick(5)

        t.helpers.retrying({ timeout = 15 }, function()
            assert(
                #attempts == 4 and box.space.kicked:count() == 0,
                'возвращённое не обработано'
            )
        end)

        consumer:stop()

        return {
            one = one,
            left_after_one = left_after_one,
            moved = moved,
            err = err,
            attempts = attempts,
            dead = box.space.kicked_dead:count(),
            empty = kicked:kick(),
        }
    end)

    t.assert_equals(seen.one, 1, 'без аргумента возвращается одно сообщение')
    t.assert_equals(seen.left_after_one, 1, 'второе зарытое осталось лежать')
    t.assert_equals(seen.moved, 1)
    t.assert_equals(seen.err, nil)
    t.assert_equals(
        seen.attempts,
        { 1, 1, 1, 1 },
        'у возвращённого счёт выдач начинается заново'
    )
    t.assert_equals(seen.dead, 0)
    t.assert_equals(seen.empty, 0, 'зарытых нет — возвращать нечего')
end

g.test_a_delayed_message_waits_and_priority_orders_the_ready_ones = function()
    local seen = on_node(function()
        local fiber = require('fiber')
        local queue = require('tnt.queue')

        local later = queue.declare('later', { ttr = 5 })
        local order = {}

        later:send('срочное', { priority = 0 })
        later:send('обычное', { priority = 5 })
        later:send('отложенное', { delay = 0.2 })

        local delayed = later:status().depth.delayed
        local consumer = later:consume(function(message)
            table.insert(order, message.body)
        end)

        t.helpers.retrying({ timeout = 15 }, function()
            assert(#order == 3, 'обработаны не все')
        end)

        consumer:stop()
        fiber.sleep(0)

        return { order = order, delayed = delayed }
    end)

    t.assert_equals(seen.delayed, 1, 'отложенное ждёт своего срока')
    t.assert_equals(seen.order, { 'срочное', 'обычное', 'отложенное' })
end

g.test_a_message_that_expired_by_ttl_is_counted_and_logged = function()
    local seen = on_node(function()
        local fiber = require('fiber')
        local queue = require('tnt.queue')

        local short = queue.declare('short', { ttr = 5 })
        local sent = short:send('недолго', { ttl = 0.1 })

        t.helpers.retrying({ timeout = 15 }, function()
            assert(short:status().counts.expired == 1, 'истёкшее не сосчитано')
        end)

        fiber.sleep(0)

        return { sent = sent, counts = short:status().counts, left = box.space.short:count() }
    end)

    t.assert_equals(seen.counts.expired, 1)
    t.assert_equals(
        seen.left,
        0,
        'истёкшее рок удаляет молча — фасад его считает'
    )
end

g.test_a_message_retried_without_a_delay_still_expires_by_ttl = function()
    local seen = on_node(function()
        local queue = require('tnt.queue')

        local lapsing = queue.declare('lapsing', { ttr = 5 })

        ---@type TntQueueConsumer
        local consumer

        -- Получатель встаёт на первой выдаче: возвращённое лежит готовым,
        -- и истечь ему не мешает новая выдача.
        consumer = lapsing:consume(function()
            consumer:stop()

            return nil, { retry_after = 0, message = 'сразу ещё раз' }
        end)

        lapsing:send('недолго', { ttl = 1 })

        t.helpers.retrying({ timeout = 15, delay = 0.01 }, function()
            assert(lapsing:status().counts.retry == 1, 'сообщение не возвращено')
        end)

        -- Срок сверяется до ожидания: без обхода он уходит за тысячи лет,
        -- и ждать истечения, чтобы это узнать, незачем.
        local task = assert(box.space.lapsing:select()[1], 'возвращённого в трубе нет')

        t.assert_equals(
            {
                status = task.status,
                ttl = tonumber(task.ttl),
                expires = tonumber(task.next_event - task.created),
            },
            { status = 'r', ttl = 1000000, expires = 1000000 },
            'готово сразу, ttl не продлён, срок удаления — отправка плюс ttl'
        )

        t.helpers.retrying({ timeout = 15 }, function()
            assert(lapsing:status().counts.expired == 1, 'истёкшее не сосчитано')
        end)

        return { left = box.space.lapsing:count() }
    end)

    t.assert_equals(seen.left, 0, 'по сроку сообщение выброшено')
end

g.test_a_retry_without_a_delay_keeps_the_key_and_a_delayed_one_lets_it_go = function()
    local seen = on_node(function()
        local queue = require('tnt.queue')

        local keyed = queue.declare('keyed', { ttr = 5 })
        local order = { now = {}, later = {} }

        keyed:send('now1', { key = 'now' })
        keyed:send('now2', { key = 'now' })
        keyed:send('later1', { key = 'later' })
        keyed:send('later2', { key = 'later' })

        -- Первое сообщение ключа отказывает один раз: у now повтор
        -- без отсрочки, у later — с отсрочкой.
        local consumer = keyed:consume(function(message)
            table.insert(order[message.key], ('%s#%d'):format(message.body, message.attempt))

            if message.attempt == 1 and message.body:sub(-1) == '1' then
                return nil, { retry_after = message.key == 'now' and 0 or 0.1 }
            end
        end)

        t.helpers.retrying({ timeout = 15 }, function()
            assert(#order.now == 3 and #order.later == 3, 'обработаны не все')
        end)

        consumer:stop()

        return order
    end)

    t.assert_equals(
        seen.now,
        { 'now1#1', 'now1#2', 'now2#1' },
        'возврат без отсрочки ключ не отпускает'
    )
    t.assert_equals(
        seen.later,
        { 'later1#1', 'later2#1', 'later1#2' },
        'на время отсрочки ключ свободен'
    )
end

g.test_a_foreign_task_in_the_tube_is_buried_at_once = function()
    local seen = on_node(function()
        local queue = require('tnt.queue')

        local mixed = queue.declare('mixed', { ttr = 5 })
        local handled = 0

        -- Кто-то положил в трубу мимо фасада: конверта нет. Все три
        -- вида не конверт по-своему: не таблица вовсе, таблица без
        -- опознавателя и таблица без счёта выдач.
        local rock = require('queue') --[[@as table]]

        ---@type table
        ---@diagnostic disable-next-line: undefined-field
        local foreign = rock.tube.mixed

        foreign:put('чужое')
        foreign:put(42)
        foreign:put({ attempt = 1, body = 'без опознавателя' })
        foreign:put({ id = 'есть', body = 'без счёта выдач' })

        local consumer = mixed:consume(function()
            handled = handled + 1
        end)

        t.helpers.retrying({ timeout = 15 }, function()
            assert(box.space.mixed_dead:count() == 4, 'чужое зарыто не всё')
        end)

        consumer:stop()

        local bodies = {}

        for _, row in box.space.mixed_dead:pairs() do
            table.insert(bodies, tostring(type(row[5].body) == 'table' and row[5].body.body or row[5].body))
        end

        table.sort(bodies)

        local dead = assert(box.space.mixed_dead:select({})[1])

        return { handled = handled, reason = dead[2], bodies = bodies, left = box.space.mixed:count() }
    end)

    t.assert_equals(seen.handled, 0, 'чужое до обработчика не доходит')
    t.assert_equals(seen.reason, 'в трубе не конверт tnt-queue')
    t.assert_equals(
        seen.bodies,
        { '42', 'без опознавателя', 'без счёта выдач', 'чужое' }
    )
    t.assert_equals(seen.left, 0)
end

g.test_deliveries_above_the_ceiling_are_buried_without_the_handler = function()
    local seen = on_node(function()
        local queue = require('tnt.queue')

        local looping = queue.declare('looping', { ttr = 5 })
        local handled = 0
        local sent = looping:send('роняет процесс')

        -- Так выглядит сообщение, которое роняло процесс: выдачи были,
        -- а итог не пришёл ни разу.
        local task = assert(box.space.looping.index.task_id:min())

        box.space.looping:update(task[1], { { '=', '[9].attempt', 9 } })

        local consumer = looping:consume(function()
            handled = handled + 1
        end, { max_attempts = 3 })

        t.helpers.retrying({ timeout = 15 }, function()
            assert(box.space.looping_dead:get(sent) ~= nil, 'сообщение не зарыто')
        end)

        consumer:stop()

        return { handled = handled, reason = assert(box.space.looping_dead:get(sent))[2] }
    end)

    t.assert_equals(seen.handled, 0)
    t.assert_equals(
        seen.reason,
        'выдач 10 при потолке 3: прежние выдачи итога не вернули'
    )
end

g.test_touch_extends_the_ttr_of_the_message_in_hand = function()
    local seen = on_node(function()
        local fiber = require('fiber')
        local queue = require('tnt.queue')

        local long = queue.declare('long', { ttr = 0.2 })
        local attempts = {}
        local outcomes = {}

        local consumer = long:consume(function(message)
            table.insert(attempts, message.attempt)

            if message.attempt == 1 then
                table.insert(outcomes, { long:touch(message, 5) })

                -- Из соседнего файбера продлить нельзя: у рока своя
                -- сессия у каждого файбера.
                local neighbour = fiber.create(function()
                    table.insert(outcomes, { long:touch(message, 5) })
                end)

                while neighbour:status() ~= 'dead' do
                    fiber.sleep(0.01)
                end

                fiber.sleep(0.35)
            end
        end, { workers = 2 })

        long:send('долгая работа')

        t.helpers.retrying({ timeout = 15 }, function()
            assert(box.space.long:count() == 0, 'сообщение не подтверждено')
        end)

        consumer:stop()

        ---@type TntQueueMessage
        ---@diagnostic disable-next-line: missing-fields
        local absent = { id = 'нет такого' }
        local missing = assert(select(2, long:touch(absent, 1)))

        local aside = outcomes[2][2]

        return {
            attempts = attempts,
            touched = outcomes[1],
            aside = { kind = aside.kind, sent = aside.sent, message = aside.message },
            counts = long:status().counts,
            missing = { kind = missing.kind, message = missing.message, retriable = missing.retriable },
        }
    end)

    t.assert_equals(seen.touched, { true }, 'продление принято')
    t.assert_equals(seen.aside.kind, 'rejected', 'из соседнего файбера продлить нельзя')
    t.assert_equals(seen.aside.sent, false, 'продление до очереди не дошло')
    t.assert_str_contains(seen.aside.message, 'Task was not taken')
    t.assert_equals(seen.attempts, { 1 }, 'продлённый ttr не отдал сообщение соседу')
    t.assert_equals(seen.counts.late, 0)
    t.assert_equals(seen.missing.kind, 'rejected')
    t.assert_equals(seen.missing.retriable, false)
    t.assert_str_contains(seen.missing.message, 'не в работе у очереди long')
end

g.test_a_body_that_is_not_of_the_shape_is_refused_and_buried = function()
    local seen = on_node(function()
        local data = require('tnt.data')
        local queue = require('tnt.queue')

        local Letter = data.define({
            name = 'Letter',
            fields = {
                { 'to', 'string', min = 3 },
            },
        })

        local shaped = queue.declare('shaped', { ttr = 5, shape = Letter })
        local handled = 0
        local _, refused = pcall(shaped.send, shaped, { to = 7 })

        -- Мимо формы кладём сами: так выглядит сообщение, отправленное
        -- прежней сборкой приложения.
        local rock = require('queue') --[[@as table]]

        ---@diagnostic disable-next-line: undefined-field
        rock.tube.shaped:put({
            id = 'ULID-ПРЕЖНЕЙ-СБОРКИ',
            name = 'shaped',
            body = { to = 7 },
            headers = {},
            attempt = 0,
            created = 0,
        })

        local consumer = shaped:consume(function()
            handled = handled + 1
        end)

        t.helpers.retrying({ timeout = 15 }, function()
            assert(box.space.shaped_dead:count() == 1, 'негодное тело не зарыто')
        end)

        consumer:stop()

        return {
            refused = tostring(refused),
            handled = handled,
            reason = assert(box.space.shaped_dead:select({})[1])[2],
            good = shaped:send({ to = 'a@example.org' }) ~= nil,
        }
    end)

    t.assert_str_contains(seen.refused, 'тело для очереди shaped не по форме: to:')
    t.assert_equals(seen.handled, 0)
    t.assert_str_contains(seen.reason, 'тело не по форме: to:')
    t.assert_equals(seen.good, true)
end

g.test_a_step_that_broke_is_counted_and_the_worker_lives_on = function()
    local seen = on_node(function()
        local fiber = require('fiber')
        local queue = require('tnt.queue')

        local broken = queue.declare('broken', { ttr = 5 })
        local consumer = broken:consume(function() end)

        -- Спейс трубы унесли из-под рока: взятие бросает на каждом шаге.
        -- Считать сбои приходится по полю очереди: `status` спрашивает
        -- у рока глубину, а тот без спейса трубы бросает сам.
        box.space.broken:drop()

        t.helpers.retrying({ timeout = 15 }, function()
            assert(broken.counts.failures >= 2, 'сбои шага не сосчитаны')
        end)

        consumer:stop()

        t.helpers.retrying({ timeout = 15 }, function()
            assert(consumer.workers == 0, 'работник не кончился')
        end)

        fiber.sleep(0)

        return { failures = broken.counts.failures, workers = consumer.workers }
    end)

    t.assert_ge(seen.failures, 2, 'сбой шага не убивает работника')
    t.assert_equals(
        seen.workers,
        0,
        'остановленный получатель работников не держит'
    )
end

g.test_declaring_over_a_foreign_space_is_raised_and_leaves_no_lock = function()
    local seen = on_node(function()
        local queue = require('tnt.queue')

        local space = box.schema.space.create('occupied', { if_not_exists = true })

        space:create_index('primary', { if_not_exists = true })

        local first = select(2, pcall(queue.declare, 'occupied'))
        local other = select(2, pcall(queue.declare, 'occupied', { ttr = 7 }))
        local second = select(2, pcall(queue.declare, 'occupied'))

        return { first = tostring(first), other = tostring(other), second = tostring(second) }
    end)

    t.assert_str_contains(seen.first, 'does not have "task_id" index')
    t.assert_equals(
        seen.other,
        seen.first,
        'неудачное объявление очередь не объявило: другие настройки — не повтор'
    )
    t.assert_equals(
        seen.second,
        seen.first,
        'неудачное объявление не оставило очередь запертой'
    )
end

g.test_a_failed_opening_keeps_the_queue_declared_before = function()
    local seen = on_node(function()
        local queue = require('tnt.queue')

        -- Подмена ставится внешней зависимостью и в объявлении модуля не видна.
        local tube = require('tnt.queue.tube') --[[@as any]]

        -- Объявили на узле для чтения: очередь есть, трубы ещё нет.
        tube._set_source({
            writable = function()
                return false
            end,
        })

        local first = queue.declare('unopened', { ttr = 5 })

        tube._set_source(nil)

        -- Пока узел не писал, имя трубы занял чужой спейс: заведение бросает.
        local space = box.schema.space.create('unopened')

        space:create_index('primary', {})

        local refused = select(2, pcall(queue.declare, 'unopened', { ttr = 5 }))

        space:drop()

        local again = queue.declare('unopened', { ttr = 5 })

        return { refused = tostring(refused), same = rawequal(again, first), state = again:status().state }
    end)

    t.assert_str_contains(seen.refused, 'does not have "task_id" index')
    t.assert_equals(
        seen.same,
        true,
        'объявленную раньше очередь отказ заведения не забыл'
    )
    t.assert_equals(seen.state, 'RUNNING')
end

g.test_two_declarations_of_one_name_are_one_queue_with_one_count = function()
    local seen = on_node(function()
        local queue = require('tnt.queue')

        -- Отправитель и получатель объявляют очередь каждый у себя.
        local sender = queue.declare('shared', { ttr = 5 })
        local receiver = queue.declare('shared', { ttr = 5 })
        local touched = {}

        local consumer = receiver:consume(function(message)
            table.insert(touched, { sender:touch(message, 5) })
        end)

        sender:send('работа')

        t.helpers.retrying({ timeout = 15 }, function()
            assert(receiver:status().counts.ack == 1, 'сообщение не обработано')
        end)

        consumer:stop()

        -- Работник, ждущий задачи, взял бы и сообщение, которое должно истечь.
        t.helpers.retrying({ timeout = 15 }, function()
            assert(consumer.workers == 0, 'работник не кончился')
        end)

        sender:send('недолго', { ttl = 0.1 })

        t.helpers.retrying({ timeout = 15 }, function()
            assert(receiver:status().counts.expired == 1, 'истёкшее не сосчитано')
        end)

        return {
            same = rawequal(sender, receiver),
            touched = touched,
            sender = sender:status().counts,
            receiver = receiver:status().counts,
        }
    end)

    t.assert_equals(seen.same, true, 'второе объявление — та же очередь')
    t.assert_equals(
        seen.touched,
        { { true } },
        'продлить можно через любое объявление'
    )
    t.assert_equals(seen.sender, seen.receiver, 'счёт у объявлений один')
    t.assert_equals(seen.sender.sent, 2)
    t.assert_equals(seen.sender.ack, 1)
    t.assert_equals(seen.sender.expired, 1)
end

g.test_after_reset_the_new_declaration_takes_over_the_tube = function()
    local seen = on_node(function()
        local queue = require('tnt.queue')

        local before = queue.declare('reloaded', { ttr = 5 })
        local consumer = before:consume(function() end)

        queue.reset()

        -- Перечитанный код объявляет очередь заново, и ttr у него другой.
        local after = queue.declare('reloaded', { ttr = 7 })

        t.helpers.retrying({ timeout = 15 }, function()
            assert(
                consumer.workers == 0,
                'получатель забытой очереди не остановился'
            )
        end)

        after:send('недолго', { ttl = 0.1 })

        t.helpers.retrying({ timeout = 15 }, function()
            assert(after:status().counts.expired == 1, 'истёкшее не сосчитано')
        end)

        return { ttr = after.ttr, before = before:status().counts, after = after:status().counts }
    end)

    t.assert_equals(
        seen.ttr,
        7,
        'забытое имя объявляется с другими настройками'
    )
    t.assert_equals(seen.after.sent, 1)
    t.assert_equals(seen.after.expired, 1, 'крюк рока считает для нового объявления')
    t.assert_equals(
        seen.before.expired,
        0,
        'забытое объявление крюк рока больше не держит'
    )
end

g.test_while_a_tube_is_being_created_the_second_declaration_waits = function()
    local seen = on_node(function()
        local fiber = require('fiber')
        local queue = require('tnt.queue')

        -- Заведение трубы — DDL, и оно уступает: пока первый файбер стоит
        -- на нём, второй не должен лезть в недоделанный спейс.
        local racer = fiber.create(function()
            rawset(_G, 'raced', queue.declare('raced', { ttr = 5 }))
        end)

        local second = queue.declare('raced', { ttr = 5 })
        local during = { second.tube:open() }

        t.helpers.retrying({ timeout = 15 }, function()
            assert(racer:status() == 'dead', 'первое объявление не кончилось')
        end)

        local after = { second.tube:open() }

        return {
            during = { during[1], during[2] },
            after = after[1],
            same = rawequal(second, rawget(_G, 'raced')),
            sent = second:send('после гонки') ~= nil,
        }
    end)

    t.assert_equals(
        seen.during,
        { false, 'труба ещё не заведена' },
        'вторая не лезет в чужое заведение'
    )
    t.assert_equals(seen.after, true, 'заведённую трубу берут как свою')
    t.assert_equals(seen.same, true, 'объявление в окне заведения — та же очередь')
    t.assert_equals(seen.sent, true)
end

g.test_wrong_declaration_and_calls_are_raised_at_the_caller = function()
    local seen = on_node(function()
        local queue = require('tnt.queue')

        local said = {}
        local outer = queue.declare('outer', { ttr = 5 })

        local function say(name, fn, ...)
            said[name] = tostring(select(2, pcall(fn, ...)))
        end

        say('name', queue.declare, '_queue')
        say('ttr', queue.declare, 'wrong', { ttr = 0 })
        say('century', queue.declare, 'wrong', { ttr = 3153600000 })
        say('unknown', queue.declare, 'wrong', { ttl = 5 })
        say('shape', queue.declare, 'wrong', { shape = {} })
        say('handler', outer.consume, outer, 'не функция')

        -- Вина за возврат зарытых в чужой транзакции — на строке
        -- вызывающего, поэтому зовём из замыкания, а не прямо из pcall:
        -- у кадра C места нет, и уровень броска в нём не видно.
        box.begin()
        say('kick', function()
            outer:kick()
        end)
        box.rollback()

        say('count', outer.kick, outer, 0)
        say('touch', outer.touch, outer, { id = 'x' }, 0)

        return said
    end)

    t.assert_str_contains(seen.name, 'имя очереди — строка по образцу')
    t.assert_str_contains(seen.ttr, 'настройки очереди.ttr — число больше 0, а не 0')
    t.assert_str_contains(seen.century, 'настройки очереди.ttr — число меньше 3153600000')
    t.assert_str_contains(
        seen.unknown,
        'настройки очереди: ключа «ttl» нет, есть shape, ttr'
    )
    t.assert_str_contains(seen.shape, 'настройки очереди.shape.from — функция')
    t.assert_str_contains(seen.handler, 'обработчик — функция')
    t.assert_str_contains(
        seen.kick,
        'queue_node_test.lua:',
        'место броска — строка вызывающего'
    )
    t.assert_str_contains(seen.kick, 'возврат зарытых идёт своей транзакцией')
    t.assert_str_contains(seen.count, 'сколько вернуть — число больше 0, а не 0')
    t.assert_str_contains(seen.touch, 'продление — число больше 0, а не 0')
end

g.test_hooks_wrap_sending_and_handling_on_the_node = function()
    local seen = on_node(function()
        local queue = require('tnt.queue')

        local hooked = queue.declare('hooked', { ttr = 5 })
        local calls = {}

        queue.hook('проверка', function(call, proceed)
            local answer = proceed()

            table.insert(calls, { kind = call.kind, name = call.name, id = call.message.id })

            return answer
        end)

        local consumer = hooked:consume(function() end)
        local sent = hooked:send('под крюком')

        t.helpers.retrying({ timeout = 15 }, function()
            assert(#calls == 2, 'крюк сработал не на обоих концах')
        end)

        consumer:stop()

        local names = queue.hooks()
        local shown = hooked:status().hooks

        queue.hook('проверка', nil)

        return { calls = calls, sent = sent, names = names, shown = shown, after = queue.hooks() }
    end)

    t.assert_equals(seen.calls[1], { kind = 'send', name = 'hooked', id = seen.sent })
    t.assert_equals(seen.calls[2], { kind = 'handle', name = 'hooked', id = seen.sent })
    t.assert_equals(seen.names, { 'проверка' })
    t.assert_equals(seen.shown, { 'проверка' }, 'крюки видны в состоянии очереди')
    t.assert_equals(seen.after, {}, 'снятый крюк уходит из списка')
end

g.test_a_queue_of_features_and_defaults_is_declared_as_the_contract_says = function()
    local seen = on_node(function()
        local queue = require('tnt.queue')

        local plain = queue.declare('plain')

        return {
            features = queue.features,
            ttr = plain.ttr,
            status = plain:status(),
        }
    end)

    t.assert_equals(seen.features, {
        delay = true,
        ttl = true,
        ttr = true,
        touch = true,
        priority = true,
        key = true,
        transactional = true,
        atomic = true,
    })
    t.assert_equals(seen.ttr, 60, 'умолчание ttr — 60 с')
    t.assert_equals(seen.status.counts, {
        sent = 0,
        ack = 0,
        retry = 0,
        bury = 0,
        late = 0,
        expired = 0,
        failures = 0,
        idle_waits = 0,
    })
    t.assert_equals(seen.status.workers, 0)
end
