--- Фасад очереди без box: проверки аргументов с местом вызывающего и отказ,
--- когда труба ещё не заведена.
---
--- В процессе проверок `box.cfg` не звали, и труба не заводится: этого
--- хватает, чтобы проверить всё, что происходит до неё, — а вина
--- за негодный аргумент обязана стоять на строке вызывающего, а не внутри
--- пакета.

local t = require('luatest')

local helper = dofile('test/helper.lua')

local queue = helper.queue

local g = t.group('tnt.queue.facade')

g.test_a_queue_is_declared_with_the_defaults_of_the_contract = function()
    local short = queue.declare('q')

    t.assert_equals(short.name, 'q', 'имя из одной буквы — тоже имя')
    t.assert_equals(short.ttr, 60)
    t.assert_equals(short.shape, nil)
    t.assert_equals(short:status().state, 'CLOSED', 'труба не заведена: очередь закрыта')
    t.assert_equals(short:status().depth, nil, 'у закрытой очереди глубины нет')
end

g.test_a_second_declaration_is_the_same_queue_and_its_settings_must_agree = function()
    local shape = {
        from = function(body)
            return body
        end,
    }

    local first = queue.declare('twice', { ttr = 60, shape = shape })

    t.assert_is(
        queue.declare('twice', { shape = shape }),
        first,
        'умолчание ttr сравнивается значением'
    )

    helper.assert_blamed({
        {
            function()
                queue.declare('twice', { ttr = 0.5, shape = shape })
            end,
            'очередь twice уже объявлена с ttr 60, а не 0.5',
        },
        {
            function()
                queue.declare('twice', { ttr = 60 })
            end,
            'очередь twice уже объявлена с другой формой тела',
        },
        {
            -- Форма сравнивается таблицей: такая же, но другая — другая.
            function()
                queue.declare('twice', { shape = { from = shape.from } })
            end,
            'очередь twice уже объявлена с другой формой тела',
        },
    })

    t.assert_is(
        queue.declare('twice', { shape = shape }),
        first,
        'отказ повтора очередь не забыл'
    )
end

g.test_reset_forgets_the_declarations_and_stops_their_consumers = function()
    local before = queue.declare('renewed', { ttr = 5 })
    local consumer = before:consume(function() end, { workers = 2 })

    queue.reset()

    local after = queue.declare('renewed', { ttr = 7 })

    t.assert_is_not(after, before, 'забытое имя объявляется заново')
    t.assert_equals(after.ttr, 7, 'и с другими настройками')
    t.assert_equals(after:status().counts.sent, 0)
    t.assert_equals(consumer.stopping, true, 'получатель забытой очереди остановлен')

    t.helpers.retrying({ timeout = 5 }, function()
        assert(consumer.workers == 0, 'работники забытой очереди ещё идут')
    end)
end

g.test_before_the_tube_is_opened_the_queue_refuses_with_a_pair = function()
    local waiting = queue.declare('waiting', { ttr = 5 })
    local sent, err = waiting:send('работа')

    t.assert_equals(sent, nil)
    t.assert_equals(err.kind, 'unreachable')
    t.assert_equals(err.sent, false)
    t.assert_equals(err.retriable, true)
    t.assert_equals(
        tostring(err),
        'очередь waiting не принимает: узел только для чтения'
    )
    t.assert_equals(waiting:status().counts.sent, 0)
end

g.test_wrong_arguments_are_raised_at_the_caller = function()
    local declared = queue.declare('declared', { ttr = 5 })

    helper.assert_blamed({
        {
            function()
                queue.declare('_queue')
            end,
            'имя очереди — строка по образцу ^%a[%w_]*$, а не «_queue»',
        },
        {
            function()
                queue.declare('wrong', { ttr = 0 })
            end,
            'настройки очереди.ttr — число больше 0, а не 0',
        },
        {
            function()
                queue.declare('wrong', { ttr = 3153600000 })
            end,
            'настройки очереди.ttr — число меньше 3153600000, а не 3153600000',
        },
        {
            function()
                queue.declare('wrong', { ttl = 5 })
            end,
            'настройки очереди: ключа «ttl» нет, есть shape, ttr',
        },
        {
            function()
                queue.declare('wrong', { shape = {} })
            end,
            'настройки очереди.shape.from — функция или вызываемая таблица, а не nil',
        },
        {
            function()
                declared:send('тело', { priorty = 1 })
            end,
            'настройки отправки: ключа «priorty» нет, есть delay, headers, id, key, priority, timeout, ttl',
        },
        {
            function()
                declared:send(require('uuid').new())
            end,
            'тело — простые данные, а не cdata',
        },
        {
            -- Конверт и кортеж рока кладут тело ещё на два уровня глубже,
            -- и кодек отказал бы уже в трубе — отказом «сообщение ушло».
            function()
                declared:send(helper.deep(126))
            end,
            'тело — вложенность глубже 100 таблиц',
        },
        {
            function()
                declared:send('тело', { atomic = true })
            end,
            'настройки отправки: ключа «atomic» нет, есть delay, headers, id, key, priority, timeout, ttl',
        },
        {
            function()
                declared:consume(helper.wrong('не функция'))
            end,
            'обработчик — функция или вызываемая таблица, а не строка',
        },
        {
            function()
                declared:consume(print, { worker = 1 })
            end,
            'настройки получателя: ключа «worker» нет, есть atomic, backoff, max_attempts, workers',
        },
        {
            function()
                declared:kick(0)
            end,
            'сколько вернуть — число больше 0, а не 0',
        },
        {
            function()
                declared:kick(helper.wrong('все'))
            end,
            'сколько вернуть — целое число, а не строка',
        },
        {
            function()
                declared:touch(helper.wrong('сообщение'), 1)
            end,
            'сообщение — таблица, а не строка',
        },
        {
            function()
                declared:touch({ id = 'x' }, 0)
            end,
            'продление — число больше 0, а не 0',
        },
        {
            function()
                declared:touch({ id = 'x' }, 3153600000)
            end,
            'продление — число меньше 3153600000, а не 3153600000',
        },
    })
end

g.test_a_body_that_is_not_of_the_shape_is_refused_before_sending = function()
    local shape = {
        from = function(body)
            if type(body) ~= 'table' or type(body.to) ~= 'string' then
                return nil, { to = 'должно быть строкой' }
            end

            return body
        end,
    }

    local shaped = queue.declare('shaped_facade', { ttr = 5, shape = shape })

    helper.assert_blamed({
        {
            function()
                shaped:send({ to = 7 })
            end,
            'тело для очереди shaped_facade не по форме: to: должно быть строкой',
        },
    })

    t.assert_equals(
        select(2, shaped:send({ to = 'a@example.org' })).kind,
        'unreachable',
        'годное тело идёт дальше'
    )
end

g.test_touching_a_message_that_is_not_in_hand_is_refused = function()
    local declared = queue.declare('touching', { ttr = 5 })
    local touched, err = declared:touch({ id = 'нет такого' }, 5)

    t.assert_equals(touched, nil)
    t.assert_equals(err.kind, 'rejected')
    t.assert_equals(err.sent, false)
    t.assert_equals(err.retriable, false)
    t.assert_equals(
        tostring(err),
        'сообщение нет такого не в работе у очереди touching'
    )
end

g.test_the_features_of_the_queue_are_the_ones_of_the_contract = function()
    t.assert_equals(queue.features, {
        delay = true,
        ttl = true,
        ttr = true,
        touch = true,
        priority = true,
        key = true,
        transactional = true,
        atomic = true,
    })
    t.assert_equals(queue.DEFAULT_TTR, 60)
end
