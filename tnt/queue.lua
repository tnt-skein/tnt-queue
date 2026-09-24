--- Очередь заданий в спейсах: фасад над роком `queue` — конверт
--- с опознавателем отправителя, итог обработчика, зарытые, отказ парой.
---
---     local queue = require('tnt.queue')
---
---     local mail = queue.declare('mail', { ttr = 30 })
---
---     local sent, err = mail:send({ to = 'a@example.org' })
---
---     local consumer = mail:consume(function(message)
---         local ok, reason = deliver(message.body)
---
---         if not ok then
---             return nil, reason              -- повтор с отсрочкой, потом зарыть
---         end
---     end, { workers = 2 })
---
--- **Работа внутри — рок `queue 1.5.0`**: очередь в спейсах, сроки, ключ
--- и возврат. Фасад задаёт договор и закрывает то, где рок молча теряет
--- или отказывает не парой: отказ парой вместо голого `nil` и исключений,
--- ожидание `RUNNING` вместо цикла без уступки, свой опознаватель вместо
--- номера задачи, который после опустошения трубы начинается заново,
--- зарытые в своём спейсе, потому что `ttl` рока стирает и их, конечный
--- `ttr` вместо ста лет по умолчанию, ключ по умолчанию вместо одной
--- цепочки на всех безключевых и ленивая загрузка рока, который на узле
--- для чтения подменяет `box.cfg`.
---
--- **Отправка в транзакции box — её часть**: откат уносит и сообщение.
--- Это главный повод держать очередь в спейсе — «сохранили и поставили
--- работу» одной транзакцией, без ящика исходящих.
---
--- **Подтверждение — итог обработчика**, а не вызов, который можно
--- забыть: значение или ничего — подтвердить, `nil, err` — вернуть
--- с отсрочкой, после `max_attempts` — зарыть.
---
--- **Гарантия — хотя бы раз**: повтор приходит по `ttr` и после
--- перезапуска, а потеря бывает только по `ttl`, и она считается. Повтор
--- отсекает получатель по `message.id` — отметкой в той же транзакции,
--- что и запись обработчика, или `atomic = true`.
---
--- **Имя — одна очередь на процесс.** Очередь объявляют там, где она нужна:
--- отправитель в одном пакете, получатель в другом. Второе объявление
--- отдаёт ту же очередь, а не вторую над той же трубой: крюк
--- `on_task_change` рок держит один на трубу, и `expired` считало бы
--- последнее объявление, `sent` и `ack` — каждое своё, а `touch` знал бы
--- только сообщения в работе у своего объекта. Поэтому и настройки у очереди
--- одни: другие — исключение, а `reset` забывает объявления при перечитывании
--- кода.
---
--- Зависимость от пакета приходит аргументом, внешняя — через `tnt-external`:
--- рок берётся по имени как внешняя зависимость, а не `require` в шапке,
--- и очередь — значение `declare`, а не запись в контейнере.

local message = require('tnt.message')
local must = require('tnt.must')
local fail = require('tnt.must.fail')

local failure = require('tnt.storage.failure')
local series = require('tnt.queue.series')
local tube_of = require('tnt.queue.tube')
local worker = require('tnt.queue.worker')

--- Крюки отправки и обработки: список пакета, общий с работником.
local hooks = message.hooks('tnt.queue')

local Module = {}

--- Умолчание `ttr`: столько у обработчика на одно сообщение.
Module.DEFAULT_TTR = 60

--- Что умеет эта очередь. Настройка, которой очередь не умеет, —
--- исключение, а не молчаливый пропуск; здесь таких нет.
Module.features = {
    delay = true,
    ttl = true,
    ttr = true,
    touch = true,
    priority = true,
    key = true,
    transactional = true,
    atomic = true,
}

--- Ставит, заменяет или снимает крюк отправки и обработки.
Module.hook = hooks.set

--- Имена поставленных крюков по порядку.
Module.hooks = hooks.names

--- Имя очереди: им же зовётся спейс трубы, а с приставкой — спейс зарытых.
local NAME = '^%a[%w_]*$'

--- Настройки объявления.
local DECLARE = { ttr = '?number', shape = '?table' }

--- Коды box, у которых отправка не дошла и повторять её в одиночку
--- бессмысленно: откат синхронной транзакции уносит всё, что за ней.
---@diagnostic disable-next-line: undefined-field
local CONFLICTS = { [box.error.SYNC_ROLLBACK] = true, [box.error.SYNC_QUORUM_TIMEOUT] = true }

---@class TntQueueDeclareOptions
---@field ttr number|nil Сколько у обработчика на одно сообщение, секунды
---@field shape table|nil Форма тела `tnt-data`: проверяется при отправке и на входе получателя

---@class TntQueue Объявленная очередь
---@field name string Имя очереди
---@field ttr number Срок обработки одного сообщения, секунды
---@field shape table|nil Форма тела
---@field tube TntQueueTube Труба рока
---@field counts table<string, integer> Счётчики
---@field handling table<string, integer> Что сейчас в работе: опознаватель → номер задачи
---@field consumers TntQueueConsumer[] Объявленные получатели
local Queue = {}
Queue.__index = Queue

--- Объявленные очереди по имени. Держит их до `reset`: очередь, которую
--- объявили и отпустили, при следующем объявлении иначе завелась бы заново
--- со счётом с нуля.
---@type table<string, TntQueue>
local declared = {}

--- Отказ рока или box при отправке: род по коду box.
---@param err any Брошенное
---@param idempotent boolean Повтор узнаётся по готовому опознавателю
---@return TntStorageFailure
local function refused(err, idempotent)
    local text = failure.text(err)

    ---@diagnostic disable-next-line: undefined-field
    if box.error.is(err) and CONFLICTS[err.code] then
        return failure.new('conflict', text, { sent = false })
    end

    return failure.new('broken', text, { idempotent = idempotent })
end

--- Отказ «очередь не принимает»: узел для чтения, смена ведущего, подъём.
---@param queue TntQueue
---@param why string|nil
---@return TntStorageFailure
local function unreachable(queue, why)
    return failure.new(
        'unreachable',
        ('очередь %s не принимает: %s'):format(queue.name, tostring(why)),
        { sent = false }
    )
end

--- Объявляет очередь либо отдаёт уже объявленную под этим именем. Труба
--- заводится сразу, если узел пишет; иначе — при первом деле, когда он
--- начнёт писать.
---
--- Повтор с теми же настройками отдаёт ту же очередь, с другими —
--- исключение: у очереди два хозяина, и решать, чьи настройки верны,
--- некому. Умолчание `ttr` сравнивается значением, а форма — таблицей.
---@param name string Имя очереди
---@param opts TntQueueDeclareOptions|nil
---@return TntQueue
function Module.declare(name, opts)
    local caller = must.at(2)

    caller.matches(name, 'имя очереди', NAME)

    local given = caller.optional.options(opts, 'настройки очереди', DECLARE) or {}

    caller.optional.positive(given.ttr, 'настройки очереди.ttr')
    caller.optional.less_than(given.ttr, 'настройки очереди.ttr', message.MAX_SECONDS)

    if given.shape ~= nil then
        caller.callable(given.shape.from, 'настройки очереди.shape.from')
    end

    local ttr = given.ttr or Module.DEFAULT_TTR
    local queue = declared[name]
    local fresh = queue == nil

    if queue == nil then
        queue = setmetatable({
            name = name,
            ttr = ttr,
            shape = given.shape,
            counts = { sent = 0, ack = 0, retry = 0, bury = 0, late = 0, expired = 0, failures = 0, idle_waits = 0 },
            handling = {},
            consumers = {},
        }, Queue)

        queue.tube = tube_of.new(name, queue.counts)

        -- В реестр — до заведения трубы: заведение уступает, и объявление,
        -- пришедшее в это окно, обязано получить эту же очередь, а не
        -- завести вторую и переставить на себя крюк рока.
        declared[name] = queue
    elseif queue.ttr ~= ttr then
        error(('очередь %s уже объявлена с ttr %s, а не %s'):format(name, queue.ttr, ttr), 2)
    elseif not rawequal(queue.shape, given.shape) then
        error(('очередь %s уже объявлена с другой формой тела'):format(name), 2)
    end

    local ok, err = pcall(queue.tube.open, queue.tube)

    if not ok then
        -- Объявление, не заведшее трубу, очередь не объявило: следующее
        -- заводит её заново и вправе прийти с другими настройками.
        -- Объявленную раньше не забываем: её держит тот, кто объявил.
        if fresh then
            declared[name] = nil
        end

        fail.raise(err)
    end

    series.watch(queue)

    return queue
end

--- Забывает объявленные очереди и останавливает их получателей.
---
--- Нужен проверкам и перечитыванию кода: модуль, загруженный заново,
--- объявляет очередь ещё раз — бывает, с другими настройками, — и заводит
--- получателя, который иначе встал бы рядом с прежним, и сообщения брал бы
--- и прежний обработчик. Трубы и сообщения остаются в спейсах, а крюки —
--- в списке пакета: их снимает `queue.hook(имя, nil)`.
function Module.reset()
    for _, queue in pairs(declared) do
        for _, consumer in ipairs(queue.consumers) do
            consumer:stop()
        end
    end

    declared = {}
end

--- Отказ отправки: считается в ряду по роду и отдаётся парой.
---@param queue TntQueue
---@param err TntStorageFailure
---@return nil
---@return TntStorageFailure
local function refuse(queue, err)
    series.refused(queue.name, err)

    return nil, err
end

--- Кладёт конверт в трубу под крюками.
---@param queue TntQueue
---@param envelope TntQueueMessage
---@param opts TntQueueSendOptions
---@return string|nil id
---@return TntStorageFailure|nil err
local function put(queue, envelope, opts)
    local ready, why = queue.tube:open()

    if not ready then
        return refuse(queue, unreachable(queue, why))
    end

    -- Рок открывает вокруг `put` свою транзакцию и на броске оставляет её
    -- открытой: она утекла бы вызывающему, и iproto ответил бы ему
    -- «Transaction is active at return from function». Чужую транзакцию
    -- (отправка — её часть) откатывает вызывающий, а не мы.
    local theirs = box.is_in_txn()
    local ok, err = pcall(queue.tube.put, queue.tube, envelope, opts, queue.ttr)

    if not ok then
        if not theirs and box.is_in_txn() then
            box.rollback()
        end

        return refuse(queue, refused(err, opts.id ~= nil))
    end

    series.count(queue, 'sent')

    return envelope.id
end

--- Отправляет сообщение и отдаёт его опознаватель.
---
--- В транзакции box отправка — её часть: откат уносит и сообщение,
--- а счёт отправленных растёт в миг вызова, не дожидаясь фиксации.
---@param body any Тело — простые данные
---@param opts TntMessageSendOptions|nil
---@return string|nil id
---@return TntStorageFailure|nil err
function Queue:send(body, opts)
    local given = message.options(opts, Module.features, 2)

    message.body(body, 2)
    message.conform(self.shape, body, ('очереди %s'):format(self.name), 2)

    local call = { kind = message.SEND, name = self.name }

    return hooks.around(call, function()
        local envelope = message.envelope(self.name, body, given)

        call.message = envelope

        -- Крюк отдаёт итог отправки как есть: пару «опознаватель, отказ».
        ---@diagnostic disable-next-line: redundant-return-value
        return put(self, envelope, given)
    end)
end

--- Объявляет получателя: работники берут сообщения и исполняют обработчик.
---@param handler fun(message: TntMessage): any, any
---@param opts table|nil Настройки получателя
---@return TntQueueConsumer
function Queue:consume(handler, opts)
    must.at(2).callable(handler, 'обработчик')

    local policy = message.policy(opts, Module.features, 2)
    local consumer = worker.start(self, handler, policy)

    table.insert(self.consumers, consumer)

    return consumer
end

--- Возвращает до `count` зарытых на выдачу и отдаёт число возвращённых.
---
--- Счёт выдач у возвращённого начинается с нуля, а `ttl`, отсрочка
--- и приоритет прежней отправки не восстанавливаются: их знает
--- отправитель, а не зарытое.
---@param count integer|nil Сколько вернуть; по умолчанию одно
---@return integer|nil moved
---@return TntStorageFailure|nil err
function Queue:kick(count)
    local caller = must.at(2)
    local asked = caller.optional.integer(count, 'сколько вернуть') or 1

    caller.positive(asked, 'сколько вернуть')

    if box.is_in_txn() then
        local misuse =
            'возврат зарытых идёт своей транзакцией, и в чужой его не зовут'

        error(misuse, 2)
    end

    local ready, why = self.tube:open()

    if not ready then
        return nil, unreachable(self, why)
    end

    local ok, moved = pcall(self.tube.kick, self.tube, asked, self.ttr)

    if not ok then
        return nil, refused(moved, false)
    end

    return moved
end

--- Продлевает `ttr` сообщения, которое сейчас в работе у этого файбера.
---@param envelope TntMessage Конверт, пришедший обработчику
---@param seconds number На сколько продлить
---@return boolean|nil продлено
---@return TntStorageFailure|nil err
function Queue:touch(envelope, seconds)
    local caller = must.at(2)

    caller.table(envelope, 'сообщение')
    caller.positive(seconds, 'продление')
    caller.less_than(seconds, 'продление', message.MAX_SECONDS)

    local task_id = self.handling[envelope.id]

    if task_id == nil then
        local text = ('сообщение %s не в работе у очереди %s'):format(
            tostring(envelope.id),
            self.name
        )

        return nil, failure.new('rejected', text, { sent = false })
    end

    local ok, err = pcall(self.tube.touch, self.tube, task_id, seconds)

    if not ok then
        return nil, failure.new('rejected', failure.text(err), { sent = false })
    end

    return true
end

--- Что с очередью сейчас: состояние, счётчики, глубина, работники.
---@return table
function Queue:status()
    local state = self.tube:state()
    local counts = {}

    for name, value in pairs(self.counts) do
        counts[name] = value
    end

    local workers = 0

    for _, consumer in ipairs(self.consumers) do
        workers = workers + consumer.workers
    end

    return {
        name = self.name,
        ttr = self.ttr,
        state = state,
        counts = counts,
        depth = state == tube_of.RUNNING and self.tube:depth() or nil,
        workers = workers,
        hooks = hooks.names(),
    }
end

return Module
