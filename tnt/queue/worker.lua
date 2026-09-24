--- Получатель очереди: работники, цикл взятия, итог обработчика.
---
--- **Взятие, обработчик и итог — в одном файбере**: у рока у каждого
--- файбера своя сессия, и подтвердить задачу из соседнего нельзя — «Task
--- was not taken». Шаг работника идёт под `pcall`: сбой считается
--- и пишется, работник живёт дальше, а после сбоя спит, чтобы беда,
--- повторяющаяся на каждом шаге, не крутила цикл без уступки.
---
--- **Работник ждёт `RUNNING` рока, а не `box.info.ro`**: после возврата
--- в запись узел уже пишет, а очередь ещё поднимается, и `take` отдал бы
--- пустоту сразу — цикл встал бы без уступки. Такой цикл на узле
--- для чтения за 2,5 минуты записал 5,4 ГБ журнала и не услышал
--- `SIGTERM`. На узле для чтения работник ждёт `box.ctl.wait_rw` со сроком,
--- в окне подъёма — короткий сон.
---
--- **Счёт выдач — потолок и для упавших.** Выдача сверх `max_attempts`
--- зарывается, не доходя до обработчика: сообщение, которое роняет процесс,
--- иначе выдавалось бы после каждого перезапуска без конца.
---
--- **Итог опоздал** — очередь движение не приняла: `ttr` вышел и сообщение
--- выдано снова (исключение рока), либо очередь перестала принимать.
--- Итог считается `late` и пишется `warn`: работа сделана и будет сделана
--- ещё раз, и об этом должен узнать тот, кто выбирал `ttr`.
---
--- **`atomic`**: обработчик идёт внутри `box.begin()`, подтверждение —
--- последний оператор той же транзакции. Уступка в обработчике рвёт
--- транзакцию, и это попытка с причиной «…fiber yield».
---
--- Обработчик — код приложения, и права у него — права фонового файбера,
--- то есть процесса, а не того, кто объявил получателя. Внутри чужой
--- транзакции он не зовётся: у работника свой файбер.

local fiber = require('fiber')

local external = require('tnt.external')
local message = require('tnt.message')

local series = require('tnt.queue.series')

local log = require('tnt.log').new('tnt.queue')

--- Крюки отправки и обработки: список пакета, общий с фасадом.
local hooks = message.hooks('tnt.queue')

--- Сбой шага пишется с подавлением повторов: беда, которая держится,
--- иначе писала бы строку на каждом шаге.
local steps = require('tnt.log').changes('tnt.queue')

local Module = {}

--- Сколько ждать задачи за один шаг, секунды: столько же `stop` ждёт,
--- пока работник это заметит.
Module.POLL = 0.5

--- Сколько ждать записи на узле для чтения за один шаг, секунды.
Module.READ_ONLY_WAIT = 0.5

--- Сколько спать, пока очередь поднимается, секунды.
Module.STARTUP_WAIT = 0.01

--- Сколько спать после сбоя шага, секунды.
Module.FAILURE_PAUSE = 0.1

--- Внешние средства: сон и ожидание записи.
local source = external.install(Module, {
    sleep = fiber.sleep,

    -- Ожидание со сроком бросает по сроку; отказ здесь — это «ещё нет».
    wait_rw = function(timeout)
        return pcall(box.ctl.wait_rw, timeout)
    end,

    read_only = function()
        return type(box.cfg) ~= 'function' and box.info.ro == true
    end,
})

---@class TntQueueConsumer Получатель: `stop()` перестаёт брать новые сообщения
---@field queue TntQueue
---@field handler fun(message: TntMessage): any, any
---@field policy TntMessagePolicy
---@field stopping boolean Попросили остановиться
---@field workers integer Сколько работников ещё живо
local Consumer = {}
Consumer.__index = Consumer

--- Обработчик внутри транзакции с подтверждением последним оператором.
---@param consumer TntQueueConsumer
---@param task_id integer
---@param envelope TntMessage
---@return any value Пустота у успеха: подтверждено внутри транзакции
---@return any err
local function atomically(consumer, task_id, envelope)
    box.begin()

    local value, err = consumer.handler(envelope)

    if not value and err ~= nil then
        box.rollback()

        return value, err
    end

    consumer.queue.tube:ack(task_id)
    box.commit()
end

--- Исполняет обработчик: как `xpcall`, у брошенного — стек.
---@param consumer TntQueueConsumer
---@param task_id integer
---@param envelope TntMessage
---@return boolean ok
---@return any value
---@return any err
local function invoke(consumer, task_id, envelope)
    if not consumer.policy.atomic then
        return xpcall(consumer.handler, message.caught, envelope)
    end

    local ok, value, err = xpcall(atomically, message.caught, consumer, task_id, envelope)

    if box.is_in_txn() then
        box.rollback()
    end

    return ok, value, err
end

--- Движения итога по приговору. Подпись у всех одна: приговор выбирает
--- движение, а не аргументы.
---@type table<string, fun(tube: TntQueueTube, task_id: integer, envelope: TntMessage, extra: any)>
local APPLY = {
    [message.ACK] = function(tube, task_id)
        tube:ack(task_id)
    end,
    [message.RETRY] = function(tube, task_id, _, delay)
        tube:release(task_id, delay)
    end,
    [message.BURY] = function(tube, task_id, envelope, reason)
        tube:bury(task_id, envelope, reason)
    end,
}

--- Записывает итог в очередь и считает его.
---@param consumer TntQueueConsumer
---@param task_id integer
---@param envelope TntMessage
---@param verdict string
---@param extra any Отсрочка у `retry`, причина у `bury`
local function settle(consumer, task_id, envelope, verdict, extra)
    local queue = consumer.queue
    local fields = { destination = queue.name, id = envelope.id, attempt = envelope.attempt }
    local ok, err = pcall(APPLY[verdict], queue.tube, task_id, envelope, extra)

    if not ok then
        series.count(queue, 'late')
        fields.outcome = verdict
        fields.err = tostring(err)

        return log.warn(
            'итог обработчика опоздал: сообщение выдадут снова',
            fields
        )
    end

    series.count(queue, verdict)

    if verdict == message.BURY then
        fields.err = extra
        log.error('сообщение зарыто', fields)
    elseif verdict == message.RETRY then
        fields.delay = extra
        log.debug('сообщение возвращено на выдачу', fields)
    end
end

--- Почему сообщение зарывается, не доходя до обработчика; nil — не зарывается.
---
--- Чужое содержимое трубы положили мимо фасада: ни опознавателя, ни счёта
--- выдач у него нет, и обработчик его не прочтёт.
---@param consumer TntQueueConsumer
---@param envelope TntMessage
---@return string|nil
local function refusal(consumer, envelope)
    if envelope.foreign then
        return 'в трубе не конверт tnt-queue'
    end

    return message.refusal(envelope, consumer.policy, consumer.queue.shape)
end

--- Обрабатывает взятое сообщение: область контекста, крюки, обработчик,
--- итог.
---@param consumer TntQueueConsumer
---@param task_id integer
---@param envelope TntMessage
local function handle(consumer, task_id, envelope)
    local queue = consumer.queue
    local reason = refusal(consumer, envelope)

    series.taken(queue.name, envelope)

    if reason ~= nil then
        return settle(consumer, task_id, envelope, message.BURY, reason)
    end

    queue.handling[envelope.id] = task_id

    local verdict, extra = message.judge(hooks, log, envelope, consumer.policy, function()
        return invoke(consumer, task_id, envelope)
    end)

    queue.handling[envelope.id] = nil

    -- У `atomic` подтверждение уже в транзакции обработчика.
    if verdict == message.ACK and consumer.policy.atomic then
        series.count(queue, message.ACK)

        return
    end

    settle(consumer, task_id, envelope, verdict, extra)
end

--- Ждёт, пока очередь начнёт принимать: на узле для чтения — записи,
--- иначе — подъёма рока.
---@param queue TntQueue
local function idle(queue)
    queue.counts.idle_waits = queue.counts.idle_waits + 1

    if source().read_only() then
        return source().wait_rw(Module.READ_ONLY_WAIT)
    end

    source().sleep(Module.STARTUP_WAIT)
end

--- Один шаг работника: взять и обработать либо подождать.
---@param consumer TntQueueConsumer
local function step(consumer)
    local tube = consumer.queue.tube

    if not tube:open() then
        return idle(consumer.queue)
    end

    local task_id, envelope = tube:take(Module.POLL)

    if task_id ~= nil then
        handle(consumer, task_id, envelope --[[@as TntMessage]])
    end
end

--- Тело работника: шаги до остановки.
---@param consumer TntQueueConsumer
---@param number integer
local function work(consumer, number)
    fiber.self():name(('queue/%s/%d'):format(consumer.queue.name, number), { truncate = true })

    while not consumer.stopping do
        local ok, err = pcall(step, consumer)

        if not ok then
            series.count(consumer.queue, 'failures')
            steps.warn(
                'шаг работника очереди сорвался',
                { destination = consumer.queue.name, err = tostring(err) }
            )
            source().sleep(Module.FAILURE_PAUSE)
        end
    end

    consumer.workers = consumer.workers - 1
end

--- Запускает работников получателя.
---@param queue TntQueue
---@param handler fun(message: TntMessage): any, any
---@param policy TntMessagePolicy
---@return TntQueueConsumer
function Module.start(queue, handler, policy)
    local consumer = setmetatable({
        queue = queue,
        handler = handler,
        policy = policy,
        stopping = false,
        workers = policy.workers,
    }, Consumer)

    for number = 1, policy.workers do
        fiber.new(work, consumer, number)
    end

    return consumer
end

--- Перестаёт брать новые сообщения; взятое дорабатывается. Работник,
--- ждущий задачи, замечает это не позже чем через `POLL`.
function Consumer:stop()
    self.stopping = true
end

return Module
