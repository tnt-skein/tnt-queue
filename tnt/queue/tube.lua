--- Труба рока `queue` под одной очередью: ленивая загрузка рока, заведение
--- трубы и спейса зарытых, движения задачи.
---
--- **Рок грузится лениво и только на узле для записи**: `require('queue')`
--- на узле для чтения подменяет вызов `box.cfg` своей обёрткой, печатает
--- стек в stdout и отказывает исключением. Поэтому до первого дела на узле
--- для записи рок не трогается, а очередь отвечает «не принимает».
--- Заведение трубы — DDL, и внутри чужой транзакции оно не идёт: уступка
--- порвала бы её.
---
--- **Драйвер — всегда `utubettl`**: он умеет и ключ, и сроки, а два
--- драйвера под одним фасадом дали бы две очереди с разным поведением.
---
--- **Зарытые — в своём спейсе `<имя>_dead`**, а не в состоянии `!` рока:
--- `ttl` рока стирает и зарытое. Перенос — одной транзакцией
--- с подтверждением задачи: зарытое не теряется и не остаётся в трубе.
---
--- **Счёт выдач пишется за взятием**: `{ '+', '[9].attempt', 1 }` —
--- четвёртая запись WAL на сообщение. Без неё сообщение, роняющее
--- процесс, после перезапуска выдавалось бы с тем же номером без конца.
---
--- **Истёкшее по `ttl` считается**: рок удаляет его молча, и узнать
--- об этом можно только крюком `on_task_change` родом `ttl`. Статус задачи
--- в нём `NULL`, а перед ним тот же номер приходит родом `delete` —
--- поэтому судим по роду.
---
--- **Возврат без отсрочки пишет срок удаления заново.** Рок `queue 1.5.0`
--- в этой ветке `release` переводит срок в микросекунды второй раз,
--- и произведение обходит `uint64` по кругу за 213 суток: сообщение
--- с `ttl` после такого возврата не истекает, а в получасовое окно
--- круга любое — и без `ttl` — выбрасывается сразу. Отсрочка
--- в миллисекунду увела бы рок в верную ветку, но на время отсрочки ключ
--- свободен, и следующее сообщение ключа обогнало бы возвращённое. Правка
--- лежит на master рока без выпуска; после подъёма закрепления обход
--- снимается, а до тех пор пишет то же, что пишет правка.
---
--- Методы рока отказывают по-разному: вне `RUNNING` — голым `nil` и строкой
--- в журнале, иначе — исключением. Здесь пустота становится исключением
--- с состоянием очереди: движение, которое не записалось, вызывающий
--- обязан увидеть.

local clock = require('tnt.clock')
local fail = require('tnt.must.fail')
local external = require('tnt.external')

local message = require('tnt.queue.message')
local series = require('tnt.queue.series')

local log = require('tnt.log').new('tnt.queue')

local Module = {}

--- Драйвер рока.
Module.DRIVER = 'utubettl'

--- Состояние рока, в котором очередь принимает и выдаёт.
Module.RUNNING = 'RUNNING'

--- Состояние очереди, пока рок не загружен или труба не заведена.
Module.CLOSED = 'CLOSED'

--- Приставка имени спейса зарытых.
Module.DEAD_SUFFIX = '_dead'

--- Поля спейса зарытых.
local DEAD_FORMAT = {
    { name = 'id', type = 'string' },
    { name = 'reason', type = 'string' },
    { name = 'attempt', type = 'unsigned' },
    { name = 'buried', type = 'number' },
    { name = 'message', type = 'map' },
}

--- Внешние средства: рок и признак узла для записи.
local source = external.install(Module, {
    load = function()
        return require('queue')
    end,

    -- До `box.cfg` вызов `box.cfg` — функция, и `box.info` ещё нет.
    writable = function()
        return type(box.cfg) ~= 'function' and box.info.ro == false
    end,
})

--- Очереди, чья труба заводится прямо сейчас: DDL уступает, и второй файбер,
--- пришедший в это окно, увидел бы спейс без индексов.
---@type table<string, boolean>
local opening = {}

---@class TntQueueTube
---@field name string Имя очереди и трубы
---@field counts table<string, integer> Счётчики очереди: сюда идёт `expired`
---@field rock table Рок; появляется вместе с трубой, в `open`
---@field tube table Труба рока; до `open` её нет, и это видно по `state`
---@field dead table Спейс зарытых; появляется вместе с трубой
local Tube = {}
Tube.__index = Tube

--- Труба очереди; рок не трогается до `open`.
---@param name string
---@param counts table<string, integer>
---@return TntQueueTube
function Module.new(name, counts)
    ---@diagnostic disable-next-line: missing-fields
    return setmetatable({ name = name, counts = counts }, Tube)
end

--- Считает и пишет сообщение, которое рок выбросил по `ttl`.
---
--- Зовётся крюком рока изнутри его движений, поэтому не бросает: бросок
--- сорвал бы само движение.
---@param task table Задача рока целиком: номер в поле 1, конверт в поле 9
function Tube:expired(task)
    local data = task[9]

    series.count(self, 'expired')
    log.warn('сообщение истекло по ttl и выброшено', {
        destination = self.name,
        id = type(data) == 'table' and data.id or nil,
        task = task[1],
    })
end

--- Заводит трубу, спейс зарытых и крюк истечения.
---@param tube TntQueueTube
---@param rock table
local function create(tube, rock)
    local made = rock.create_tube(tube.name, Module.DRIVER, { if_not_exists = true })
    local dead = box.schema.space.create(tube.name .. Module.DEAD_SUFFIX, {
        if_not_exists = true,
        format = DEAD_FORMAT,
    })

    dead:create_index('primary', { if_not_exists = true, parts = { 'id' } })

    made:on_task_change(function(task, kind)
        if kind == 'ttl' then
            pcall(tube.expired, tube, task)
        end
    end)

    tube.rock = rock
    tube.tube = made
    tube.dead = dead
end

--- Состояние очереди: состояние рока либо `CLOSED`, пока труба не заведена.
---@return string
function Tube:state()
    if self.tube == nil then
        return Module.CLOSED
    end

    return self.rock.state()
end

--- Заводит трубу, если можно, и говорит, принимает ли очередь.
---
--- Пустота второго значения при `true`; при `false` — почему нет: узел
--- для чтения, транзакция, состояние рока.
---@return boolean ready
---@return string|nil why
function Tube:open()
    if self.tube ~= nil then
        local state = self:state()

        return state == Module.RUNNING, state
    end

    if not source().writable() then
        return false, 'узел только для чтения'
    end

    if box.is_in_txn() or opening[self.name] then
        return false, 'труба ещё не заведена'
    end

    local rock = source().load()
    local state = rock.state()

    if state ~= Module.RUNNING then
        return false, state
    end

    opening[self.name] = true

    local ok, err = pcall(create, self, rock)

    opening[self.name] = nil

    if not ok then
        -- Пустота вместо ошибки бывает у `error(nil)`: бросить её значило бы
        -- отдать вызывающему отказ без слов, а его не расследовать.
        fail.raise(err or ('труба %s не завелась'):format(self.name))
    end

    return true
end

--- Отказ, если движение не записалось: рок вне `RUNNING` отдаёт пустоту.
---
--- Бросок без места: текст уходит в журнал и сверяется целиком, а место
--- внутри фасада вызывающему ничего не говорит.
---@param tube TntQueueTube
---@param result any Что вернул рок
local function recorded(tube, result)
    if result == nil then
        fail.raise(('очередь %s в состоянии %s'):format(tube.name, tube:state()))
    end
end

--- Кладёт конверт в трубу. Ключ рока — `key`, а без него `id`: иначе все
--- безключевые шли бы одной цепочкой под ключом `'nil'`.
---@param envelope TntQueueMessage
---@param opts TntQueueSendOptions
---@param ttr number
function Tube:put(envelope, opts, ttr)
    recorded(
        self,
        self.tube:put(envelope, {
            utube = envelope.key or envelope.id,
            delay = opts.delay,
            ttl = opts.ttl,
            pri = opts.priority,
            ttr = ttr,
        })
    )
end

--- Берёт задачу и пишет выдачу в конверт.
---
--- Чужое содержимое трубы — не конверт — отдаётся конвертом с новым
--- опознавателем и признаком `foreign`: получатель зароет его сразу.
---@param wait number Сколько ждать задачи, секунды
---@return integer|nil task_id
---@return TntQueueMessage|nil envelope
function Tube:take(wait)
    local task = self.tube:take(wait)

    if task == nil then
        return nil
    end

    local data = task[3]

    if type(data) ~= 'table' or type(data.id) ~= 'string' or type(data.attempt) ~= 'number' then
        return task[1], message.foreign(self.name, data)
    end

    local counted = box.space[self.name]:update(task[1], { { '+', '[9].attempt', 1 } }) --[[@as table]]

    return task[1], counted[9]
end

--- Подтверждает задачу.
---@param task_id integer
function Tube:ack(task_id)
    recorded(self, self.tube:ack(task_id))
end

--- Возвращает задачу на выдачу с отсрочкой.
---
--- Без отсрочки срок удаления пишется заново той же транзакцией: рок
--- `queue 1.5.0` в этой ветке переводит его в микросекунды второй раз
--- (шапка модуля). Срок — отправка плюс `ttl`: столько же рок пишет
--- задаче, которая вышла из отсрочки или из `ttr`.
---@param task_id integer
---@param delay number
function Tube:release(task_id, delay)
    box.atomic(function()
        recorded(self, self.tube:release(task_id, { delay = delay }))

        -- Ветку без отсрочки рок выбирает так же: всё, что не больше нуля.
        if delay > 0 then
            return
        end

        -- Задача есть: без неё рок отказал бы исключением выше.
        local space = box.space[self.name]
        local task = space:get(task_id) --[[@as table]]

        space:update(task_id, { { '=', 'next_event', task.created + task.ttl } })
    end)
end

--- Зарывает: задача уходит из трубы, конверт с причиной — в спейс
--- зарытых, одной транзакцией.
---@param task_id integer
---@param envelope TntQueueMessage
---@param reason string
function Tube:bury(task_id, envelope, reason)
    box.atomic(function()
        recorded(self, self.tube:ack(task_id))
        self.dead:replace({ envelope.id, reason, envelope.attempt, clock.realtime(), envelope })
    end)
end

--- Продлевает `ttr` взятой задачи.
---@param task_id integer
---@param seconds number
function Tube:touch(task_id, seconds)
    recorded(self, self.tube:touch(task_id, seconds))
end

--- Возвращает до `count` зарытых на выдачу со счётом выдач с нуля,
--- одной транзакцией.
---@param count integer
---@param ttr number
---@return integer moved
function Tube:kick(count, ttr)
    local rows = self.dead:select({}, { limit = count })

    box.atomic(function()
        for _, row in ipairs(rows) do
            local envelope = row[5]

            envelope.attempt = 0
            self:put(envelope, {}, ttr)
            self.dead:delete(row[1])
        end
    end)

    return #rows
end

--- Сколько сообщений в каком состоянии.
---@return { ready: integer, taken: integer, delayed: integer, dead: integer }
function Tube:depth()
    local tasks = self.rock.statistics(self.name).tasks

    return {
        ready = tasks.ready,
        taken = tasks.taken,
        delayed = tasks.delayed,
        dead = self.dead:len(),
    }
end

return Module
