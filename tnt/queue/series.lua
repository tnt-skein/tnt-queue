--- Ряды метрик очереди: отправлено, обработано по итогу, истекло,
--- сорвалось, сколько ждало и сколько лежит.
---
--- Ряды `message_*` объявляет общая часть договора (`tnt.message.series`):
--- они одни у очереди, шины событий и клиентов брокеров, и панель,
--- собранная для одной, читает любую. Своё у очереди — счётчики `status()`,
--- которые растут вместе с рядами (`count`), и то, чью глубину показывать:
--- объявленные очереди, пока их кто-то держит.
---
--- Длина очереди — шкала со сбором: глубину спрашивает у рока каждая
--- выкладка, а не каждое движение сообщения.

local shared = require('tnt.message.series')

local Module = {}

--- Очереди, чью глубину показывает шкала, по имени.
---
--- Слабые значения: очередь, которую приложение больше не держит, из рядов
--- уходит сама. Очередь, объявленная под тем же именем ещё раз, заменяет
--- прежнюю — рок держит одну трубу на имя.
---@type table<string, TntQueue>
local watched = setmetatable({}, { __mode = 'v' })

--- Глубина всех очередей по состояниям — то же, что `status().depth`.
---
--- Очередь, которая сейчас не принимает (узел для чтения, подъём),
--- глубины не знает, и её строк в ряду нет.
---@return table<string, table<string, integer>>
local function measured()
    local depths = {}

    for name, queue in pairs(watched) do
        depths[name] = queue:status().depth
    end

    return depths
end

shared.depth('tnt.queue', measured)

--- Считает событие очереди: и в счётчиках `status()`, и в ряду.
---@param owner { name: string, counts: table<string, integer> } Очередь либо её труба
---@param what string Имя счётчика
function Module.count(owner, what)
    shared.count(owner.counts, owner.name, what)
end

--- Считает отказ отправки по роду.
Module.refused = shared.refused

--- Меряет ожидание сообщения на первой выдаче.
Module.taken = shared.taken

--- Показывает глубину очереди в ряду `message_depth`.
---@param queue TntQueue
function Module.watch(queue)
    watched[queue.name] = queue
end

return Module
