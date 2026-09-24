--- Своё у конверта очереди: чужое содержимое трубы.
---
--- Конверт, настройки отправки и тело — общая часть договора, пакет
--- `tnt-message`: они одни у всех очередей, и обработчик переезжает с одной
--- на другую без правки. Здесь — только то, чего у других очередей нет:
--- задача, положенная в трубу мимо фасада.

local clock = require('tnt.clock')
local id = require('tnt.id')

local Module = {}

--- Конверт и настройки отправки — те же, что у всех очередей договора.
---@alias TntQueueMessage TntMessage
---@alias TntQueueSendOptions TntMessageSendOptions

--- Конверт для чужого содержимого трубы: его положили мимо фасада, и
--- ни опознавателя, ни счёта выдач у него нет. Получатель зароет его
--- сразу — с новым опознавателем, под которым его найдёт человек.
---@param name string Назначение
---@param data any Что лежало в задаче
---@return TntMessage
function Module.foreign(name, data)
    return {
        id = id.ulid(),
        name = name,
        body = data,
        headers = {},
        attempt = 1,
        created = clock.realtime(),
        foreign = true,
    }
end

return Module
