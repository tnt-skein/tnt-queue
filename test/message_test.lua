--- Своё у конверта очереди: чужое содержимое трубы.
---
--- Конверт, настройки отправки и тело проверяет общая часть договора;
--- здесь — только то, чего у других очередей нет.

local t = require('luatest')

local helper = dofile('test/helper.lua')

local message = helper.message

local g = t.group('tnt.queue.message')

g.test_a_foreign_task_is_wrapped_into_an_envelope = function()
    local envelope = message.foreign('mail', 'чужое')

    t.assert_equals(envelope.name, 'mail')
    t.assert_equals(envelope.body, 'чужое')
    t.assert_equals(envelope.headers, {})
    t.assert_equals(envelope.attempt, 1)
    t.assert_equals(envelope.foreign, true)
    t.assert_equals(#envelope.id, 26)
    t.assert_almost_equals(envelope.created, require('clock').realtime(), 1)
end
