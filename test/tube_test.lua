--- Труба без box: состояние закрытой очереди и запись об истёкшем
--- сообщении.
---
--- Крюк `on_task_change` рок зовёт изнутри своих движений, и увидеть
--- запись, которую фасад из него пишет, проще всего здесь — задачей рока,
--- собранной руками.

local t = require('luatest')

local helper = dofile('test/helper.lua')

local tube_of = helper.tube

local g = t.group('tnt.queue.tube')

g.after_each(function()
    helper.journal.forget()
end)

g.after_all(function()
    helper.journal.release()
end)

--- Задача рока: номер, состояние и данные в девятом поле, как в спейсе
--- трубы драйвера `utubettl`.
---@param number integer
---@param data any
---@return table
local function task(number, data)
    return { number, nil, 0, 0, 0, 0, 0, 'ключ', data }
end

g.test_a_tube_that_is_not_opened_is_closed = function()
    local tube = tube_of.new('mail', { expired = 0 })

    t.assert_equals(tube:state(), 'CLOSED')
    t.assert_equals(tube:open(), false, 'без box.cfg труба не заводится')
    t.assert_equals(select(2, tube:open()), 'узел только для чтения')
end

g.test_an_expired_message_is_counted_and_written_with_its_identifier = function()
    local counts = { expired = 0 }
    local tube = tube_of.new('mail', counts)

    tube:expired(task(7, { id = '01J0000000000000000000000X', body = 'тайна' }))

    local entry = helper.journal.find('сообщение истекло по ttl')

    t.assert_equals(counts.expired, 1)
    t.assert_equals(entry.record.fields.destination, 'mail')
    t.assert_equals(entry.record.fields.id, '01J0000000000000000000000X')
    t.assert_equals(entry.record.fields.task, 7)
    t.assert_str_contains(entry.line, 'WARN [tnt.queue]')
    t.assert_not_str_contains(entry.line, 'тайна', 'тело в журнал не пишется')
end

g.test_an_expired_task_without_an_envelope_is_counted_too = function()
    local counts = { expired = 0 }
    local tube = tube_of.new('mail', counts)

    tube:expired(task(9, 'чужое'))

    local record = helper.journal.find('сообщение истекло по ttl').record

    t.assert_equals(counts.expired, 1)
    t.assert_equals(record.fields.id, nil, 'опознавателя у чужого нет')
    t.assert_equals(record.fields.task, 9)
end
