promise = require 'bluebird'
EventEmitter = require('events').EventEmitter


class EmailAnalyzer

    parsemail: (unparsedEmail) ->
        return new promise (fulfill, reject) =>
            MP = require('mailparser').MailParser
            mp = new MP
            mp.on 'end', (pemail) =>
                parsedEmail =
                    headers:
                        to: pemail.headers.To
                        from:pemail.headers.from
                        subject:pemail.subject
                        cc:pemail.cc
                        bcc:pemail.bcc
                        inReplyTo: pemail.inReplyTo
                        priority: pemail.priority
                        date:pemail.date
                    content: pemail.text ?= pemail.html
                    attachments: pemail.attachments
                console.log parsedEmail.headers
                return fulfill parsedEmail
                
            mp.write unparsedEmail
            mp.end()
            timeout =  =>
                return reject new Error "Timed out"
            setTimeout timeout, 10000

    
class EventAnalyzer extends EventEmitter
    constructor: ->
        @emailanalyzer = new EmailAnalyzer

    stripHeader: (message) ->
        size = message.readUInt32LE 0
        data = message.slice 4, size
        data

    decodeBinarySyslog: (data) ->
        syslog = {}
        syslog.pri = data.readUInt8 0
        timestamp = data.readUInt32LE 1
        header.timestamp = new Date 1000 * timestamp
        cnameLen = data.readUInt32LE 5
        formatLen = data.readUInt32LE 9
        msgLen = data.readUInt32LE 13
        cname = data.slice 17, cnameLen
        syslog.cname = cname.toString()
        format = data.slice 17+cnameLen, formatLen
        syslog.format = format.toString()
        message = data.slice 17+cnameLen+formatLen, msgLen
        syslog.message = JSON.parse message.toString()
       
        syslog

    emailvirus: (syslog) ->
        return new promise (fulfill, reject) =>
             @emailanalyzer.parsemail syslog.message.email
              . then (parsedemail) =>
                  result =
                      id: ""
                      virusNames: data.virus
                      timestamp: data.timestamp
                      email: parsedemail
                  syslog.parsedemail = result
                  return fulfill syslog
              . catch (error) =>
                  return reject error




module.exports = EventAnalyzer

if require.main is module
    ea = new EventAnalyzer
    # sample content
    #
    body = "From: ravivsn@ravivsn.com\r\n To: kumar@kumar.com\r\n\r\n"
    buf = new Buffer 2048
    buf.write "testcname\0", 0 # cname
    buf.writeDoubleLE 14000, 48 # timestamp
    buf.writeUInt32LE 14, 56 # virus length
    buf.writeUInt32LE 14, 60 # email length
    buf.write "virus1,virus2", 64
    buf.write body, 78

