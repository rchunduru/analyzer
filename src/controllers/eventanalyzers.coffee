promise = require 'bluebird'
EventEmitter = require('events').EventEmitter

parseUInt = (str) ->
    return 0 unless str?
    i = 0
    multiplier = 1
    sum = 0
    while i++ < str.length
        value = (str[(str.length - i)] * multiplier)
        multiplier *= 10
    sum


class EmailAnalyzer

    parsemail: (unparsedEmail) ->
        return new promise (fulfill, reject) =>
            MP = require('mailparser').MailParser
            mp = new MP
            mp.on 'end', (pemail) =>
                parsedEmail =
                    headers:pemail.headers
                    ###
                        to: pemail.headers.To
                        from:pemail.headers.from
                        subject:pemail.subject
                        cc:pemail.cc
                        bcc:pemail.bcc
                        inReplyTo: pemail.inReplyTo
                        priority: pemail.priority
                        date:pemail.date
                    ###
                    content: pemail.text ?= pemail.html
                    attachments: pemail.attachments
                console.log "Debug: parsed email headers are ", pemail.headers
                return reject new Error "Failed to parse headers" if pemail.headers is {}
                return fulfill parsedEmail
            
            buf = new Buffer unparsedEmail, 'base64'    
            mp.write buf.toString()
            mp.end()
            timeout =  =>
                return reject new Error "Timed out"
            setTimeout timeout, 10000

    
class EventAnalyzer extends EventEmitter
    constructor: ->
        @emailanalyzer = new EmailAnalyzer

    stripHeader: (message) ->
        content = {}
        msgs = message.split " {"
        headers = msgs[0].split(' ')

        console.log "Debug: headers rcvd are ", headers
        header = 
            priority: parseUInt headers[0]
            timestamp: parseUInt headers[1]
            format: headers.pop()
            cname: (buf for buf in headers[2...headers.length]).join(' ')

        data = message.substring msgs[0].length, message.length
        content.header = header
        content.data = JSON.parse data
        #console.log "Debug: stripHeader is generating content", content.data
        content

    decodeBinarySyslog: (content) ->
        data = content.data
        syslog = {}
        return syslog if content.size < 20
        syslog.pri = data.readUInt8 0
        timestamp = data.readUInt32LE 1
        syslog.timestamp = new Date 1000 * timestamp
        cnameLen = data.readUInt32LE 5
        formatLen = data.readUInt32LE 9
        msgLen = data.readUInt32LE 13
        cname = data.slice 17, cnameLen
        syslog.cname = cname.toString()
        format = data.slice 17+cnameLen, formatLen
        syslog.format = format.toString()
        message = data.slice 17+cnameLen+formatLen, msgLen
        syslog.message = JSON.parse message.toString()
      
        console.log "Debug: analyzed syslog message is ", syslog 
        syslog

    emailvirus: (content) ->
        return new promise (fulfill, reject) =>
             @emailanalyzer.parsemail content.data.mail
              . then (parsedemail) =>
                  result =
                      id: ""
                      virusNames: content.data.virus
                      timestamp: (parseUInt content.data.timestamp) * 1000
                      mail: parsedemail
                  return fulfill result
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

