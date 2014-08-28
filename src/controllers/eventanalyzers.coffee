promise = require 'bluebird'
EventEmitter = require('events').EventEmitter


class EmailAnalyzer
    constructor: ->
        @parser = require('packet').createParser()
    parseMessage: (message) ->
        return new promise (fulfill, reject) =>
            parsed = false
            @parser.extract "x448, l32 =>virusListLength, l32 =>emailLength", (header) =>
                parsed = true
                console.log "headers parsed", header
                return fulfill header.virusListLength, header.emailLength
            timeout = (parsed)  =>
                return reject new Error "Timed out" unless parsed

            @parser.parse message

            setTimeout timeout, 10000
            
    parsevirus: (message, virusLength, emailLength) ->
            cname = message.toString 'utf-8', 0, 48
            viruslist  = message.toString 'utf-8', 64, (64 + virusLength)
            #email = message.toString 'utf-8', (64+virusLength), (64 + virusLength + emailLength)
            timestamp = message.readDoubleLE 48, 56
            email = message.toString 'utf-8', 64 + virusLength
            #console.log "cname is ", cname, "virus list is ", viruslist, "email is ", email
            data =
                cname:cname
                virusList: viruslist
                email: email
                timestamp:  new Date 1000 * timestamp
            return data
                

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

    emailvirus: (message) ->
        @emailanalyzer.parseMessage message
         . then (viruslength, emaillength) =>
             data = @emailanalyzer.parsevirus message, viruslength, emaillength
             @emailanalyzer.parsemail data.email
              . then (parsedemail) =>
                  result =
                      identification: data.cname
                      timestamp: data.timestamp
                      virusNames: data.virus
                      email: parsedemail
                  @emit 'emailvirus.result', result
              . catch (error) =>
                        @emit 'emailvirus.error', error
                            


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

    #console.log buf.toString()
    ea.emailvirus buf
    ea.on 'emailvirus.result', (result) ->
        

    ea.on 'emailvirus.error', (error) ->
        




            







module.exports = EventAnalyzer
