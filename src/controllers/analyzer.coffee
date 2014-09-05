promise = require 'bluebird'
bunyan = require 'bunyan'
EM = require './elasticmanager'
MM = require './mqmanager'
validate = require('json-schema').validate
uuid = require 'node-uuid'

async = require 'async'
SR = require('stormagent').StormRegistry
SD = require('stormagent').StormData
SA = require 'stormagent'

EA = require './eventanalyzers'
parseurl = require('../helpers/utils').parseUrl
parseMessage = require('../helpers/utils').parseMessage
postRequest = require('../helpers/utils').postRequest
parsequery = require('../helpers/utils').parseQuery


class AnalyzerService extends SD
    schema =
        name: "AnalyzerService"
        type: "object"
        required: true
        additionalProperties: true
        properties:
            id:                 {"type": "string", "required": false}
            name:               {"type": "string", "required": false}
            output:             {"type": "string", "required": true}
            transmitters:       {"type": "array",  "required": true}
            sources:            {"type": "array",  "required": true}


    constructor: (id, data) ->
        super id, data, schema
        @em  = promise.promisifyAll(EM.prototype)
        @log "Debug: Elastic manager object is ", @em
        @mq = new MM
        @ea = new EA
        @susbscriptions = {}
        @topics = []
        @mqConnect()
        @dbConnect()
        

    mqConnect:  ->
        if @data and @data.sources?
            @data.sources.map (source) =>
                parsedurl = parseurl source
                # Build the topics ready for subscription
                @log "Debug: parsedurl query is ", parsedurl.query
                if parsedurl.query?
                    switch parsedurl.query.topic.prototype
                        when 'array'
                            @topics = parsedurl.query.topic
                        else
                            @topics = [parsedurl.query.topic]
                    @log "Topics identified to subscribe", @topics

                connect = =>
                    @log "Debug: Connecting to MQ hostname", parsedurl.hostname, "port: ", parsedurl.port
                    @mq.connect parsedurl.hostname, parsedurl.port, parsedurl.username, parsedurl.password, 10000, 5

                setTimeout connect, 1000

                @mq.on 'mq.connected', (client) =>
                    @log "Connected to the MQ broker", @data.sources
                    @log " ABout to subscribe to ", @topics
                    @mqclient = client
                    @subscribe()


                @mq.on 'mq.error', (error) =>
                    @mqclient = ""
                    @subscriptions = {}
                    @log "MQ connection failure after multiple retries. Error is #{error}"
                    @log "Retrying connection to MQ in 100 seconds"
                    setTimeout connect, 100000 # 100 seconds

    dbConnect:  ->
        if @data and @data.output?
            parsedurl = parseurl @data.output
            @log "Elastic DB server details: ", parsedurl
            @eclient = @em.init host:parsedurl.host, loglevel:'error'
            @log "Connected to the Elasticsearch DB", @data.output

    subscribe: ->
        # XXX Add async loop here
        for topic in @topics
            (@subscriptions ?= {})[b = topic] = @mq.subscribe "/topic/#{topic}", @loggersyslog

    loggersyslog: (message) ->
        @log "Assuming string format rcvd from MQ", message
        buf = new Buffer message.body.length
        msg  = @ea.stripHeader buf
        syslog = @ea.decodeBinarySyslog msg
        switch syslog.format
            when 'email.virus', 'EMAIL.VIRUS'
                @ea.emailvirus syslog
                 . then (body) =>
                     body.id = @id
                     @mailvirus body
                     message.ack()
                     return

            else
                data = syslog.message
                # First get the key as transactions or violations
                strarray = data.format.split(".")
                bodykey = strarray.pop()
                # Get the key as "web.contentfiltering" etc.,
                objkey = ""
                for str in strarray
                    objkey += str

                objkey = objkey.toLowerCase()
                bodykey = bodykey.toLowerCase()
                @getTransaction()
                . then (content) =>
                    content._source.transactions ?= {}
                    content._source.timestamp ?= data.start
                    # Get the data.format into the key of content._source.transactions

                    (content._source.transactions)[b = objkey] ?= {}
                    # Now set the content of the created object
                    contentvalue = (content._source.transactions)[b = objkey]
                    (contentvalue)[b = bodykey] ?= 0
                    (contentvalue)[b = bodykey] += data.count

                     
                    @updateTransaction content._id, content._source
                    . then (result) =>
                        return message.ack()
                    , (error) =>
                        @log "Debug: Error in updating the transaction", error, content._source
                        return message.ack()
                , (error) =>
                    #write onto Elastic DB
                    (transaction = {})[b = bodykey] ?= {}
                    contentvalue = (transaction)[b = bodykey]
                    (contentvalue)[b = bodykey] ?= 0
                    (contentvalue)[b = bodykey] = data.count
                    content =
                        id: @id
                        timestamp: data.start
                        transactions:transaction
                    @createTransaction content
                     .then (result) =>
                        return message.ack()
                     , (error) =>
                         @log " Failed to create a transaction record in Elastic search", error, content
                         return


    mailvirus: (emailData) ->
        #Write onto elastic DB
        @eclient.createDocumentAsync @id, 'email.virus', emailData
            . then (response) ->
                @log "Added email virus ", response
                @emit 'email.virus', emailData
            , (error) ->
                @log "Error while adding email virus into elastic DB", error


    createTransaction: (content) ->
        @eclient.ceateDocument @eclient, @id, 'transaction.summary', content
         . then (result) ->
             @log 'added transaction summary record with content', content
         , (error) ->
             @log 'error in adding transaction record', error

    updateTransaction: (id, content) ->
        @eclient.updateDocument @eclient, @id, 'transaction.summary', content
         . then (result) ->
             @log 'updated transaction summary with content', content
         , (error) ->
             @log "error in updating the transaction with id #{id}", error

    getTransaction:  ->
        body = query:filtered:{ query:{match_all:{}} , filter:range:timestamp:gte:"now/d"}
        return new promise (fulfill, reject) =>
            @eclient.search @eclient, @id, 'transaction.summary', body
            . then (results) =>
                if results.length > 1
                    @log "Warning More search results.", results
                    return fulfill results[0]
            , (error) =>
                return reject error


    getStats: (from, to, interval) ->
        interval ?= '1d'
        switch interval
            when 'month'
                interval = '1m'
            when 'day'
                interval = '1d'
            when 'year'
                interval = '1y'
        from  ?= '2014-01-01'
        to    ?= new Date()

        return new promise (fulfill, reject) =>
            body =
                aggregations:
                    stats:
                        date_historgram:
                            field:"timestamp"
                            interval: interval
                            format: 'yyyy-mm-dd'
                            extended_bounds:min:from, max:to
                        aggs:
                            # XXX Check the type of keys we will have in the DB
                            webVirusTransactions:sum:field:"transactions.webvirus.transactions"
                            webVirusViolations:sum:field:"transactions.webvirus.violations"
                            webContentFilteringTransactions:sum:field:"transactions.webcontentfiltering.transactions"
                            webContentFilteringViolations:sum:field:"transactions.webcontentfiltering.violations"
                            mailVirusTransactions:sum:field:"transactions.mailvirus.transactions"
                            mailVirusViolations:sum:field:"transactions.mailvirus.violations"

            @em.searchAsync @eclient, @id, 'transaction.summary', body
            . then (results) =>
                 # XXX Format the results
                 return fulfill results
            , (error) =>
                 return reject error

class AnalyzerServices extends SR
    constructor: (filename) ->
        @on 'load', (key, val) ->
            entry = new AnalyzerService key, val
            if entry?
                entry.saved = true
                @add key, entry
                entry.on 'email.virus', (emailData) =>
                    @emit 'email.virus', emailData


        @on 'removed', (key) ->
        super filename


    get: (key) ->
        entry = super key
        return unless entry? and entry.data?
        entry.data.id = entry.id
        entry.data

    getEntry: (key) ->
        return unless key
        @entries[key]

    add: (key, entry) ->
        return unless entry? and entry.data?
        entry.data.id = entry.id
        super key, entry

class AnalyzerManager extends SA

    constructor: (config) ->
        super config
        @config = config
        @import module

    run: (config) ->
        super config
        @log "data dir is ", @config
        @aservices = new AnalyzerServices "#{@config.datadir}/analyzerservices.db"
        @aservices.on 'email.virus', (emailData) =>
            @notifyUSG emailData

     addEventService: (service) ->
         @log "rcvd service", service
         return new promise (fulfill, reject) =>
             try
                aservice = new AnalyzerService null, service
             catch err
                 @log "error is ", err
                 return reject new Error err
             @aservices.add aservice.id, aservice
             return fulfill aservice.data

    updateEventService: (id, service) ->
        console.log "rcvd contents", "id: #{id}", "entry: ", service
        return new promise (fulfill, reject) =>
            try
                aservice = new AnalyzerService id, service
            catch err
                return reject new Error err
            entry = @aservices.update aservice.id, aservice
            return fulfill entry

     getStats: (id, params) ->
         entry = @aservices.getEntry id
         return entry.getStats params.from, params.to, params.interval

     notifyUSG: (emailData) ->
         postRequest emailData, @config.usgEmailNotify


module.exports = AnalyzerManager

