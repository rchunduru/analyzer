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
            host = parsedurl.hostname
            if parsedurl.port is not 9200
              host = parsedurl.host
            @eclient = @em.init host:host, loglevel:'error'
            @log "Connected to the Elasticsearch DB", @data.output

    subscribe: ->
        loggersyslog = (message) =>

            #@log "Rcvd message from MQ", message
            return if message.length < 20

            msg  = @ea.stripHeader message.body
            @log "Debug: After stripping the header, the msg header is ", msg.header
            return if msg is {}
           
            #process it if the cname is in our list
            cname = @data.sources.filter (source) =>
                source is msg.header.cname 
            @log "Debug: result of cname is ", cname
            #return if cname.length is 0
            switch msg.header.format.toLowerCase()
                    when 'email.av', 'email.virus'
                        @ea.emailvirus msg
                         . then (body) =>
                             body.id = @id
                             @mailvirus body
                             #message.ack()
                             return

                    else
                        data = msg.data
                        # Few validations on the data
                        return unless data.count? and data.start?
                        # First get the key as transactions or violations
                        strarray = msg.header.format.split(".")
                        return if strarray.length == 1
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
                                return # message.ack()
                            , (error) =>
                                @log "Debug: Error in updating the transaction", error, content._source
                                return # message.ack()
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
                                return # message.ack()
                             , (error) =>
                                 @log " Failed to create a transaction record in Elastic search", error, content
                                 return

        # XXX Add async loop here
        for topic in @topics
            (@subscriptions ?= {})[b = topic] = @mq.subscribe "/topic/#{topic}", loggersyslog

    mqUnsubscribe: ->
        return unless @mqclient?
        for key of @subscriptions
            @mq.unsubscribe  @subscriptions[key]
        @mq.disconnect()


    mailvirus: (emailData) ->
        #Write onto elastic DB
        @em.createDocument @eclient, @id, 'email.virus', emailData
            . then (response) =>
                @log "Added email virus ", response
                @emit 'email.virus', @id, emailData
            , (error) =>
                @log "Error while adding email virus into elastic DB", error

    cleanElasticDocuments: ->
        return new promise (fulfill, reject) =>
            # XXX Need to maintain state if this instance has ever wrote content into Elastic DB
            # If so, need to throw error if elastic servers are not reachable
            return fulfill unless @eclient
            @em.deleteAllDocumentsAsync @eclient, @id, 'email.virus'
            . then (response) =>
                 @log "Deleted all documents of type email.virus"
                 @em.deleteAllDocumentsAsync @eclient, @id, 'transaction.summary'
                 . then (response) =>
                     @log "Deleted all documents of type transaction.summary"
                     return fulfill "success"
                 , (error) =>
                     @log "error while cleaning transaction.summary documents for instance #{@id}", error
                     return reject new Error error
            , (error) =>
                 @log "error while cleaning email.virus documents for instance #{@id}", error
                 return reject new Error error
        

    createTransaction: (content) ->
        @em.createDocument @eclient, @id, 'transaction.summary', content
         . then (result) ->
             @log 'added transaction summary record with content', content
         , (error) ->
             @log 'error in adding transaction record', error

    updateTransaction: (id, content) ->
        @em.updateDocument @eclient, @id, 'transaction.summary', content
         . then (result) ->
             @log 'updated transaction summary with content', content
         , (error) ->
             @log "error in updating the transaction with id #{id}", error

    getTransaction:  ->
        body = query:filtered:{ query:{match_all:{}} , filter:range:timestamp:gte:"now/d"}
        return new promise (fulfill, reject) =>
            @em.search @eclient, @id, 'transaction.summary', body
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

    cleanup: ->
        @mqUnsubscribe()
        return @cleanElasticDocuments()
        


class AnalyzerServices extends SR
    constructor: (filename) ->
        @on 'load', (key, val) ->
            entry = new AnalyzerService key, val
            if entry?
                entry.saved = true
                @add key, entry
                entry.on 'email.virus', (id, emailData) =>
                    @emit 'email.virus', id, emailData


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


    remove: (key) ->
        entry = @getEntry key
        entry.cleanup()
         . then (resp) =>
             super key
         , (error) =>
             @log "Error in removing the analyzer instance."
             return new Error "Failed to cleanup the DB or MQ due to error #{error}"
           

class AnalyzerManager extends SA

    constructor: (config) ->
        super config
        @import module

    run: (config) ->
        super config
        @log "data dir is ", @config
        @aservices = new AnalyzerServices "#{@config.datadir}/analyzerservices.db"
        @aservices.on 'email.virus', (id, emailData) =>
            @log "Debug: Notifying USG #{@config.usgEmailNotify} with email Data for instance#{id}"
            @notifyUSG emailData
             . then (response) =>
                 @log "Successfully updated the USG", response
             , (error) =>
                 @log "Failed to notify USG", error    

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
         return postRequest emailData, @config.usgEmailNotify
         


module.exports = AnalyzerManager

