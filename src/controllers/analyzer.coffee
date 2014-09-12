body:promise = require 'bluebird'
bunyan = require 'bunyan'
EM = require './elasticmanager'
MM = require './mqmanager'
validate = require('json-schema').validate
uuid = require 'node-uuid'
async = require 'async'

async = require 'async'
SR = require('stormagent').StormRegistry
SD = require('stormagent').StormData
SA = require 'stormagent'

EA = require './eventanalyzers'
parseurl = require('../helpers/utils').parseUrl
parseMessage = require('../helpers/utils').parseMessage
postRequest = require('../helpers/utils').postRequest
parsequery = require('../helpers/utils').parseQuery
parseUInt = require('../helpers/utils').parseUInt



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
        #@em  = promise.promisifyAll(EM.prototype)
        @em  = new EM
        @mq = new MM
        @ea = new EA
        @susbscriptions = {}
        @topics = []
        @mqConnect()
        @dbConnect()
        @forEver()
        

    mqConnect:  ->
        if @data and @data.sources?
            @data.sources.map (source) =>
                parsedurl = parseurl source
                # Build the topics ready for subscription
                #@log "Debug: parsedurl query is ", parsedurl.query
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
            # XXX - Make this a promise. For now try catch the error
            try
                msg  = @ea.stripHeader message.body
            catch err
                @log "Error in stripping header", err
                return
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
                        return unless data.count? and data.timestamp?
                        # First get the key as transactions or violations
                        strarray = data.format.split(".")
                        return if strarray.length == 1
                        bodykey = strarray.pop()
                        # Get the key as "web.contentfiltering" etc.,
                        objkey = ""
                        for str in strarray
                            objkey += str

                        objkey = objkey.toLowerCase()
                        bodykey = bodykey.toLowerCase()
                        @log "Debug: generated objkey #{objkey} and bodykey as #{bodykey}"
                        data.count = parseUInt data.count
                        return unless data.timestamp?
                        @getTransaction data.timestamp
                        . then (content) =>
                            content._source.transactions ?= {}
                            # Get the data.format into the key of content._source.transactions

                            (content._source.transactions)[b = objkey] ?= {}
                            # Now set the content of the created object
                            contentvalue = (content._source.transactions)[b = objkey]
                            (contentvalue)[b = bodykey] ?= 0
                            (contentvalue)[b = bodykey] += data.count
                            @log "Debug: existing: generated transaction is ", content._source.transactions

                     
                            @updateTransaction content._id, content._source
                            . then (result) =>
                                return 
                            , (error) =>
                                @log "Debug: Error in updating the transaction", error, content._source
                                return 
                        , (error) =>
                            #write onto Elastic DB
                            @log "Creating a new data record"
                            (transaction = {})[b = objkey] ?= {}
                            contentvalue = (transaction)[b = objkey]
                            (contentvalue)[b = bodykey] ?= 0
                            (contentvalue)[b = bodykey] = data.count
                            content =
                                id: @id
                                timestamp: data.timestamp
                                transactions:transaction
                            @log "Debug: generated content is ", content
                            @createTransaction content
                             .then (result) =>
                                return
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
        @em.createDocument @eclient, 'email.virus', @id,  emailData
            . then (response) =>
                @log "Added email virus ", response
                @emit 'email.virus', @id, response._id, emailData
            , (error) =>
                @log "Error while adding email virus into elastic DB", error

    deleteDocument: (index, type, id) ->
        return @em.deleteDocument @eclient, index, type, id

    cleanElasticDocuments: ->
        return new promise (fulfill, reject) =>
            # XXX Need to maintain state if this instance has ever wrote content into Elastic DB
            # If so, need to throw error if elastic servers are not reachable
            return fulfill unless @eclient
            @em.deleteAllDocumentsAsync @eclient, 'email.virus', @id
            . then (response) =>
                 @log "Deleted all documents of type email.virus"
                 @em.deleteAllDocumentsAsync @eclient, 'transaction.summary', @id
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
        return new promise (fulfill, reject) =>    
            @em.createDocument @eclient, 'transaction.summary', @id,  content
             . then (result) =>
                 @log 'added transaction summary record with content', content
                 return fulfill result._id
             , (error) =>
                 @log 'error in adding transaction record', error
                 return reject error

    updateTransaction: (id, content) ->
        @em.updateDocument @eclient, 'transaction.summary', @id, id, content
         . then (result) =>
             @log 'updated transaction summary with content', content
         , (error) =>
             @log "error in updating the transaction with id #{id}", error

    getTransaction: (timestamp)  ->
        #body = query:filtered:{ query:{match_all:{}} , filter:range:timestamp:gte:timestamp}
        today = new Date()
        stoday = today.getFullYear() + "-" + today.getMonth() + "-" + today.getDate()
        body = query:filtered:{ query:{match_all:{}} , filter:range:timestamp:gte:timestamp}
        return new promise (fulfill, reject) =>
            @em.search @eclient, 'transaction.summary', @id, body
            . then (results) =>
                return reject new Error "no results found" if Object.keys(results).length is 0
                @log "Got some results", results.hits
                return reject new Error "no results found" if results.hits.hits.length is 0
                if results.hits.hits.length >= 1
                    @log "Warning More search results.", results.hits.hits
                    return fulfill results.hits.hits[0]
                else
                    return reject new Error "no results found"
            , (error) =>
                @log "get transaction failed with error", error
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
        unless to?
            todate = new Date()
            to = todate.getFullYear() + "-" + todate.getMonth() + "-" + todate.getDay()

        return new promise (fulfill, reject) =>
            body =
                aggregations:
                    analyzerstats:
                        date_histogram:
                            field:"timestamp"
                            interval: interval
                            format: 'yyyy-MM-dd'
                            extended_bounds:min:from, max:to
                        aggs:
                            # XXX Check the type of keys we will have in the DB
                            webVirusTransactions:sum:field:"transactions.webvirus.transactions"
                            webVirusViolations:sum:field:"transactions.webvirus.violations"
                            webContentFilteringTransactions:sum:field:"transactions.webcontentfiltering.transactions"
                            webContentFilteringViolations:sum:field:"transactions.webcontentfiltering.violations"
                            mailVirusTransactions:sum:field:"transactions.mailvirus.transactions"
                            mailVirusViolations:sum:field:"transactions.mailvirus.violations"

            @em.search @eclient, 'transaction.summary', @id, body
            . then (results) =>
                 # XXX Format the results
                 @log "results for getstats are ", results
                 return fulfill [] if Object.keys(results).length is 0
                 response =
                     stats: results.aggregations.analyzerstats.buckets
                     id: @id
                 return fulfill response
            , (error) =>
                 @log "Debug: getStats() error is ", error
                 return reject error

    cleanup: ->
        @mqUnsubscribe()
        return @cleanElasticDocuments()
        
    forEver: ->
             
        processEmail = (entry) =>
            return new promise (fulfill, reject) =>
                @emit 'email.virus', @id, entry._id, entry._source
                return fulfill entry._id

        async.whilst(
            () ->
                1
            (searchloop) =>
                body = query:filtered:{ query:{match_all:{}}}
                @em.search @eclient, 'email.virus', @id, body
                 . then (results) =>
                     return if Object.keys(results).length is 0
                     if results? and results.hits? and results.hits.hits? and results.hits.hits.length is 0
                         return
                     promise.all results.hits.hits.map (result) =>
                             return processEmail result
                              . then (resp) =>
                                 return resp
                     . then (totalResults) =>
                          @log "total results are ", totalResults
                  , (error) =>
                      @log "Oops, failed to search", error

                setTimeout searchloop, 20000000 # set a configurable value
            (err) =>
                @log "Alert: Should not be here", err
        )


class AnalyzerServices extends SR
    constructor: (filename) ->
        @on 'load', (key, val) ->
            entry = new AnalyzerService key, val
            if entry?
                entry.saved = true
                @add key, entry
                entry.on 'email.virus', (type, recordId, emailData) =>
                    @emit 'email.virus', type, recordId, emailData


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
        @aservices.on 'email.virus', (type, recordId, emailData) =>
            @log "Debug: Notifying USG #{@config.usgEmailNotify} with email Data for instance#{type} and recordId #{recordId}"
            @notifyUSG emailData
             . then (response) =>
                 @log "Successfully updated the USG", response
                 entry = @aservices.getEntry type
                 entry.deleteDocument 'email.virus', type, recordId
                 . then (resp) =>
                     @log "successfully deleted the document of type #{type} and recordId #{recordId}"
                 , (error) =>
                     @log "Failed to delete document", error
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
         @log "Debug: entry is #{entry} and params are #{params}"
         return entry.getStats params.from, params.to, params.interval

     notifyUSG: (emailData) ->
         return postRequest emailData, @config.usgEmailNotify
         


module.exports = AnalyzerManager

