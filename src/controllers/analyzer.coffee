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
helper = require '../helpers/utils'


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


    constructor: (id, data, @config) ->
        super id, data, schema
        @em  = promise.promisifyAll(EM.prototype)
        @mq = new MM
        @ea = new EA
        @susbscriptions = {}
        @topics = []
        @mqConnect()
        @dbConnect()
        @getTransaction()

    mqConnect:  ->
        if @data and @data.sources?
            @data.sources.map (source) =>
                parsedurl = helper.parseUrl source
                # Build the topics ready for subscription
                if parsedurl.query? and parsedurl.query.topic?
                    parsedurl.query.topic.map (topic) =>
                        @topics.push topic

                connect = =>
                    @mq.connect parsedurl.host, parsedurl.port, parsedurl.username, parsedurl.password, 10000, 5
                @mq.on 'mq.connected', (client) =>
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
            parsedurl = helper.parseurl entry.output
            @eclient = @em.init "#{parsedurl.host}:#{parsedurl.port}", @config.loglevel

    subscribe: ->
            @topics.map (topic) =>
                # XXX Use eval to build the handlers
                switch topic
                    when 'web.contentfiltering.transactions'
                        @subscriptions[topic] = @subscribe "/topic/#{topic}", @webcontentfilteringtransactions
                    when 'web.contentfiltering.violations'
                        @subscriptions[topic] = @subscribe "/topic/#{topic}", @webcontentfilteringviolations
                    when 'mail.virus.result'
                        @subscriptions[topic] = @subscribe "/topic/#{topic}", @mailvirusresult
                    when 'mail.virus.violations'
                        @subscriptions[topic] = @subscribe "/topic/#{topic}", @mailvirusviolations
                    when 'mail.virus.transactions'
                        @subscriptions[topic] = @subscribe "/topic/#{topic}", @mailvirustransactions
                    when 'web.virus.violations'
                        @subscriptions[topic] = @subscribe "/topic/#{topic}", @webvirusviolations
                    when 'web.virus.transactions'
                        @subscriptions[topic] = @subscribe "/topic/#{topic}", @webvirustransactions
                    when 'transactions'
                        @subscriptions[topic] = @subscribe "/topic/#{topic}", @transactions


    mailvirusresult: (message) ->
        helper.parseMessage message
            . then (emailData) =>
                @log "Email Data recvd is", emailData
                #Write onto elastic DB
                @eclient.createDocumentAsync @id, 'email.virus', emailData
                    . then (response) ->
                        @log "Added email virus ", response
                , (error) ->
                    @log "Error while adding email virus into elastic DB", error

    webcontentfilteringtransactions: (message) ->
        helper.parseMessage message
            . then (transaction) =>
                @getTransaction()
                    . then (content) =>
                         content._source.transactions ?= {}
                         content._source.transactions.webContentFiltering ?=  {}
                         content._source.transactions.webContentFiltering.transactions += transaction.count
                         @updateTransaction content._id, content._source
              , (error) =>
                  #write onto Elastic DB
                  entry =
                      timestamp: transaction.start
                      transactions:
                        webContentFiltering:
                            transactions: transaction.count
                @createTransaction content

    webcontentfilteringviolations: (message) ->
       helper.parseMessage message
        . then (transaction) =>
            @getTransaction()
             . then (content) =>
                 content._source.transactions ?= {}
                 content._source.transactions.webContentFiltering ?=  {}
                 content._source.transactions.webContentFiltering.violations += transaction.count
                 @updateTransaction content._id, content._source
              , (error) =>
                #write onto Elastic DB
                entry =
                    timestamp: transaction.start
                    transactions:
                        webContentFiltering:
                            violations: transaction.count
                @createTransaction content

    emailvirusviolations: (message) ->
       helper.parseMessage message
        . then (transaction) =>
            @getTransaction()
             . then (content) =>
                 content._source.transactions ?= {}
                 content._source.transactions.emailVirus?=  {}
                 content._source.transactions.emailVirus.violations += transaction.count
                 @updateTransaction content._id, content._source
              , (error) =>
                #write onto Elastic DB
                entry =
                    timestamp: transaction.start
                    transactions:
                        emailVirus:
                            violations: transaction.count
                @createTransaction content

    emailvirustransactions: (message) ->
       helper.parseMessage message
        . then (transaction) =>
            @getTransaction()
             . then (content) =>
                 content._source.transactions ?= {}
                 content._source.transactions.emailVirus?=  {}
                 content._source.transactions.emailVirus.transactions += transaction.count
                 @updateTransaction content._id, content._source
              , (error) =>
                #write onto Elastic DB
                entry =
                    timestamp: transaction.start
                    transactions:
                        emailVirus:
                            transactions: transaction.count
                @createTransaction content

    webvirusviolations: (message) ->
       helper.parseMessage message
        . then (transaction) =>
            @getTransaction()
             . then (content) =>
                 content._source.transactions ?= {}
                 content._source.transactions.webVirus?=  {}
                 content._source.transactions.webVirus.violations += transaction.count
                 @updateTransaction content._id, content._source
              , (error) =>
                #write onto Elastic DB
                entry =
                    timestamp: transaction.start
                    transactions:
                        webVirus:
                            violations: transaction.count
                @createTransaction content

    webvirustransactions: (message) ->
       helper.parseMessage message
        . then (transaction) =>
            @getTransaction()
             . then (content) =>
                 content._source.transactions ?= {}
                 content._source.transactions.webVirus?=  {}
                 content._source.transactions.webVirus.transactions += transaction.count
                 @updateTransaction content._id, content._source
              , (error) =>
                #write onto Elastic DB
                entry =
                    timestamp: transaction.start
                    transactions:
                        webVirus:
                            transactions: transaction.count
                @createTransaction content

    transactions: (message) ->
       helper.parseMessage message
        . then (transaction) =>
            @getTransaction()
             . then (content) =>
                 content._source.transactions ?= {}
                 content._source.transactions.count ?= 0
                 content._source.transactions.count += transaction.count
                 @updateTransaction content._id, content
              , (error) =>
                #write onto Elastic DB
                entry =
                    timestamp: transaction.start
                    transactions:
                        count: transaction.count
                @createTransaction content



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
                            webVirusTransactions:sum:field:"transactions.webVirus.transactions"
                            webVirusViolations:sum:field:"transactions.webVirus.violations"
                            webContentFilteringTransactions:sum:field:"transactions.webContentFiltering.transactions"
                            webContentFilteringViolations:sum:field:"transactions.webContentFiltering.violations"
                            mailVirusTransactions:sum:field:"transactions.mailVirus.transactions"
                            mailVirusViolations:sum:field:"transactions.mailVirus.violations"

            @eclient.search @id, 'transaction.summary', body
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

        @on 'removed', (key) ->
        super filename

    get: (key) ->
        entry = super key
        return unless entry?
        if entry.data? and entry.data instanceof AnalyzerService
            entry.data.id = entry.id
            entry.data
        else
            entry








#class AnalyzerManager extends mixof SA, EM, MM
class AnalyzerManager extends SA

    constructor: (config) ->
        super config
        @config = config
        @import module

    run: (config) ->
        super config
        @log "data dir is ", @config
        @aservices = new AnalyzerServices "#{@config.datadir}/analyzerservices.db"

     addEventService: (service) ->
         @log "rcvd service", service
         return new promise (fulfill, reject) =>
             try
                aservice = new AnalyzerService null, service
             catch err
                 return reject new Error err
             return fulfill aservice

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
         entry = @aservices.get id
         return entry.data.getStats params.from, params.to, params.interval

     dummyHandler: (message) ->
        console.log "recvd message form ActiveMQ", message
        message.ack() if message

module.exports = AnalyzerManager

