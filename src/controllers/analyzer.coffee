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

#eventAnalyzers = require './eventanalyzers'



class AnalyzerService extends SD
    schema =
        name: "EventService"
        type: "object"
        required: true
        additionalProperties: true
        properties:
            id:                 {"type": "string", "required": false}
            name:               {"type": "string", "required": false}
            topic:              {"type": "string", "required": true}
            identification:     {"type": "string", "required": true}

    constructor: (id, data) ->
        super id, data, schema
    

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

    match: (data) ->
        for key of @entries
            entry = @entries[key]
            return unless entry? and entry.data?
            instance = entry.data
            if instance.topic is data.topic
                return instance

mixof = (base, mixins...) ->
    class Mixed extends base
    for mixin in mixins by -1 #earlier mixins override later ones
        for name, method of mixin::
            Mixed::[name] = method
    Mixed

#class AnalyzerManager extends mixof SA, EM, MM
class AnalyzerManager extends SA

    constructor: (config) ->
        super config
        @config = config
        @import module
        @mq = new MM @config.mq
        @edb = new EM @config.edb

    run: (config) ->
        super config
        @log "data dir is ", @config
        @aservices = new AnalyzerServices "#{@config.datadir}/analyzerservices.db"

        # Connect to elasticdb
        if @config.edb
            @eclient = @edb.init @config.edb.host, @config.edb.logevel


        @mq.on 'mq.error', (err) ->
            # Recennect on error - Goes on
            @log "Failed to connect to MQ. Error is ", err
            setTimeout  connect, @config.retryInterval


        # Connect to MQ
        connect = =>
            @log "Connecting to MQ..."
            @mq.connect @config.mq.host, @config.mq.port, @config.mq.username, @config.mq.password, @config.mq.retryInterval, @config.mq.retryCount

        connect()

        @mq.on 'mq.connected', (mqclient) =>
            @mqclient = mqclient
            @log.debug "MQ client instance created to broker #{@config.mq.host} port #{@config.mq.port}"
            @aservices.on 'added', (aservice) =>
                aservice.subscription = @subscribe aservice.data.topic, @dummyHandler
                @log.debug "Subscribed to topic #{aservice.data.topic}"

            @aservices.on 'updated', (aservice) =>
                @unsubscribe aservice.subscription
                aservice.subscription = @subscribe aservice.data.topic, @dummyHandler
                @log.debug "Subscribed to topic #{aservice.data.topic}"

            @aservices.on 'removed', (aservice) =>
                @unsubscribe aservice.subscription


     addEventService: (service) ->
         @log "rcvd service", service
         return new promise (fulfill, reject) =>
             try
                aservice = new AnalyzerService null, service
             catch err
                 return reject new Error err

             old = @aservices.match aservice.data
             if old?
                 console.log "Found matching Analyzer service ", old
                 return fulfill 409
             @aservices.add  aservice.id, aservice
             console.log "New Analyzer service added", aservice
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

     getStats: (sid, apptype) ->
        return @getDocumentStats @dbclient, 'service', sid, apptype

     dummyHandler: (message) ->
        console.log "recvd message form ActiveMQ", message
        message.ack() if message

module.exports = AnalyzerManager

