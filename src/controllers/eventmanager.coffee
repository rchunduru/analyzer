promise = require 'bluebird'
dbController = require './database'
mqController = require './mq'
validate = require('json-schema').validate
uuid = require 'node-uuid'

async = require 'async'

#eventAnalyzers = require './eventanalyzers'

db = promise.promisifyAll  dbController
mq = promise.promisifyAll  mqController
#promise.promisifyAll  eventAnalyzers

mixof = (base, mixins...) ->
    class Mixed extends base
    for mixin in mixins by -1 #earlier mixins override later ones
        for name, method of mixin::
            Mixed::[name] = method
    Mixed

module.exports.validateEventQueueService = validateEventQueueService = ->
    schema =
        name: "EventService"
        type: "object"
        additionalProperties: false
        properties:
            id:                 {"type": "string", "required": "false"}
            name:               {"type": "string", "required": "false"}
            topic:              {"type": "string", "required": "true"}
            identification:     {"type": "string", "required": "true"}
    result = validate @body, schema
    return @next new Error "Invalid Event Queue Service schema! #{JSON.stringify result.errors}" unless result.valid
    @next()

class Base
    constructor: ->

class EventServiceManager extends mixof Base , dbController, mqController

    constructor: (config) ->
        super()
        @eServices = []
        if config.db
            @dbclient = @initDb config.db.host, config.db.loglevel
        @on 'connected', (client)  ->
            @mqclient = client

        @on 'error', (err) ->
            # Recennect on error - Goes on
            setTimeout  connect, config.retryInterval

        @logger = config.logger
        @logger.debug dbclient:@dbclient, mqclient:@mqclient
        connect = =>
            @connectMq config.mq.host, config.mq.port, config.mq.username, config.mq.password, config.mq.retryInterval, config.mq.retryCount

        connect() if config.mq


     list: ->
         if @eServices.length is 0
            @getDocument @dbclient, 'service', null,  (error, document) =>
                if document.hits and document.hits.total > 0
                 @eServices = document.hits.hits

     _get: (topic) ->
         @list()
         existing = []
         existing = @eServices.filter (svc) ->
             return false if svc._source.topic is topic
         existing
     

     add: (service) ->
         return new promise (fulfill, reject) =>
             #check if existing document
             existing = _get(service.topic)
             return fulfill "{result: existing}" if existing.length
             service.id = uuid.v4()

             @createDocument @dbclient, 'service', service.id , service, (error, newservice) =>
                    return reject error if error
                    subscription = @subscribe service.topic,  @dummyhandler
                    @logger.debug " new service body is ", newservice
                    newservice.subscription = subscription
                    @eServices.push newservice
                    @logger.debug "error is ", error, "newservice is ", newservice
                    return fulfill "{result:success}"


     modify: (sid, service) ->
         return new promise (fulfill, reject) =>
             @list()
             slist = @eServices.filter (svc) ->
                 return false if svc._source.id is sid
             return reject if slist.length is 0
             oldservice = slist[0]
             @modifyDocument @dbclient, 'service', sid, service, (error, newservice) =>
                 return reject error if error
                 @unsubscribe slist.subscription
                 @subscribe service.topic, @dummyhandler
                 @eServices = []
                 return fulfill "{result:success}"

     delete: (sid) ->
         return new promise (fulfill, reject) =>
             async.each @eServices,
                 (svc, next) =>
                     return next() unless svc._source.id is sid
                     console.log  "svc about to be deleted is ", svc
                     @deleteDocument @dbclient, svc._index, svc._type, svc._id, (error, response) =>
                         return next(error) if error
                         next()
                 (result) =>
                     return reject error if result? and result instanceof Error
                     @_get()
                     return fulfill ""

     get: (sid) ->
        return new promise (fulfill, reject) =>
            @getDocument @dbclient, 'service', sid,  (error, document) =>
                return reject error if error instanceof Error
                if document.hits and document.hits.total > 0
                    service =  document.hits.hits
                    return fulfill service
                else
                    return fulfill []

     getStats: (sid, apptype) ->
         return @getDocumentStats @dbclient, 'service', sid, apptype

     dummyHandler: (message) ->
         @logger.debug "recvd message form ActiveMQ", message
         message.ack() if message

module.exports.EventServiceManager = EventServiceManager
