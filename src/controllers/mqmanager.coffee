stomp = require 'stompjs'
EventEmitter = require('events').EventEmitter

class MqController extends EventEmitter
    connect: (host, port, username, password, retryInterval, retryCount) ->
        connectcb = (frame) =>
            console.log "MQ connected"
            @emit 'mq.connected', @client

        errorcb = (error) =>
            # Reconnect again unless retry count is reached
            if @retryCount--
                setTimeOut connect, @retryInterval
            else
                console.log "MQ conenctivity failed. Error is ", error
                err = new Error
                err =
                    args: [error]
                    name: 'StormpConnectionError'
                    message: "Failed due to error #{error}"
                @emit 'mq.error', err

        connect = =>
            console.log "Debug: Stomp Client connecting to host: #{host} port: #{port}"
            headers = { login:username, passcode:password, 'client-id':'analyzer'}
            @client.connect headers, connectcb, errorcb

        @client = stomp.overTCP host, port
        @client.heartbeat.outgoing = 20000
        @client.heartbeat.incoming = 0
        connect()

    subscribe: (queue, handler) ->
        return @client.subscribe queue, handler

    unsubscribe: (subscription) ->
        subscription.unsubscribe()

    produce: (queue, body) ->
        @client.send queue, {},  JSON.stringify(body)

    ack: (message) ->
        message.ack()

    disconnect: ()->
        @client.disconnect()


module.exports = MqController
