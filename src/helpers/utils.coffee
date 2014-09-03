url = require 'url'
promise = require 'bluebird'

module.exports.parseUrl = parseUrl = (givenUrl) ->
    parsedurl = url.parse givenUrl, true
    return parsedurl

module.exports.parseMessage = parseMessage =  (message) ->
    parser = require('packet').createParser()
    return new promise (fulfill, reject) =>
        parser.extract "l32 =>size", (record) =>
            size = record.size
            data = message.toString 'utf-8', 4, size
            return fulfill JSON.parse data
        parser.parse message

