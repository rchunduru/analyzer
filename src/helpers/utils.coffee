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

module.exports.postRequest = postRequest = (body, url) ->
    parsedurl = parseUrl url
    options = {host:parsedurl.hostname, port:parsedurl.port, path:parsedurl.pathname, method:POST, headers:{'content-Type':"application/json"}}
    http = require 'http'
    req = http.request options, (res) ->
        console.log 'rcvd response for http request', res
    req.write body
    req.end()

module.exports.parseQuery = parseQuery = (query) ->
    querystring = require 'querystring'
    return querystring.parse query
