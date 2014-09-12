url = require 'url'
promise = require 'bluebird'


module.exports.parseUInt = parseUInt = (str) ->
    #return 0 unless str?
    i = 0
    multiplier = 1
    sum = 0
    while i++ < str.length
        sum += (str[(str.length - i)] * multiplier)
        multiplier *= 10
    sum

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
    return new promise (fulfill, reject) =>
        parsedurl = parseUrl url
        options = {host:parsedurl.hostname, port:parsedurl.port, path:parsedurl.pathname, method:'POST', headers:{'content-Type':"application/json"}}
        console.log "Debug: util: options for http post req are ", options
        http = require 'http'
        req = http.request options, (res) =>
            #console.log 'rcvd response for http request', res
            if res.statusCode != 200
                return reject new Error "Failed with status code #{res.statusCode}"
            res.on 'data', (data) =>
                console.log "Response for POST is ", data
            res.on 'error', (error) =>
                console.log "Error: USG notification failed due to #{error}"
                return reject error
            res.on 'end', =>
                return fulfill "success"
        req.on 'error', (error) =>
            return reject error
        req.write JSON.stringify body if body
        req.end()

module.exports.parseQuery = parseQuery = (query) ->
    querystring = require 'querystring'
    return querystring.parse query


module.exports.getNextDay = getNextDay = (givenday) ->
    dlist = givenday.split "-"
    return new Error "UnSupported date format #{givenday}" unless dlist.length is 3
    year = parseUInt dlist[0]
    month = parseUInt dlist[1]
    day = parseUInt dlist[2]
  
    nextday = new Date()
    nextday.setFullYear year
    nextday.setMonth month
    nextday.setDate day+1

    followingday = nextday.getFullYear() + "-" + nextday.getMonth() + "-" + nextday.getDate()
    followingday 
