elasticClient = require 'elasticsearch'
promise = require 'bluebird'
class ElasticController

    init: (@host, @loglevel) ->
        @loglevel ?= 'trace'
        return new elasticClient.Client host: @host, log: @loglevel, keepAlive:true

    deleteType: (dbclient, index, type) ->
        return dbclient.delete index, type, id:'_all'

    createDocument: (dbclient, index, type, body) ->
        content =
            index: index
            type: type
            body: body
        console.log "content is ", content
        return dbclient.create content
        

    deleteDocument: (dbclient, index, type, id) ->
        return dbclient.delete index:index, type:type, id:id, ignoreUnavailable:true

    deleteAllDocuments: (dbclient, index, type) ->
        return new promise (fulfill, reject) =>
            dbclient.delete index:index, type:type, id:'_all', ignoreUnavailable:true
             . then (response) =>
                return fulfill "success"
             , (error) =>
            	return reject error


    getDocument: (dbclient, index, type) ->
        content = {index:index}
        content.type = type if type
        return @search dbclient, content

    search: (dbclient, index, type,  content) ->
        return new promise (fulfill, reject) =>
            dbclient.search index:index, type:type, body:content
            . then (response) =>
                if response.hits? and response.hits.total >= 1
                    #results = (hit._source for hit in response.hits,hits)
                    return fulfill response.hits
                else
                    return fulfill []
            , (error) =>
                return reject error
    

module.exports = ElasticController
