elasticClient = require 'elasticsearch'
class ElasticController

    init: (@host, @loglevel) ->
        @loglevel ?= 'trace'
        return new elasticClient.Client host: @host, log: @loglevel, keepAlive:true

    deleteType: (dbclient, index, type, callback) ->
        dbClient.delete index, type, (error) =>
            return callback error

    #createDocument: (dbclient, index, type, body, callback) ->
    createDocument: (dbclient, index, type, body) ->
        content =
            index: index
            type: type
            body: body
        console.log "content is ", content
        ###
        dbclient.create content, (error, response) =>
            return callback error, response
        ###
        return dbclient.create content
        

    deleteDocument: (dbclient, index, type, id, callback) ->
        dbclient.delete index:index, type:type, id: id, ignoreUnavailable:true, (error, response) =>
            return callback error, response

    deleteAllDocuments: (dbclient, index, type, callback) ->
        dbclient.delete index:index, type:type, id:'_all', ignoreUnavailable:true, (error, results) =>
            return callback "success" unless error instanceof Error
            return callback error


    getDocument: (dbclient, index, type, callback) ->
        content = {index:index}
        content.type = type if type
        return @search dbclient, content, callback

    search: (dbclient, index, type,  content, callback) ->
        dbclient.search index:index, body:content, (error, response) =>
            unless error instanceof Error
                if response.hits? and response.hits.total > 1
                    #results = (hit._source for hit in response.hits,hits)
                    callback null, response.hits
            else
                callback error
    

module.exports = ElasticController
