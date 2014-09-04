
@include = ->
    agent = @settings.agent


    @post '/analyzers':  ->
        agent.addEventService @body
         .then  (service) =>
            @send service
          , (error) =>
              @next "#{error}"

    @get '/analyzers/:id': ->
        match = agent.aservices.get @params.id
        unless match is undefined
            @send match
        else
            @send 404

    @get '/analyzers': ->
        @send agent.aservices.list()

    @put '/analyzers/:id':  ->
        agent.updateEventService @params.id, @body
         .then (service) =>
            @send service
          , (error) =>
              @next error

    @delete  '/analyzers/:id': ->
        agent.aservices.remove @params.id
        @send 204

    @get '/analyzers/:id/stats': ->
        agent.getStats @params.id, @query
         .then(response) =>
             @send response
          , (error) =>
              @next error
