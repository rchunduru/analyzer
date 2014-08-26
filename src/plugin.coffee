
@include = ->
    agent = @settings.agent


    @post '/event/consumers':  ->
        agent.addEventService @body
         .then  (service) =>
            @send service
          , (error) =>
              @next error

    @get '/event/consumers/:id': ->
        match = agent.aservices.get @params.id
        unless match is undefined
            @send match
        else
            @send 404

    @get '/event/consumers': ->
        @send agent.aservices.list()

    @put '/event/consumers/:id':  ->
        agent.updateEventService @params.id, @body
         .then (service) =>
            @send service
          , (error) =>
              @next error

    @delete  '/event/consumers/:id': ->
        agent.aservices.remove @params.id
        @send 204

    @get '/event/stats': ->
        agent.getStats @params.id, @query
         .then(response) =>
             @send response
          , (error) =>
              @next error
