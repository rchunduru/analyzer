Analyzer = require('./controllers/analyzer')

        
if require.main is module
    argv = require('minimist')(process.argv.slice(2))
    if argv.h?
        console.log """
            -h view this help
            -p port number
            -l logfile
            -d datadir
        """
        return

    config = {}
    config.port = argv.p ? 9000
    config.logfile = argv.l ? "/tmp/log/analyzer.log"
    config.mq =
        host:'control3.dev.intercloud.net'
        port:61616
        username: 'admin'
        password: 'admin'
        retryInterval: 8000
        retryCount: 3
    config.edb =
        #host:'control3.dev.intercloud.net:2358'
        host:'10.1.10.187:9200'
        loglevel: 'trace'

    config.datadir = argv.d ? "/tmp"

    storm = null

    agent = new Analyzer config
    agent.run storm
