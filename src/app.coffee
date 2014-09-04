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
    config.edb =
        #host:'control3.dev.intercloud.net:2358'
        loglevel: 'trace'

    config.datadir = argv.d ? "/tmp"

    storm = null

    agent = new Analyzer config
    agent.run storm
