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

    config = require('../package.json').config

    storm = null

    agent = new Analyzer config
    agent.run storm
