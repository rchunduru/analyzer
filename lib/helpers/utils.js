// Generated by CoffeeScript 1.8.0
(function() {
  var getNextDay, parseMessage, parseQuery, parseUInt, parseUrl, postRequest, promise, url;

  url = require('url');

  promise = require('bluebird');

  module.exports.parseUInt = parseUInt = function(str) {
    var i, multiplier, sum;
    i = 0;
    multiplier = 1;
    sum = 0;
    while (i++ < str.length) {
      sum += str[str.length - i] * multiplier;
      multiplier *= 10;
    }
    return sum;
  };

  module.exports.parseUrl = parseUrl = function(givenUrl) {
    var parsedurl;
    parsedurl = url.parse(givenUrl, true);
    return parsedurl;
  };

  module.exports.parseMessage = parseMessage = function(message) {
    var parser;
    parser = require('packet').createParser();
    return new promise((function(_this) {
      return function(fulfill, reject) {
        parser.extract("l32 =>size", function(record) {
          var data, size;
          size = record.size;
          data = message.toString('utf-8', 4, size);
          return fulfill(JSON.parse(data));
        });
        return parser.parse(message);
      };
    })(this));
  };

  module.exports.postRequest = postRequest = function(body, url) {
    return new promise((function(_this) {
      return function(fulfill, reject) {
        var http, options, parsedurl, req;
        parsedurl = parseUrl(url);
        options = {
          host: parsedurl.hostname,
          port: parsedurl.port,
          path: parsedurl.pathname,
          method: 'POST',
          headers: {
            'content-Type': "application/json"
          }
        };
        console.log("Debug: util: options for http post req are ", options);
        http = require('http');
        req = http.request(options, function(res) {
          if (res.statusCode !== 200) {
            return reject(new Error("Failed with status code " + res.statusCode));
          }
          res.on('data', function(data) {
            return console.log("Response for POST is ", data);
          });
          res.on('error', function(error) {
            console.log("Error: USG notification failed due to " + error);
            return reject(error);
          });
          return res.on('end', function() {
            return fulfill("success");
          });
        });
        req.on('error', function(error) {
          return reject(error);
        });
        if (body) {
          req.write(JSON.stringify(body));
        }
        return req.end();
      };
    })(this));
  };

  module.exports.parseQuery = parseQuery = function(query) {
    var querystring;
    querystring = require('querystring');
    return querystring.parse(query);
  };

  module.exports.getNextDay = getNextDay = function(givenday) {
    var day, dlist, followingday, month, nextday, year;
    dlist = givenday.split("-");
    if (dlist.length !== 3) {
      return new Error("UnSupported date format " + givenday);
    }
    year = parseUInt(dlist[0]);
    month = parseUInt(dlist[1]);
    day = parseUInt(dlist[2]);
    nextday = new Date();
    nextday.setFullYear(year);
    nextday.setMonth(month);
    nextday.setDate(day + 1);
    followingday = nextday.getFullYear() + "-" + nextday.getMonth() + "-" + nextday.getDate();
    return followingday;
  };

}).call(this);
