#!/usr/bin/env node

var through2 = require("through2");
var http = require("http");
var minimist = require('minimist')(process.argv.slice(2));

if (minimist.verbose || minimist.v)
  process.env['DEBUG'] = 'http-replayer';

var debug = require('debug')('http-replayer');

var JobPuller = require("./JobPuller");
var streamLimiter = require("./stream-rate-limiter");

var puller = new JobPuller();

var replayTarget = minimist.host || '127.0.0.1';
var replayPort = minimist.port || 8081;



var stream = streamLimiter(20, function (strJob, done) {
  var job = JSON.parse(strJob);
  var options = {
    hostname: replayTarget,
    port: replayPort,
    path: job.url,
    method: job.method,
    headers: job.headers,
    agent:false
  }
  http.get(options, function (res) {
    res.socket.end()
    if (res.statusCode===200) {
      puller.finishJob(strJob)
    } else {
      console.log('erroredJob')
      puller.erroredJob(strJob)
    }
    done(null, job)
  }).on('error', function(e) {
    puller.erroredJob(strJob)
    setTimeout(function(){
      console.log(e)
      done(null, job)
    }, 250)
  });

})

puller.jobProcess = function (strJob) {
  stream.write(strJob);
};

puller.start(minimist)

console.log('started')
