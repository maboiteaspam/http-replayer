#!/usr/bin/env node

var http = require("http");
var minimist = require('minimist')(process.argv.slice(2));

if (minimist.verbose || minimist.v)
  process.env['DEBUG'] = 'http-replayer';

var debug = require('debug')('http-replayer');

var JobPuller = require("./JobPuller");

var puller = new JobPuller();

var replayTarget = minimist.host || '127.0.0.1';
var replayPort = minimist.port || 8081;

puller.jobProcess = function (strJob, cb) {
  var job = JSON.parse(strJob);
  var options = {
    hostname: replayTarget,
    port: replayPort,
    path: job.url,
    method: job.method,
    headers: job.headers
  }
  http.get(options, function (res) {
    res.socket.end()
    if (res.statusCode===200) {
      puller.finishJob(strJob)
    } else {
      console.log('erroredJob')
      puller.erroredJob(strJob)
    }
    cb(null, job)
  }).on('error', function(e) {
    puller.erroredJob(strJob)
    setTimeout(function(){
      console.log(e)
      cb(null, job)
    }, 250)
  });
};

puller.start(minimist)
console.log('started')
