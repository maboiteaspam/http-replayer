
var redis = require("redis");
var async = require("async");
var _ = require("underscore");
var debug = require('debug')('http-replayer');

var debugStat = function(w,a,b,c,d,e){
  var args = Array.prototype.slice.call(arguments);
  args[0] = '  '+args[0];
  debug.apply(debug, args);
};

module.exports = function (){

  var that = this;
  var client;
  var jobsRealized = 0;
  var jobQueueLength = 50;

  this.start = function (opts, len) {
    if (client) return client
      client = redis.createClient(
        opts.rport || 6379,
        opts.rhost || '127.0.0.1',
        opts.ropts && JSON.parse(opts.ropts) || {

        });
    jobQueueLength = len && parseInt(len) || jobQueueLength;
    return client
  };
  this.end = function () {
    client.end();
  };

  var currentBlockKey = false
  var currentBlockKeys = []
  var currentJobs = []
  var finishedJobs = [];


  this.getJobs = function (then) {
    var todo = [];
    if (!currentBlockKeys.length) {
      todo.push(function(next){
        that.fetchBlockKeys(function (keys) {
          currentBlockKeys = currentBlockKeys.concat(keys)
          next()
        })
      })
    }
    if (!currentJobs.length) {
      todo.push(function(next){
        pullJobs(function (jobs) {
          next()
        })
      })
    }
  };
  this.finishJob = function (err, job) {
    finishedJobs.push(job)
    currentJobs = _.without(currentJobs, job);
  };
  this.erroredJob = function (job) {
    currentJobs = _.without(currentJobs, job);
  };


  this.fetchJobs = function (blockKey, done) {
    client.lrange('r'+blockKey, 0, 500, done);
  };

  this.fetchBlockKeys = function (done) {
    debug('pullBlockKeys')
    client.smembers('blockKeys', function (err, res) {
      res.sort(blockKeyComparer);
      res.shift()
      done(res)
    })
  };

  this.deleteJob = function (strJob, done) {
    var job = JSON.parse(strJob)
    var blockKey = job.blockKey;
    client.lrem('r'+blockKey, 0, strJob, function (er){
      if (er) throw er;
      done()
    });
  };

  this.deleteBlockKey = function (blockKey, done) {
    //debug('%s is deleted ', 'r'+blockKey)
    client.srem('blockKeys', blockKey, function (err, f){
      if (err) throw err;
      done()
    });
  };



  var blockKeyComparer = function compare(a, b) {
    a = parseInt(a.substr(1))
    b = parseInt(b.substr(1))
    if (a>b) return -1;
    if (a<b) return 1;
    return 0;
  };

  var pullBlockKeys = function (done) {
    if (currentBlockKeys.length) return done(currentBlockKeys);
    that.fetchBlockKeys(function (keys) {
      keys.slice(0,50).forEach(function (k) {
        if(currentBlockKeys.indexOf(k)===-1) currentBlockKeys.push(k)
      });
    })
  };

  var pullJobs = function (done) {
    if (!currentBlockKey) currentBlockKey = currentBlockKeys.shift();
    that.fetchJobs(currentBlockKey, function (jobs) {
      jobs.slice(0,500).forEach(function (k) {
        if(currentJobs.indexOf(k)===-1) currentJobs.push(k)
      });
      done(jobs)
    })
  };


  var doSomeCleanUp = function (allDone) {
    debug('doSomeCleanUp')
    var o = [];
    var blocks = [];
    var cblock = currentBlockKey;
    finishedJobs.splice(0).forEach(function (job) {
      o.push(function (next) {
        deleteJobs(job, next)
      })
      blocks.push(JSON.parse(job).blockKey)
    })
    if (currentBlockKey) blocks.push(currentBlockKey)

    var t = [];
    _.uniq(blocks).forEach(function (b) {
      t.push(function (next) {
        deleteBlockKey(b, next)
      })
    })

    var done = function (){
      debugStat('%s jobsRealized.length = %s +%s',
        cblock, jobsRealized,
        o.length
      );
      jobsRealized+=o.length;
      setTimeout(allDone, 1);
    }
    async.series([
      function(next) {async.parallelLimit(o, 5, next)},
      function(next) {async.parallelLimit(t, 100, next)}
    ], done)
  };

  var deleteJobs = function (strJob, done) {
    var job = JSON.parse(strJob)
    var blockKey = job.blockKey;
    client.lrem('r'+blockKey, 0, strJob, function (er){
      if (er) throw er;
      //finishedJobs = _.without(finishedJobs, strJob);
      //debug('delete jobs %s %s', job.blockKey, job.jobId)
      done()
    });
  };

  var deleteBlockKey = function (blockKey, done) {
    client.exists('r'+blockKey, function (er, ex) {
      if(!ex) {
        //debug('%s is deleted ', 'r'+blockKey)
        client.srem('blockKeys', blockKey, function (err, f){
          if (err) throw err;
          done()
        });
      } else {
        debug('%s exist %s', 'r'+blockKey, ex?'y':'n')
        done()
      }
    });
  };

};
