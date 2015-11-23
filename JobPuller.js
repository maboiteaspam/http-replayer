
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
  var updateInterval;
  var jobsRealized = 0;
  var jobQueueLength = 50;

  this.start = function (opts, len) {
    if (client) return client
      client = redis.createClient(
        opts.rport || 6379,
        opts.rhost || '0.0.0.0',
        opts.ropts && JSON.parse(opts.ropts) || {

        });
    jobQueueLength = len && parseInt(len) || jobQueueLength;

    client.on('ready', function (){
      debug('client ready')
      updateCurrentJobs()
    })
    return client
  };
  this.end = function () {
    client.end();
    clearInterval(updateInterval);
  };

  this.jobProcess = function (chunk, callback) {
    callback(null, chunk)
  };

  var currentBlockKey = false
  var currentBlockKeys = []
  var currentJobs = []
  this.getJobs = function () {
    return _.difference(currentJobs, finishedJobs)
  };
  var finishedJobs = [];
  this.finishJob = function (job) {
    finishedJobs.push(job)
    currentJobs = _.without(currentJobs, job);
  };
  this.erroredJob = function (job) {
    currentJobs = _.without(currentJobs, job);
  };

  var isBusy = false;
  var noMoreBusy = function(){
    isBusy=false;
    setTimeout(updateCurrentJobs, currentBlockKeys.length===0?1500:1)
  };
  var updateCurrentJobs = function () {
    if(isBusy) return;

    if (currentBlockKey===false) {
      isBusy= true;
      pullBlockKeys(function(){
        updateCurrentBlockKey(noMoreBusy)
      })

    } else if (currentJobs.length) {
      processJobs(noMoreBusy)

    } else if (currentJobs.length===0) {
      debug('pullJobs')
      isBusy = true;
      pullJobs(function () {
        doSomeCleanUp(function (){
          updateCurrentBlockKey(noMoreBusy)
        })
      })

    }else {
      debug('weird')
    }
  };

  var processJobs = function (done) {
    var y = []
    that.getJobs().forEach(function(job) {
      y.push(function (next) {
        that.jobProcess(job, next)
      })
    });
    debugStat('processJobs = %s jobs.length = %s',
      currentBlockKey,
      y.length);
    async.parallelLimit(y, jobQueueLength, done);
  };


  var lastKey = null;
  var updateCurrentBlockKey = function (done) {
    if (currentBlockKeys.length>0) {
      currentBlockKey = currentBlockKeys.shift();
      lastKey = currentBlockKey;
      debugStat('currentBlockKey = %s blockKeys.length = %s',
        currentBlockKey,
        currentBlockKeys.length);
    } else {
      currentBlockKey = false
    }
    done()
  };

  var blockKeyComparer = function compare(a, b) {
    a = parseInt(a.substr(1))
    b = parseInt(b.substr(1))
    if (a>b) return -1;
    if (a<b) return 1;
    return 0;
  };
  var pullBlockKeys = function (done) {
    if (currentBlockKeys.length) return done();
    debug('pullBlockKeys')
    client.smembers('blockKeys', function (err, res) {
      if (res.length>1) {
        res.sort(blockKeyComparer);
        //res.reverse()
        res.shift()
        currentBlockKeys = res
      } else {
        currentBlockKeys=[]
      }
      done()
    })
  };

  var pullJobs = function (done) {
    if (!currentJobs.length && currentBlockKey!==false) {
      client.lrange('r'+currentBlockKey, 0, 500, function (err, res) {
        debugStat('currentBlockKey = r%s currentBlockKeys.length = %s jobs.length = %s',
          currentBlockKey,
          currentBlockKeys.length,
          res.length);
        if(res.length) {
          var t = currentJobs.splice(0).concat(res);
          currentJobs = _.difference(t, finishedJobs);
        }
        done()
      });
    } else {
      done()
    }
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
