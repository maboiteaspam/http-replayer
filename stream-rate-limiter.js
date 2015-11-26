
module.exports = function (rate, limitedRateFn) {

  var touts = {};
  var pend  = {};
  rate      = rate && parseInt(rate) || 5;

  function fnTransform (){
    return function (chunk, enc, cb) {
      var that = this;
      if(touts && touts.length>=rate) {
        if(!pend) pend = []
        pend.push(function () {
          limitedRateFn(chunk, function () {
            if(pend.length) pend.shift()();
            that.push(chunk)
          });
          cb(null);
        })
      } else {
        limitedRateFn(chunk, function () {
          if(pend && pend.length)
            pend.shift()();
          that.push(chunk)
        });
        cb(null);
      }
    };
  }

  function fnFlush (){
    return function (cb) {
      var waitForThemToFinish = function () {
        if(!touts) cb();
        else if(!touts.length) cb()
        else setTimeout(waitForThemToFinish, 10);
      };
      waitForThemToFinish();
    };
  }


  return through2.obj(fnTransform, fnFlush);
}