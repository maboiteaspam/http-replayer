
module.exports = function () {

  var jobs = {};
  var pend  = {};

  function fnTransform (s){
    return function (chunk, enc, cb) {
      var that = this;

    };
  }

  function fnFlush (s){
    return function (cb) {
      var waitForThemToFinish = function () {
        if(!touts[s]) cb();
        else if(!touts[s].length) cb()
        else setTimeout(waitForThemToFinish, 10);
      };
      waitForThemToFinish();
    };
  }


  return through2.obj(fnTransform, fnFlush);
}