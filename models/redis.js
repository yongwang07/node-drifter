var redis = require('redis');
var uuid = require('node-uuid');
var poolModule = require('generic-pool');
var async = require('async');

var pool = poolModule.Pool({
  name: 'redisPool',
  create: function (callback) {
    var client = redis.createClient();
    callback(null, client);
  },
  destroy: function (client) {
    client.quit();
  },
  max: 100,
  min: 5,
  idleTimeoutMillis: 30000,
  log: true
});

function waterfallOperate(type, owner, cb) {
  async.waterfall([
    function (callback) {
      pool.acquire(function (err, client) {
        if (err) return callback(err);
        callback(null, client);
      });
    },

    function (client, callback) {
      client.SELECT(type === 'throw' ? 2 : 3, function (err, res) {
        if (err) return callback(err, client);
        callback(null, client);
      });
    },

    function (client, callback) {
      client.GET(owner, function (err, result) {
        if (err) return callback(err, client);
        if (result >= 10) {
          console.log(type, ',you have pick/throw up:', result);
          return cb({ code: 0, msg: '今天' + (type === 'throw' ? '扔' : '捡') + '瓶子的机会已经用完啦~' });
        }
        client.INCR(owner, function (err) {
          if (err) return callback(err, client);
          callback(null, client);
        });
      });
    },

    function (client, callback) {
      client.TTL(owner, function (err, ttl) {
        if (err) return callback(err, client);
        if (ttl == -1) {
          client.EXPIRE(owner, 86400, function (err) {
            if (err) return callback(err, client);
            callback(null, client, ttl);
          });
        } else callback(null, client, ttl);
      });
    }
  ], function (err, client, ttl) {
    pool.release(client);
    if (err) cb({ code: 0, msg: err });
    else cb({ code: 1, msg: ttl });
  });
}

function checkThrowTimes(owner, cb) {
  waterfallOperate('throw', owner, cb);
}

function checkPickTimes(owner, cb) {
  waterfallOperate('pick', owner, cb);
}

function throwOneBottle(bottle, cb) {
  bottle.time = bottle.time || Date.now();
  var bottleId = uuid.v4();
  var type = { male: 0, female: 1 };
  async.waterfall([
    function(callback) {
      pool.acquire(function (err, client) {
        if (err) return callback(err);
        callback(null, client);
      });
    },
    
    function(client, callback) {
      client.SELECT(type[bottle.type], function (err) {
        if (err) return callback(err, client);
        callback(null, client);
      })
    },
    
    function(client, callback) {
      client.HMSET(bottleId, bottle, function (err, result) {
        if (err) return callback('过会儿再试试吧！');
        else {
          client.PEXPIRE(bottleId, 86400000 + bottle.time - Date.now(), function (err){
            if (err) return callback(err, client);
            callback(null, result, client);
          });
        }
      });
    }
], function(err, result, client) {
  pool.release(client);
  if (err) cb({ code: 0, msg: err });
  cb({ code: 1, msg: result });
});
}

function pickOneBottle(info, cb) {
  var type = { all: Math.round(Math.random()), male: 0, female: 1 };
  info.type = info.type || 'all';
  async.waterfall([
    function (callback) {
      pool.acquire(function (err, client) {
        if (err) return callback(err);
        callback(null, client);
      });
    },

    function (client, callback) {
      client.SELECT(type[info.type], function (err) {
        if (err) return callback(err);
        callback(null, client);
      });
    },

    function (client, callback) {
      client.RANDOMKEY(function (err, bottleId) {
        if (err) return callback(err);
        else if (!bottleId) {
          cb({ code: 1, msg: "海星" });
        } else callback(null, client, bottleId);
      });
    },

    function (client, bottleId, callback) {
      client.HGETALL(bottleId, function (err, bottle) {
        if (err) return callback("漂流瓶破损了...");
        else {
          client.DEL(bottleId, function (err) {
            if (err) return callback(err, client);
            callback(null, client, bottle);
          });
        }
      });
    }
  ], function (err, client, bottle) {
    if (err) cb({ code: 0, msg: err });
    else {
      pool.release(client);
      cb({ code: 1, msg: bottle })
    };
  });
}

exports.throw = function (bottle, callback) {
  checkThrowTimes(bottle.owner, function (result) {
    if (result.code === 0) {
      return callback(result);
    }
    throwOneBottle(bottle, function (result) {
      callback(result);
    });
  });
}

exports.pick = function (info, callback) {
  checkPickTimes(info.user, function (result) {
    if (result.code === 0) {
      return callback(result);
    }
    if (Math.random() <= 0.2) {
      return callback({ code: 1, msg: "海星" });
    }
    pickOneBottle(info, function (result) {
      callback(result);
    });
  });
}