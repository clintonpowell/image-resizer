/*
* Written by Clinton Powell
* 04/24/2014
*/

var Persist = module.exports = function(config, dbconn) {
  if(!(this instanceof Persist)) {
    return new Persist(config, dbconn);
  }
  _config = config || {};
  _dbconn = dbconn;
}
, _dbconn = null
, _config = { }
, _db = require('mongoskin').db('mongodb://hippoweb:cVPG9vf45BcAG7FQ@localhost:27017/hippomsg')
, _q = require('Q')
, _store = _db.collection('images')
, _interval = _config.interval || 200
, _waitTimeout = _config.waitTimeout || 10 * _interval;
Persist.prototype = {
  set: function(key, value) {
    var deferred = _q.defer();
    _store.update({key: key}, {$set: {value: value}}, {upsert: true}, function(err, result) {
      if (err) deferred.reject(err);
      else deferred.resolve(result);
    });

    return deferred.promise;
  }
  , setnx: function(key, value) {
    var deferred = _q.defer();
    _store.findOne({key: key}, function(err, result){
      if(err) deferred.reject('Error setting key');
      else if(result) deferred.resolve(true);
      else {
        _db.collection('images').insert({key: key, value: value}, function(err, result) {
          if(err) deferred.reject('Error setting key');
          else deferred.resolve(result[0]);
          console.log("HERE2");
        });
      }
    });
    return deferred.promise;
  }
  , get: function(key) {
    var deferred = _q.defer();

    _store.findOne({key: key}, function(err, result) {
      if (err) deferred.reject(err);
      else if(result) deferred.resolve(result.value);
      else deferred.resolve(null);
    });

    return deferred.promise;
  }
  , del: function(keys) {
    var deferred = _q.defer();

    if (typeof keys === 'string')
      keys = [keys];
    _store.remove({key: {$in: keys}}, function(err, result) {
      if (err) deferred.reject(err);
      else deferred.resolve(result);
    });

    return deferred.promise;
  }
  , setLock: function(key) {
    var timeout;
    timeout = Date.now() + _waitTimeout;
    return this.setnx(key, timeout);
  }
  , lock: function(key) {
    var deferred = _q.defer()
    , instance = this;

    // query for the lock key
    instance.get(key).catch(function(err) {
      deferred.reject(err);
    }).done(function(result) {
      var expiration, lockTimer;
      // assuming we get a response
      if (result !== null) {
        // reject if the lock key has expired
        if (parseInt(result, 10) < Date.now()) {
          deferred.reject('lock expired');
        } else {
          // set a timeout expiration
          expiration = Date.now() + _waitTimeout;
          // construct a loop to poll for the lock key at the set interval
          lockTimer = setInterval(function() {
            instance.get(key).catch(function() {
              clearInterval(lockTimer);
              deferred.reject('lock error');
            }).done(function(_result) {
              // reject if the lock is gone
              if (!_result) {
                clearInterval(lockTimer);
                deferred.resolve('lock absent');
              } else {
                // reject if the lock has expired
                if (Date.now() > expiration) {
                  clearInterval(lockTimer);
                  deferred.reject('lock timeout');
                }

                // otherwise proceed again...
              }
            });
          }, _interval);
        }

      // reject if the lock key is not found
      } else {
        deferred.reject('no lock');
      }
    });

    return deferred.promise;
  }
  , ping: function() {
    var deferred = _q.defer();
    setTimeout(function() {
      deferred.resolve();
    }, 100);
    return deferred.promise;
  }
};