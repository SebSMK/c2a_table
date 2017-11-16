var mongoclient = require('mongodb').MongoClient, 
  Q = require('q'),    
  logger = require('c2a_utils').logging;

mongo = (function() {    
    
    var db_inst, path;
    
    /**
     * Constructor
     **/
    function mongo(path) {
      this.path = path;
    }
    
    /**
     * Instance Methods 
     **/  
    
    /**
     * connect
     **/            
    mongo.prototype.connect = function() {
        var deferred = Q.defer();
        var self = this;
        
        mongoclient.connect(self.path)
        .then(function(db) {
            logger.info("mongo - connected: " + self.path);
            self.db_inst = db;
            deferred.resolve(db)
        }) 
        .catch(function(err) {
          /*catch and break on all errors or exceptions on all the above methods*/
          logger.error('mongo.prototype.connect', err);
          deferred.reject(err);
        }); 
      
      return deferred.promise;
    }
    
    /**
     * create
     **/  
    mongo.prototype.create = function(collection, data) {
        var self = this; 
        if (self.db_inst == undefined)
          throw ("No MongoDB instance");
        
        var deferred = Q.defer();
        
        try{
          var db_collection = self.db_inst.collection(collection); 
          
          db_collection.insert(data, {w:1})
          .then(function(){
            logger.info("mongo - inserted: " + JSON.stringify(data));
            deferred.resolve(data);
          });
                  
        }
        catch(err) {
          /*catch and break on all errors or exceptions on all the above methods*/
          logger.error('mongo.prototype.insert', err);
          deferred.reject(err);
        } 
                                               
      return deferred.promise;
    } 
    
    /**
     * update
     **/  
    mongo.prototype.update = function(collection, key, value) {
        var self = this; 
        if (self.db_inst == undefined)
          throw ("No MongoDB instance");
        
        var deferred = Q.defer();
        
        try{
          var db_collection = self.db_inst.collection(collection);
          //var modif = db_collection.update(key, value, {w:1});
          
          db_collection.update(key, value, {w:1})
          .then(function(number){
            logger.info("mongo - updated: " + JSON.stringify(JSON.stringify(key)));
            deferred.resolve(number);
          })          
        }
        catch(err) {
          /*catch and break on all errors or exceptions on all the above methods*/
          logger.error('mongo.prototype.update', err);
          deferred.reject(err);
        } 
                                               
      return deferred.promise;
    }
    
    /**
     * retrieve
     **/  
    mongo.prototype.retrieve = function(collection, req) {
        var self = this; 
        if (self.db_inst == undefined)
          throw ("No MongoDB instance");
        
        var deferred = Q.defer();
        
        try{
          var items = [];
          var db_collection = self.db_inst.collection(collection);
          var stream = db_collection.find(req).stream();
          stream.on("data", function(item) {items.push(item)});
          stream.on("end", function() {
              logger.info("mongo - found: " + JSON.stringify(items));
              deferred.resolve(items);
          });           
        }
        catch(err) {
          /*catch and break on all errors or exceptions on all the above methods*/
          logger.error('mongo.prototype.retrieve', err);
          deferred.reject(err);
        } 
                                               
      return deferred.promise;
    }
    
    /**
     * delete
     **/ 
    mongo.prototype.delete = function(collection, req) {
        var self = this; 
        if (self.db_inst == undefined)
          throw ("No MongoDB instance");
        
        var deferred = Q.defer();
        
        try{
          var items = [];
          var db_collection = self.db_inst.collection(collection);
          db_collection.remove(req, {w:1})
          .then(function(number){
            logger.info("mongo - removed: " + JSON.stringify(JSON.stringify(req)));
            deferred.resolve(number);
          })   
        }
        catch(err) {
          /*catch and break on all errors or exceptions on all the above methods*/
          logger.error('mongo.prototype.delete', err);
          deferred.reject(err);
        } 
                                               
      return deferred.promise;
    }

    return mongo;
})();

module.exports = mongo;
