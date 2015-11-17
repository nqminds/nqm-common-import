/**
 * Created by toby on 16/11/15.
 */

(function(exports) {
  "use strict";

  var util = require("util");
  var request = require("request");
  var fs = require("fs");
  var _ = require("lodash");
  var parser = require("mongo-parse");

  exports.getAccessToken = (function() {
    var log = require("debug")("getAccessToken");

    function getToken(commandHost, credentials, cb) {
      var url = util.format("%s/token", commandHost);
      request({ url: url, method: "post", headers: { "authorization": "Basic " + credentials }, json: true, body: { grant_type: "client_credentials" } }, function(err, response, content) {
        if (err || response.statusCode !== 200) {
          err = err || new Error(response.body.error || "not found");
          log("failure [%s]", err.message);
          cb(err);
        } else {
          log("result from server: %j", response.body);
          cb(null, response.body.access_token);
        }
      });
    }

    return getToken;
  }());

  exports.createTargetDataset = (function() {
    var log = require("debug")("createTargetDataset");

    var buildSchema = function(schema, inp) {
      _.forEach(inp, function(v,k) {
        if (Array.isArray(v)) {
          schema[k] = v;
        } else if (typeof v === "object") {
          schema[k] = {};
          buildSchema(schema[k], v);
        } else if (typeof v === "string" && k !== "type") {
          schema[k] = { type: v };
        } else {
          schema[k] = v;
        }
      });
      return schema;
    };

    function createTargetDataset(commandHost, accessToken, name, schema, primaryKey, cb) {
      var url = util.format("%s/commandSync/dataset/create", commandHost);
      var data = {};
      data.name = name;
      data.uniqueIndex = _.map(primaryKey, function(key) { return { asc: key }} );
      data.schema = {};
      buildSchema(data.schema, schema);
      _.forEach(primaryKey, function(key) {
        var pointer = parser.DotNotationPointers(data.schema, key)[0];
        pointer.val.index = true;
        pointer.val.unique = true;
      });
      log("sending create dataset [%j]",data);
      request({ url: url, method: "post", headers: { authorization: "Bearer " + accessToken }, json: true, body: data }, function(err, response, content) {
        if (err || response.statusCode !== 200 || response.body.error) {
          err = err || new Error(response.body.error || "not found");
          log("failure upserting dataset data [%s]", err.message);
          cb(err);
        } else {
          log("result from server: %j", response.body);
          cb(null, response.body.response.id);
        }
      });
    }

    return createTargetDataset;
  }());

  /*
   * Helper to get a dataset using the nqmHub query api.
   */
  exports.getDataset = (function() {
    var log = require("debug")("getDataset");

    function getDS(queryHost, datasetId, cb) {
      var url = util.format("%s/datasets/%s", queryHost, datasetId);
      request({ method: "get", url: url, json: true }, function(err, response, content) {
        if (err || response.statusCode !== 200) {
          err = err || new Error("not found");
          log("failure getting dataset %s [%s]",datasetId, err.message);
          cb(err);
        } else {
          cb(null, content);
        }
      });
    }

    return getDS;
  }());

  /*
   * Helper to add data to a dataset using the nqmHub command api.
   */
  exports.addDatasetDataBulk = (function() {
    var log = require("debug")("addDatasetDataBulk");

    function addDataBulk(commandHost, accessToken, datasetId, data, cb) {
      var url = util.format("%s/commandSync/dataset/data/createMany", commandHost);
      var bulk = {};
      bulk.datasetId = datasetId;
      bulk.payload = data;
      log("sending createMany [%d]",data.length);
      request({ url: url, method: "post",  headers: { authorization: "Bearer " + accessToken }, json: true, body: bulk }, function(err, response, content) {
        if (err || response.statusCode !== 200 || response.body.error) {
          err = err || new Error(response.body.error || "not found");
          log("failure adding dataset data [%s]", err.message);
          cb(err);
        } else {
          log("result from server: %j", response.body);
          cb(null);
        }
      });
    }

    return addDataBulk;
  }());

  /*
   * Helper to upsert data to a dataset using the nqmHub command api.
   */
  exports.upsertDatasetDataBulk = (function() {
    var log = require("debug")("upsertDatasetDataBulk");

    function upsertDataBulk(commandHost, accessToken, datasetId, data, cb) {
      var url = util.format("%s/commandSync/dataset/data/upsertMany", commandHost);
      var bulk = {};
      bulk.datasetId = datasetId;
      bulk.payload = data;
      log("sending upsertMany [%d]",data.length);
      request({ url: url, method: "post", headers: { authorization: "Bearer " + accessToken }, json: true, body: bulk }, function(err, response, content) {
        if (err || response.statusCode !== 200 || response.body.error) {
          err = err || new Error(response.body.error || "not found");
          log("failure upserting dataset data [%s]", err.message);
          cb(err);
        } else {
          log("result from server: %j", response.body);
          cb(null);
        }
      });
    }

    return upsertDataBulk;
  }());

}(module.exports));

