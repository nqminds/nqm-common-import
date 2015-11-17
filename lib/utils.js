/**
 * Created by toby on 16/11/15.
 */

(function(exports) {
  "use strict";

  var _ = require("lodash");

  /*
   * Provides an opportunity to clean/transform data before it is added to the dataset.
   *
   * ToDo - expand this to cover common data mismatches.
   */
  exports.sanitiseData = function(type, data) {
    var transformed;

    if (typeof type === "string" && type.toLowerCase() === "number" && typeof data === "string") {
      // Remove any commas from numeric string values (e.g. 4,300,000)
      transformed = parseFloat(data.replace(/,/g,''));
    } else {
      transformed = data;
    }
    return transformed;
  };

  /*
   * Sets a property value in the target object, where the property
   * can be arbitrarily deep, e.g. obj.level1.level2.value
   */
  exports.setData = function(obj, targetProperty, value, isArray) {
    var components = targetProperty.split('.');
    var parent = null;
    var property;
    for (var i = 0, len = components.length-1; i < len; i++) {
      property = components[i];
      if (!obj.hasOwnProperty(property)) {
        obj[property] = {};
      }
      parent = obj;
      obj = obj[property];
    }
    if (isArray) {
      if (!obj.hasOwnProperty(components[i])) {
        obj[components[i]] = [];
      }
      obj[components[i]].push(value);
    } else {
      obj[components[i]] = value;
    }
  };

  /*
   * Flattens a mongoose schema definition
   */
  exports.flattenSchema = function(data) {
    var result = {};
    function recurse (current, prop) {
      if (Object(current) !== current) {
        result[prop] = current;
      } else if (Array.isArray(current)) {
        for(var i=0, l=current.length; i<l; i++) {
          recurse(current[i], prop + "[" + i + "]");
        }
        if (l === 0) {
          result[prop] = [];
        }
      } else {
        var isEmpty = true;
        for (var p in current) {
          if (!current[p].type || typeof current[p].type !== "string") {
            isEmpty = false;
            recurse(current[p], prop ? prop + "." + p : p);
          } else {
            isEmpty = false;
            result[prop ? prop + "." + p : p] = current[p].type;
          }
        }
        if (isEmpty && prop) {
          result[prop] = {};
        }
      }
    }
    recurse(data, "");
    return result;
  };

  /*
   * Creates a hash map of the dataset schema fields - for fast lookup.
   */
  exports.createFieldMap = function(schema) {
    // Flatten the dataset schema.
    var flattened = exports.flattenSchema(schema);
    // Create a lookup for all possible target fields, and determine
    // if the target is an array.
    var fieldMap = {};
    _.forEach(flattened, function(v,k) {
      fieldMap[k] = {
        name: k,
        type: v.type ? v.type : v
      };
    });

    return fieldMap;
  };

}(module.exports));

