/**
 * Created by toby on 16/11/15.
 */

(function(exports) {
  "use strict";

  var request = require("request");
  var fs = require("fs");


  /*
   * Helper to download a file from a URL to a local file.
   */
  exports.downloadFile = (function() {
    "use strict";

    var log = require("debug")("downloadFile");

    function download(url, target, encoding, cb) {
      // Encoding defaults to utf8.
      if (typeof encoding === "function") {
        cb = encoding;
        encoding = "utf8";
      }
      request(url, function (err, response, content) {
        if (err || response.statusCode !== 200) {
          err = err || new Error("not found");
          log("failure downloading from %s [%s]", url, err.message);
          cb(err);
        } else {
          fs.writeFile(target, content, {encoding: encoding}, cb);
        }
      });
    }

    return download;
  }());

}(module.exports));

