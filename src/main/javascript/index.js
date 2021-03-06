'use strict';

const Buffer = require('safe-buffer').Buffer;

// [START functions_helloworld_debug]
require('@google-cloud/debug-agent').start();
// [END functions_helloworld_debug]

exports.helloGCS = function (event, callback) {
  const file = event.data;

  if (file.resourceState === 'not_exists') {
    console.log(`File ${file.name} deleted.`);
  } else if (file.metageneration === '1') {
    // metageneration attribute is updated on metadata changes.
    // on create value is 1
    console.log(`File ${file.name} uploaded.`);
  } else {
    console.log(`File ${file.name} metadata updated.`);
  }

  callback();
};