'use strict';

const helpers = {
    convertBufferToJSON(buffer) {
        return JSON.parse(buffer.toString());
    },
    convertJSONToBuffer(object) {
        return Buffer.from(JSON.stringify(object));
    }
};

module.exports = helpers;
