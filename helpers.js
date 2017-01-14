'use strict';

const _ = require('lodash');
const msgpack = require('msgpack-lite');

const helpers = {
    areTasksEquivalent(task1, task2) {
        return task1.type === task2.type && _.isEqual(task1.detail, task2.detail) && _.isEqual(task1.id, task2.id);
    },
    convertBufferToJSON(buffer) {
        return msgpack.decode(buffer);
    },
    convertJSONToBuffer(object) {
        return msgpack.encode(object);
    },
    describeJob(jobID, jobType, jobOrigin) {
        let description = 'job';

        if (jobID) {
            description += ` '${jobID}'`;
        }

        if (jobType) {
            description += ` of type '${jobType}'`;
        }

        if (jobOrigin) {
            description += ` from '${jobOrigin}'`;
        }

        return description;
    }
};

module.exports = helpers;
