'use strict';

const _ = require('lodash');
const amqp = require('amqplib');
const asyncClass = require('async-class');
const debug = require('debug')('remote-work-queue:client');
const moment = require('moment');
const uuid = require('uuid');

const helpers = require('./helpers');

const DEFAULT_PRIORITY = 15;
const MAX_PRIORITY = 255;

class RemoteWorkQueueClient {
    constructor({
        amqpURL = 'amqp://localhost',
        defaultPriority = DEFAULT_PRIORITY,
        jobQueue = 'remote-work-jobs'
    } = {}) {
        this.connectionOptions = {
            amqpURL,
            jobQueue
        };
        this.defaultPriority = defaultPriority;

        this.connection = null;
        this.channel = null;

        this.jobs = new Map();

        this.active = false;
        this.initializing = false;
        this.closing = false;
    }

    * initialize() {
        if (this.active || this.initializing || this.closing) {
            return;
        }

        this.initializing = true;

        this.jobs.clear();

        this.connection = yield amqp.connect(this.connectionOptions.connect);
        this.channel = yield this.connection.createChannel();

        yield this.channel.assertQueue(this.connectionOptions.jobQueue, {
            maxPriority: MAX_PRIORITY
        });

        let jobResultQueue = yield this.channel.assertQueue('', {
            exclusive: true,
            autoDelete: true
        });
        this.jobResultQueue = jobResultQueue.queue;

        yield this.channel.prefetch(1);
        yield this.channel.consume(this.jobResultQueue, _.bind(this.receiveTaskResult, this));

        this.initializing = false;
        this.active = true;
    }

    * shutdown(immediate = false) {
        if (!this.active || this.initializing || this.closing) {
            return;
        }

        this.closing = true;

        if (immediate) {
            // reject all active jobs
            for (let job of this.jobs.values()) {
                job.reject(new Error('client is shutting down'))
            }

            yield this.finishShutdown();
        }
        else {
            // all active jobs will be completed regularly before shutdown finishes

            if (this.jobs.size === 0) {
                // no jobs actually remaining, just shut down

                yield this.finishShutdown();
            }
        }
    }

    * finishShutdown() {
        // clear lists
        this.jobs.clear();

        // close the channel and connection
        yield this.channel.close();
        yield this.connection.close();

        this.closing = false;
        this.active = false;
    }

    * queueJob(tasks, {
        priority = this.defaultPriority,
        unique = false,
        retries = 0,
        timestamp = Date.now(),
        id,
        type,
        origin,
        waitForJobCompletion = true
    } = {}) {
        priority = _.clamp(_.toInteger(priority), 0, 255);
        unique = !!unique;
        retries = _.clamp(_.toInteger(retries));
        timestamp = moment(timestamp);

        if (!this.active) {
            throw new Error('cannot queue jobs when uninitialized');
        }

        if (this.closing) {
            throw new Error('cannot queue new jobs while closing');
        }

        if (!_.every(tasks, task => _.isString(task.type))) {
            throw new Error('one or more tasks are in invalid format');
        }

        if (waitForJobCompletion) {
            // create and associate job with an ID and promise for later usage
            let jobID = uuid.v4();
            let job = {};
            let promise = new Promise(function(resolve, reject) {
                job.resolve = resolve;
                job.reject = reject;
            });
            this.jobs.set(jobID, job);

            // send job to queue
            yield this.channel.sendToQueue(this.connectionOptions.jobQueue, helpers.convertJSONToBuffer({
                tasks,
                unique,
                retries
            }), {
                priority,
                persistent: true,
                correlationId: jobID,
                replyTo: this.jobReturnQueue,
                messageId: id,
                timestamp: timestamp.unix(),
                type,
                appId: origin
            });

            // wait for job finish and return any results
            let result = yield promise;
            return result;
        }
        else {
            // just send the job without waiting for any results
            yield this.channel.sendToQueue(this.connectionOptions.jobQueue, helpers.convertJSONToBuffer({
                tasks,
                unique,
                retries
            }), {
                priority,
                persistent: true,
                messageId: id,
                timestamp: timestamp.unix(),
                type,
                appId: origin
            });
        }
    }

    * receiveJobResult(msg) {
        if (!this.active) {
            return;
        }

        let result = helpers.convertBufferToJSON(msg.content);

        yield this.processJobResult(_.assign({}, result, {
            priority: msg.properties.priority,
            returnID: msg.properties.correlationId,
            jobID: msg.properties.messageId,
            timestamp: msg.properties.timestamp,
            jobType: msg.properties.type,
            jobOrigin: msg.properties.appId,
            originalMessage: msg
        }));
    }

    * processJobResult({
        success = false,
        results = [],
        error = new Error(),
        returnID,
        jobID,
        jobType,
        jobOrigin,
        originalMessage
    } = {}) {
        if (jobID || jobType || jobOrigin) {
            debug(`received results for ${helpers.describeJob(jobID, jobType, jobOrigin)}`);
        }

        if (this.jobs.has(returnID)) {
            let job = this.jobs.get(returnID);

            if (success) {
                job.resolve(results);
            }
            else {
                job.reject(error);
            }

            this.jobs.delete(returnID);
        }

        if (originalMessage && this.active) {
            yield this.channel.ack(originalMessage);
        }

        if (this.closing && this.jobs.size === 0) {
            yield this.finishShutdown();
        }
    }
}

module.exports = asyncClass.wrap(RemoteWorkQueueClient);
