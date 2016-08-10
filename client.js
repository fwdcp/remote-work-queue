'use strict';

const _ = require('lodash');
const amqp = require('amqplib');
const asyncClass = require('async-class');
const crypto = require('crypto');
const moment = require('moment');

const helpers = require('./helpers');

class RemoteWorkQueueClient {
    constructor({
        connect = 'amqp://localhost',
        commandQueue = 'remote-work-commands',
        resultExchange = 'remote-work-results',
        concurrentJobs = 1,
        defaultPriority = 10
    } = {}) {
        this.connectionOptions = {
            connect,
            commandQueue,
            resultExchange
        };
        this.concurrentJobs = concurrentJobs;
        this.defaultPriority = defaultPriority;

        this.active = false;
        this.connection = null;
        this.channel = null;
        this.queue = [];
        this.currentJobs = [];
        this.closing = false;
        this.reportCloseComplete = _.noop;
    }

    *
    initialize() {
        if (this.active) {
            return;
        }

        this.connection = yield amqp.connect(this.connectionOptions.connect);
        this.channel = yield this.connection.createChannel();

        yield this.channel.assertQueue(this.connectionOptions.commandQueue);

        yield this.channel.assertExchange(this.connectionOptions.resultExchange, 'fanout');
        let responseQueue = yield this.channel.assertQueue('', {
            exclusive: true
        });
        yield this.channel.bindQueue(responseQueue.queue, this.connectionOptions.resultExchange, '');

        yield this.channel.consume(responseQueue.queue, this.processResponse.bind(this));

        this.active = true;
        this.closing = false;
        this.reportCloseComplete = _.noop;
    }

    *
    close(immediate = false) {
        function terminateJob(job) {
            job.failed = true;
            job.failure = new Error('remote work queue is shutting down');
        }

        if (!this.active || this.closing) {
            return;
        }

        if (immediate) {
            this.active = false;

            _.forEach(this.queue, terminateJob);
            _.forEach(this.currentJobs, terminateJob);

            this.maintainJobs();

            yield this.channel.close();
            yield this.connection.close();
        }
        else {
            this.closing = true;

            yield new Promise(_.bind(function(resolve) {
                this.reportCloseComplete = resolve;
            }, this));

            yield this.channel.close();
            yield this.connection.close();

            this.active = false;
            this.closing = false;
            this.reportCloseComplete = _.noop;
        }
    }

    queueJob(tasks, {
        maxRuntime = null,
        maxWait = null,
        priority = this.defaultPriority,
        unique = false
    } = {}) {
        return new Promise(_.bind(function(resolve, reject) {
            if (!this.active) {
                reject(new Error('remote work queue is inactive'));
                return;
            }

            if (this.closing) {
                reject(new Error('remote work queue is shutting down'));
                return;
            }

            let newJob = {
                priority,
                requested: moment().valueOf(),
                tasks: unique ? _.map(tasks, task => _.defaults({
                    _remoteWorkQueueId: crypto.randomBytes(8).toString()
                }, task)) : _.uniqWith(tasks, _.isEqual),
                completed: _.fill([], false, 0, _.size(tasks)),
                results: _.fill([], null, 0, _.size(tasks)),
                failed: false,
                failure: null,
                maxRuntime,
                maxWait,
                runtimeTimeout: null,
                waitTimeout: null,
                reportSuccess: resolve,
                reportFailure: reject
            };

            let queueIndex = _.findLastIndex(this.queue, job => (job.priority >= newJob.priority)) + 1;
            this.queue.splice(queueIndex, 0, newJob);

            if (newJob.maxWait) {
                newJob.waitTimeout = setTimeout(_.bind(function(job) {
                    if (!job.failed) {
                        job.failed = true;
                        job.failure = new Error('task waited too long');
                    }

                    this.maintainJobs();
                }, this, newJob), newJob.maxWait);
            }

            this.dispatchJobs();
        }, this));
    }

    *
    dispatchJobs() {
        while (_.size(this.currentJobs) < this.concurrentJobs && _.size(this.queue) > 0) {
            let nextJob = this.queue.shift();
            this.currentJobs.push(nextJob);

            try {
                for (let task of nextJob.tasks) {
                    yield this.channel.sendToQueue(this.connectionOptions.commandQueue, helpers.convertJSONToBuffer(task), {
                        persistent: true
                    });
                }
            }
            catch (err) {
                nextJob.failed = true;
                nextJob.failure = err;
            }

            if (nextJob.maxRuntime) {
                nextJob.runtimeTimeout = setTimeout(_.bind(function(job) {
                    if (!job.failed) {
                        job.failed = true;
                        job.failure = new Error('task timed out');
                    }

                    this.maintainJobs();
                }, this, nextJob), nextJob.maxRuntime);
            }
        }

        this.maintainJobs();
    }

    maintainJobs() {
        function checkIfJobComplete(job) {
            if (job.failed || !_.includes(job.completed, false)) {
                if (job.runtimeTimeout) {
                    clearTimeout(job.runtimeTimeout);
                    job.runtimeTimeout = null;
                }

                if (job.waitTimeout) {
                    clearTimeout(job.waitTimeout);
                    job.waitTimeout = null;
                }

                if (!_.includes(job.completed, false)) {
                    job.reportSuccess(job.results);
                }

                if (job.failed) {
                    job.reportFailure(job.failure);
                }

                return true;
            }

            return false;
        }

        this.currentJobs = _.reject(this.currentJobs, checkIfJobComplete);
        this.queue = _.reject(this.queue, checkIfJobComplete);

        this.dispatchJobs();

        if (this.closing) {
            if (_.size(this.currentJobs) === 0 && _.size(this.queue) === 0) {
                this.reportCloseComplete();
            }
        }
    }

    *
    processResponse(msg) {
        if (!this.active) {
            yield this.channel.ack(msg);
            return;
        }

        let response = helpers.convertBufferToJSON(msg.contents);

        function applyTaskResultToJob(job, {
            ignoreFailure = false
        } = {}) {
            if (response.start > job.requested) {
                let taskIndex = _.findIndex(job.tasks, task => _.isEqual(task, response.task));

                if (taskIndex !== -1) {
                    if (response.success) {
                        job.completed[taskIndex] = true;
                        job.results[taskIndex] = response.result;
                    }
                    else if (!ignoreFailure) {
                        if (!job.failed) {
                            job.failed = true;
                            job.failure = new Error(response.error);
                        }
                    }
                }
            }
        }

        _.forEach(this.currentJobs, applyTaskResultToJob);

        // NOTE: ignore task failures for unqueued jobs
        _.forEach(this.queue, _.partial(applyTaskResultToJob, _, {
            ignoreFailure: true
        }));

        yield this.channel.ack(msg);

        this.maintainJobs();
    }
}

module.exports = asyncClass.wrap(RemoteWorkQueueClient);
