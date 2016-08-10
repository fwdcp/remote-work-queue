'use strict';

const _ = require('lodash');
const amqp = require('amqplib');
const asyncClass = require('async-class');
const moment = require('moment');

const helpers = require('./helpers');

class RemoteWorkQueueClient {
    constructor({
        connect = 'amqp://localhost',
        commandQueue = 'remote-work-commands',
        resultExchange = 'remote-work-results',
        concurrentJobs = 1
    } = {}) {
        this.connectionOptions = {
            connect,
            commandQueue,
            resultExchange
        };
        this.concurrentJobs = concurrentJobs;

        this.channel = null;
        this.queue = [];
        this.currentJobs = [];
    }

    *
    initialize() {
        if (!this.channel) {
            let connection = yield amqp.connect(this.connectionOptions.connect);
            this.channel = yield connection.createChannel();

            yield this.channel.assertQueue(this.connectionOptions.commandQueue);

            yield this.channel.assertExchange(this.connectionOptions.resultExchange, 'fanout');
            let responseQueue = yield this.channel.assertQueue('', {
                exclusive: true
            });
            yield this.channel.bindQueue(responseQueue.queue, this.connectionOptions.resultExchange, '');

            yield this.channel.consume(responseQueue.queue, this.processResponse.bind(this));
        }
    }

    queueJob(tasks, {
        maxRuntime = null,
        maxWait = null
    } = {}) {
        return new Promise(function(resolve, reject) {
            let newJob = {
                requested: moment().valueOf(),
                tasks: _.uniqWith(tasks, _.isEqual),
                completed: _.fill([], false, 0, _.size(tasks)),
                results: _.fill([], null, 0, _.size(tasks)),
                maxRuntime,
                maxWait,
                runtimeTimeout: null,
                waitTimeout: null,
                reportSuccess: resolve,
                reportFailure: reject
            };

            this.queue.push(newJob);

            if (newJob.maxWait) {
                setTimeout(_.bind(function() {
                    this.reportFailure(new Error('task waited too long'));

                    if (this.runtimeTimeout) {
                        clearTimeout(this.runtimeTimeout);
                        this.runtimeTimeout = null;
                    }

                    if (this.waitTimeout) {
                        clearTimeout(this.waitTimeout);
                        this.runtimeTimeout = null;
                    }
                }, newJob), newJob.maxWait);
            }

            this.dispatchJobs();
        });
    }

    *
    dispatchJobs() {
        while (_.size(this.currentJobs) < this.concurrentJobs && _.size(this.queue) > 0) {
            let nextJob = this.queue.shift();
            this.currentJobs.push(nextJob);

            for (let task of nextJob.tasks) {
                yield this.channel.sendToQueue(this.connectionOptions.commandQueue, helpers.convertJSONToBuffer(task), {
                    persistent: true
                });
            }

            if (nextJob.maxRuntime) {
                setTimeout(_.bind(function() {
                    this.reportFailure(new Error('task timed out'));

                    if (this.runtimeTimeout) {
                        clearTimeout(this.runtimeTimeout);
                        this.runtimeTimeout = null;
                    }

                    if (this.waitTimeout) {
                        clearTimeout(this.waitTimeout);
                        this.waitTimeout = null;
                    }
                }, nextJob), nextJob.maxRuntime);
            }
        }
    }

    processResponse(msg) {
        let response = helpers.convertBufferToJSON(msg.contents);

        function applyTaskResultToJob(job, {
            ignoreFailure = false
        } = {}) {
            let jobFinished = false;
            let jobSuccessful;
            let jobReturn;

            if (response.start > job.requested) {
                let taskIndex = _.findIndex(job.tasks, task => _.isEqual(task, response.task));

                if (taskIndex !== -1) {
                    if (response.success) {
                        job.completed[taskIndex] = true;
                        job.results[taskIndex] = response.result;

                        if (!_.includes(job.completed, false)) {
                            jobFinished = true;
                            jobSuccessful = true;
                            jobReturn = job.results;
                        }
                    }
                    else if (!ignoreFailure) {
                        if (!job.completed[taskIndex]) {
                            jobFinished = true;
                            jobSuccessful = false;
                            jobReturn = new Error(response.error);
                        }
                    }
                }
            }

            if (jobFinished) {
                if (job.runtimeTimeout) {
                    clearTimeout(job.runtimeTimeout);
                    job.runtimeTimeout = null;
                }

                if (job.waitTimeout) {
                    clearTimeout(job.waitTimeout);
                    job.waitTimeout = null;
                }

                if (jobSuccessful) {
                    job.reportSuccess(jobReturn);
                }
                else {
                    job.reportFailure(jobReturn);
                }
            }

            return jobFinished;
        }

        this.currentJobs = _.reject(this.currentJobs, applyTaskResultToJob);

        // NOTE: ignore task failures for unqueued jobs
        this.queue = _.reject(this.queue, _.partial(applyTaskResultToJob, _, {
            ignoreFailure: true
        }));

        this.dispatchJobs();
    }
}

module.exports = asyncClass.wrap(RemoteWorkQueueClient);
