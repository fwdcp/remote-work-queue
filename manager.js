'use strict';

const _ = require('lodash');
const amqp = require('amqplib');
const asyncClass = require('async-class');
const debug = require('debug')('remote-work-queue:manager');
const moment = require('moment');
const promiseDebouncer = require('promise-debouncer');
const uuid = require('uuid');

const helpers = require('./helpers');

const DEBOUNCE_INTERVAL = 100;
const DEBOUNCE_MAX_WAIT = 1000;
const DEFAULT_PRIORITY = 15;
const MAX_PRIORITY = 255;

class RemoteWorkQueueManager {
    constructor({
        amqpURL = 'amqp://localhost',
        defaultPriority = DEFAULT_PRIORITY,
        jobQueue = 'remote-work-jobs',
        maxConcurrentJobs = 100,
        taskQueue = 'remote-work-tasks',
        taskResultExchange = 'remote-work-task-results'
    } = {}) {
        this.connectionOptions = {
            amqpURL,
            jobQueue,
            taskQueue,
            taskResultExchange
        };
        this.defaultPriority = defaultPriority;
        this.maxConcurrentJobs = _.clamp(maxConcurrentJobs, 1, Number.MAX_SAFE_INTEGER);

        this.activeJobs = [];
        this.activeTasks = [];
        this.waitingJobs = [];

        this.maintainJobQueue = promiseDebouncer(_.bind(this.maintainJobQueueActual, this), DEBOUNCE_INTERVAL, {
            maxWait: DEBOUNCE_MAX_WAIT
        });
        this.maintainTaskQueue = promiseDebouncer(_.bind(this.maintainTaskQueueActual, this), DEBOUNCE_INTERVAL, {
            maxWait: DEBOUNCE_MAX_WAIT
        });

        this.active = false;
        this.initializing = false;
        this.closing = false;
    }

    * initialize() {
        if (this.active || this.initializing || this.closing) {
            return;
        }

        this.initializing = true;

        this.activeJobs = [];
        this.activeTasks = [];
        this.waitingJobs = [];

        this.connection = yield amqp.connect(this.connectionOptions.amqpURL);
        this.channel = yield this.connection.createChannel();

        yield this.channel.assertQueue(this.connectionOptions.jobQueue, {
            maxPriority: MAX_PRIORITY
        });
        yield this.channel.assertQueue(this.connectionOptions.taskQueue, {
            maxPriority: MAX_PRIORITY
        });
        yield this.channel.assertExchange(this.connectionOptions.taskResultExchange, 'fanout');

        let taskResultQueue = yield this.channel.assertQueue('', {
            exclusive: true,
            autoDelete: true
        });
        yield this.channel.bindQueue(taskResultQueue.queue, this.connectionOptions.taskResultExchange, '');

        yield this.channel.prefetch(this.maxConcurrentJobs);
        let jobConsumerInfo = yield this.channel.consume(this.connectionOptions.jobQueue, _.bind(this.receiveJob, this));
        this.jobConsumerTag = jobConsumerInfo.consumerTag;

        yield this.channel.prefetch(1);
        yield this.channel.consume(taskResultQueue.queue, _.bind(this.receiveTaskResult, this));

        this.initializing = false;
        this.active = true;
    }

    * shutdown(immediate = false) {
        if (!this.active || this.initializing || this.closing) {
            return;
        }

        this.closing = true;

        if (immediate) {
            // stop receiving jobs
            yield this.channel.cancel(this.jobConsumerTag);

            yield this.finishShutdown();
        }
        else {
            // stop receiving jobs
            yield this.channel.cancel(this.jobConsumerTag);

            // only return waiting jobs to queue
            yield _.map(this.waitingJobs, job => this.channel.reject(job.originalMessage, true));

            // clear waiting jobs list
            this.waitingJobs = [];

            yield this.maintainJobQueue();

            // all active jobs will be completed regularly before shutdown finishes
        }
    }

    * finishShutdown() {
        if (!this.closing) {
            return;
        }

        // return all jobs to queue
        yield _.map([...this.waitingJobs, ...this.activeJobs], job => this.channel.reject(job.originalMessage, true));

        // clear lists
        this.activeJobs = [];
        this.activeTasks = [];
        this.waitingJobs = [];

        // close the channel and connection
        yield this.channel.close();
        yield this.connection.close();

        this.closing = false;
        this.active = false;
    }

    * receiveJob(msg, manual = false) {
        if (!this.active) {
            return;
        }

        if (this.closing) {
            // reject new job and send back to queue

            yield this.channel.reject(msg, true);
            return;
        }

        let job = helpers.convertBufferToJSON(msg.content);

        yield this.processJob(_.assign({}, job, {
            priority: msg.properties.priority,
            returnID: msg.properties.correlationId,
            returnQueue: msg.properties.replyTo,
            jobID: msg.properties.messageId,
            timestamp: msg.properties.timestamp,
            jobType: msg.properties.type,
            jobOrigin: msg.properties.appId,
            originalMessage: msg
        }), !manual);
    }

    * receiveTaskResult(msg) {
        if (!this.active) {
            return;
        }

        let result = helpers.convertBufferToJSON(msg.content);

        yield this.processTaskResult(_.assign({}, result, {
            priority: msg.properties.priority,
            id: msg.properties.correlationId,
            timestamp: msg.properties.timestamp,
            type: msg.properties.type,
            originalMessage: msg
        }));
    }

    * processJob({
        tasks = [],
        priority = this.defaultPriority,
        unique = false,
        retries = 0,
        timestamp = moment().unix(),
        returnQueue,
        returnID,
        jobID,
        jobType,
        jobOrigin,
        originalMessage
    } = {}, maintainQueue = true) {
        if (jobID || jobType || jobOrigin) {
            debug(`received ${helpers.describeJob(jobID, jobType, jobOrigin)} from queue`);
        }

        timestamp = moment.unix(timestamp);

        if (_.size(this.activeJobs) >= this.maxConcurrentJobs || moment().isBefore(timestamp)) {
            this.waitingJobs.push({
                priority,
                timestamp,
                originalMessage
            });
        }
        else {
            let transformedTasks = _.map(tasks, task => ({
                original: _.cloneDeep(task),
                priority: _.defaultTo(task.priority, priority),
                unique: _.defaultTo(task.unique, unique),
                retries: _.defaultTo(task.retries, retries),
                timestamp: task.timestamp ? moment(task.timestamp) : timestamp,
                id: (task.unique || unique) ? uuid.v4() : undefined,
                completed: false,
                success: null,
                result: null,
                error: null
            }));

            this.activeJobs.push({
                tasks: transformedTasks,
                priority,
                timestamp,
                returnQueue,
                returnID,
                jobID,
                jobType,
                jobOrigin,
                originalMessage
            });
        }

        if (maintainQueue) {
            yield this.maintainJobQueue();
        }
    }

    * processTaskResult({
        type,
        detail,
        id,
        timestamp = moment().unix(),
        success = false,
        result,
        error,
        originalMessage
    } = {}) {
        // remove from active tasks
        _.remove(this.activeTasks, _.partial(helpers.areTasksEquivalent, {
            type,
            detail,
            id
        }));

        timestamp = moment.unix(timestamp);

        for (let job of this.activeJobs) {
            if (timestamp.isAfter(job.timestamp)) {
                _.forEach(job.tasks, function(task) {
                    if (helpers.areTasksEquivalent(task, {
                            type,
                            detail,
                            id
                        }) && !task.completed) {
                        if (success) {
                            task.completed = true;
                            task.success = true;
                            task.result = result;
                            task.error = error;
                        }
                        else {
                            if (task.retries > 0) {
                                task.retries--;
                            }
                            else {
                                task.completed = true;
                                task.success = false;
                                task.result = result;
                                task.error = error;
                            }
                        }
                    }
                });
            }
        }

        if (originalMessage && this.active) {
            yield this.channel.ack(originalMessage);
        }

        yield this.maintainJobQueue();
    }

    * maintainJobQueueActual() {
        if (!this.active) {
            return;
        }

        let delay = null;

        for (let i = 0; i < _.size(this.activeJobs); i++) {
            let currentJob = this.activeJobs[i];

            let completedTasks = _.filter(currentJob.tasks, 'completed');
            let failedTask = _.find(completedTasks, task => !task.success);

            let response;

            if (failedTask) {
                // one task failed so entire job failed

                response = {
                    success: false,
                    failedTask: failedTask.original,
                    error: failedTask.error,
                    results: _.map(currentJob.tasks, 'result'),
                    tasks: _.map(currentJob.tasks, 'original')
                };
            }
            else if (_.size(completedTasks) === _.size(currentJob.tasks)) {
                // all tasks completed

                response = {
                    success: true,
                    results: _.map(currentJob.tasks, 'result'),
                    tasks: _.map(currentJob.tasks, 'original')
                };
            }
            else {
                response = null;
            }

            if (response) {
                // job completed, remove from active list
                _.pull(this.activeJobs, currentJob);
                i--;

                if (currentJob.returnQueue) {
                    // send response
                    yield this.channel.sendToQueue(currentJob.returnQueue, helpers.convertJSONToBuffer(response), {
                        priority: currentJob.priority,
                        persistent: true,
                        correlationId: currentJob.returnID,
                        messageId: currentJob.jobID,
                        timestamp: moment().unix(),
                        type: currentJob.jobType,
                        appId: currentJob.jobOrigin
                    });
                }

                if (currentJob.originalMessage && this.active) {
                    // acknowledge message to remove from queue
                    yield this.channel.ack(currentJob.originalMessage);
                }
            }
        }

        if (!this.closing) {
            // only retrieve more jobs if not closing

            if (_.size(this.activeJobs) < this.maxConcurrentJobs) {
                this.waitingJobs = _.orderBy(this.waitingJobs, [job => job.timestamp.valueOf(), 'priority'], ['asc', 'desc']);

                // push jobs out of the waiting queue to be requeued
                while (_.size(this.activeJobs) < this.maxConcurrentJobs && _.size(this.waitingJobs) > 0) {
                    let waitingJob = _.head(this.waitingJobs);

                    if (moment().isBefore(waitingJob.timestamp)) {
                        // ensure the job queue is checked again once enough time has passed
                        delay = waitingJob.timestamp.diff(moment());

                        break;
                    }

                    if (waitingJob.originalMessage) {
                        // requeue the job
                        this.waitingJobs = _.drop(this.waitingJobs);
                        yield this.receiveJob(waitingJob.originalMessage, true);
                    }
                }

                // accept more jobs if limit is still not reached
                if (_.size(this.activeJobs) < this.maxConcurrentJobs) {
                    let jobsAdded = false;

                    while (_.size(this.activeJobs) < this.maxConcurrentJobs) {
                        let msg = yield this.channel.get(this.connectionOptions.jobQueue);

                        // check if actually a message first, if not no jobs are available to be fetched
                        if (msg === false) {
                            // check again in a second
                            if (_.isNil(delay) || delay > 1000) {
                                delay = 1000;
                            }

                            break;
                        }

                        yield this.receiveJob(msg, true);

                        jobsAdded = true;
                    }

                    if (jobsAdded) {
                        // run this again immediately
                        _.defer(_.bind(this.maintainJobQueue, this));
                    }
                }
            }
        }
        else {
            if (_.size(this.activeJobs) === 0) {
                // no more jobs to process, finish shutting down

                yield this.finishShutdown();
                return;
            }
        }

        this.activeJobs = _.orderBy(this.activeJobs, ['priority'], ['desc']);

        if (!_.isNil(delay)) {
            // run this again after a delay
            _.delay(_.bind(this.maintainJobQueue, this), delay);
        }

        yield this.maintainTaskQueue();
    }

    * maintainTaskQueueActual() {
        if (!this.active) {
            return;
        }

        for (let job of this.activeJobs) {
            for (let task of job.tasks) {
                if (!task.completed) {
                    yield this.queueTask(task);
                }
            }
        }
    }

    * queueTask({
        type,
        detail,
        id,
        priority = DEFAULT_PRIORITY
    } = {}) {
        let activeTask = _.find(this.activeTasks, _.partial(helpers.areTasksEquivalent, {
            type,
            detail,
            id
        }));
        let newTask = {
            type,
            detail,
            id
        };

        if (!activeTask) {
            yield this.channel.sendToQueue(this.connectionOptions.taskQueue, helpers.convertJSONToBuffer(newTask), {
                priority,
                persistent: true,
                correlationId: id,
                timestamp: moment().unix(),
                type
            });

            this.activeTasks.push(newTask);
        }
        else if (activeTask.priority < priority) {
            yield this.channel.sendToQueue(this.connectionOptions.taskQueue, helpers.convertJSONToBuffer(newTask), {
                priority,
                persistent: true,
                correlationId: id,
                timestamp: moment().unix(),
                type
            });

            activeTask.priority = priority;
        }
    }
}

module.exports = asyncClass.wrap(RemoteWorkQueueManager);
