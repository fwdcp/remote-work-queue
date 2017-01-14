'use strict';

const _ = require('lodash');
const amqp = require('amqplib');
const asyncClass = require('async-class');
const debug = require('debug')('remote-work-queue:manager');
const moment = require('moment');
const promiseDebouncer = require('promise-debouncer');

const helpers = require('./helpers');
const RemoteWorkQueueClient = require('./client');
const RemoteWorkQueueTaskHelpers = require('./taskHelpers');

const DEBOUNCE_INTERVAL = 100;
const DEBOUNCE_MAX_WAIT = 1000;
const DEFAULT_PRIORITY = 15;
const MAX_PRIORITY = 255;

class RemoteWorkQueueWorker {
    constructor(taskFunctions = {}, {
        amqpURL = 'amqp://localhost',
        defaultPriority = DEFAULT_PRIORITY,
        maxConcurrentTasks = 1,
        jobQueue = 'remote-work-jobs',
        taskQueue = 'remote-work-tasks',
        taskResultExchange = 'remote-work-task-results',
        useSubtaskClient = false
    } = {}) {
        this.connectionOptions = {
            amqpURL,
            jobQueue,
            taskQueue,
            taskResultExchange
        };
        this.defaultPriority = defaultPriority;
        this.maxConcurrentTasks = maxConcurrentTasks;
        this.taskFunctions = taskFunctions;
        this.useSubtaskClient = useSubtaskClient;

        this.connection = null;
        this.channel = null;
        this.client = null;

        this.waitingTasks = [];
        this.activeTasks = new Set();
        this.suspendedTasks = new Set();

        this.maintainTasks = promiseDebouncer(_.bind(this.maintainTasksActual, this), DEBOUNCE_INTERVAL, {
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

        this.waitingTasks = [];
        this.activeTasks.clear();
        this.suspendedTasks.clear();

        this.connection = yield amqp.connect(this.connectionOptions.amqpURL);
        this.channel = yield this.connection.createChannel();

        yield this.channel.assertQueue(this.connectionOptions.taskQueue, {
            maxPriority: MAX_PRIORITY
        });
        yield this.channel.assertExchange(this.connectionOptions.taskResultExchange, 'fanout');

        yield this.channel.prefetch(this.maxConcurrentTasks);
        let taskConsumerInfo = yield this.channel.consume(this.connectionOptions.taskQueue, _.bind(this.receiveTask, this));
        this.taskConsumerTag = taskConsumerInfo.consumerTag;

        this.initializing = false;
        this.active = true;
    }

    * shutdown(immediate = false) {
        if (!this.active || this.initializing || this.closing) {
            return;
        }

        this.closing = true;

        if (immediate) {
            // stop receiving tasks
            yield this.channel.cancel(this.taskConsumerTag);

            if (this.client) {
                yield this.client.shutdown(true);
            }

            yield this.finishShutdown();
        }
        else {
            // stop receiving tasks
            yield this.channel.cancel(this.taskConsumerTag);

            if (this.client) {
                yield this.client.shutdown(false);
            }

            // at this point, all active tasks will be completed regularly before shutdown finishes
        }
    }

    * finishShutdown() {
        if (!this.closing) {
            return;
        }

        // return all tasks to queue
        yield _.map([...this.waitingTasks, ...this.activeTasks, ...this.suspendedTasks], task => (task.originalMessage ? this.channel.reject(task.originalMessage, true) : Promise.resolve()));

        // clear lists
        this.waitingTasks = [];
        this.activeTasks.clear();
        this.suspendedTasks.clear();

        // close the channel and connection
        yield this.channel.close();
        yield this.connection.close();

        this.closing = false;
        this.active = false;
    }

    * openClient() {
        if (!this.client) {
            this.client = new RemoteWorkQueueClient({
                amqpURL: this.connectionOptions.amqpURL,
                defaultPriority: this.defaultPriority,
                jobQueue: this.connectionOptions.jobQueue
            });

            yield this.client.initialize();
        }
    }

    * receiveTask(msg) {
        if (!this.active) {
            return;
        }

        if (this.closing) {
            // reject new task and send back to queue

            yield this.channel.reject(msg, true);
            return;
        }

        let task = helpers.convertBufferToJSON(msg.content);

        yield this.processTask(_.assign({}, task, {
            priority: msg.properties.priority,
            id: msg.properties.correlationId,
            timestamp: msg.properties.timestamp,
            type: msg.properties.type,
            originalMessage: msg
        }));
    }

    * processTask({
        type,
        detail,
        id,
        priority = this.defaultPriority,
        mergeWithActiveTask = true,
        originalMessage,
        resolve,
        reject
    } = {}, maintainQueue = true) {
        let sameTask = _.find(this.waitingTasks, _.partial(helpers.areTasksEquivalent, {
            type,
            detail,
            id
        }));

        if (!sameTask && mergeWithActiveTask) {
            sameTask = _.find([...this.activeTasks], _.partial(helpers.areTasksEquivalent, {
                type,
                detail,
                id
            }));
        }

        if (sameTask) {
            // merge with same task

            if (sameTask.originalMessage && originalMessage) {
                // remove duplicate task from queue

                if (sameTask.priority > priority) {
                    if (this.active) {
                        this.client.ack(originalMessage);
                    }
                }
                else {
                    if (this.active) {
                        this.client.ack(sameTask.originalMessage);
                    }

                    sameTask.originalMessage = originalMessage;
                }
            }

            sameTask.priority = Math.max(sameTask.priority, priority);

            if (resolve) {
                sameTask.resolves.push(resolve);
            }

            if (reject) {
                sameTask.rejects.push(reject);
            }
        }
        else {
            // add new task

            this.waitingTasks.push({
                type,
                detail,
                id,
                priority,
                originalMessage,
                resolves: [],
                rejects: []
            });
        }

        if (maintainQueue) {
            yield this.maintainTasks();
        }
    }

    * runTask(task) {
        let {
            type,
            detail,
            id,
            priority,
            originalMessage,
            resolves = [],
            rejects = []
        } = task;

        let taskHelpers = new RemoteWorkQueueTaskHelpers(this, task);

        _.pull(this.waitingTasks, task);
        this.activeTasks.add(task);

        let success;
        let result;
        let error;
        let timestamp = moment();

        try {
            if (!_.hasIn(this.taskFunctions, type)) {
                throw new Error('task type not supported');
            }

            success = true;
            result = yield this.taskFunctions(detail, taskHelpers);
        }
        catch (err) {
            success = false;
            error = err;
        }

        if (this.active) {
            yield this.channel.publish(this.connectionOptions.taskResultExchange, '', helpers.convertBufferToJSON({
                type,
                detail,
                id,
                success,
                result,
                error
            }), {
                priority,
                persistent: true,
                correlationId: id,
                timestamp: timestamp.unix(),
                type
            });

            if (originalMessage) {
                yield this.channel.ack(originalMessage);
            }
        }

        _.forEach(success ? resolves : rejects, func => func(result));
    }

    * maintainTasksActual() {
        if (!this.active) {
            return;
        }

        let activeTaskCount = this.activeTasks.size;

        if (activeTaskCount < this.maxConcurrentTasks) {
            this.waitingTasks = _.orderBy(this.waitingTasks, ['priority'], ['desc']);

            // push tasks out of the waiting queue to be activated
            while (activeTaskCount < this.maxConcurrentTasks && _.size(this.waitingTasks) > 0) {
                this.runTask(_.head(this.waitingTasks));
                activeTaskCount++;
            }

            // accept more tasks if the limit is still not reached
            if (!this.closing && activeTaskCount < this.maxConcurrentTasks) {
                let tasksAdded = false;

                while (activeTaskCount < this.maxConcurrentTasks) {
                    let msg = yield this.channel.get(this.connectionOptions.taskQueue);

                    // check if actually a message first, if not no tasks are available to be fetched
                    if (msg === false) {
                        // check again in a second
                        _.delay(_.bind(this.maintainTasks, this), 1000);

                        break;
                    }

                    yield this.receiveTask(msg, true);

                    tasksAdded = true;
                }

                if (tasksAdded) {
                    // run this again immediately
                    _.defer(_.bind(this.maintainTasks, this));
                }
            }
        }

        if (this.closing) {
            if (_.size(this.waitingTasks) === 0 && this.activeTasks.size === 0 && this.suspendedTasks.size === 0) {
                // no more tasks left, shut down

                yield this.finishShutdown();
            }
        }
    }
}

module.exports = asyncClass.wrap(RemoteWorkQueueWorker);
