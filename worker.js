'use strict';

const _ = require('lodash');
const amqp = require('amqplib');
const asyncClass = require('async-class');
const moment = require('moment');

const helpers = require('./helpers');

class RemoteWorkQueueWorker {
    constructor(taskFunctions = {}, {
        connect = 'amqp://localhost',
        commandQueue = 'remote-work-commands',
        resultExchange = 'remote-work-results',
        concurrentTasks = 1
    } = {}) {
        this.taskFunctions = taskFunctions;
        this.connectionOptions = {
            connect,
            commandQueue,
            resultExchange
        };
        this.concurrentTasks = concurrentTasks;

        this.active = false;
        this.connection = null;
        this.channel = null;
        this.consumerTag = null;
        this.currentTasks = 0;
        this.closing = false;
        this.reportCloseComplete = _.noop;
    }

    *
    initialize() {
        if (!this.active) {
            this.connection = yield amqp.connect(this.connectionOptions.connect);
            this.channel = yield this.connection.createChannel();

            yield this.channel.assertQueue(this.connectionOptions.commandQueue);
            yield this.channel.prefetch(this.concurrentTasks);

            yield this.channel.assertExchange(this.connectionOptions.resultExchange, 'fanout');

            this.active = true;
            this.closing = false;
            this.reportCloseComplete = _.noop;

            let consumeResponse = yield this.channel.consume(this.connectionOptions.commandQueue, this.processTask.bind(this));
            this.consumerTag = consumeResponse.consumerTag;
        }
    }

    *
    close(immediate = false) {
        if (!this.active || this.closing) {
            return;
        }

        if (immediate) {
            this.active = false;

            yield this.channel.close();
            yield this.connection.close();
        }
        else {
            this.closing = true;

            yield this.channel.cancel(this.consumerTag);

            if (this.currentTasks !== 0) {
                yield new Promise(_.bind(function(resolve) {
                    this.reportCloseComplete = resolve;
                }, this));
            }

            yield this.channel.close();
            yield this.connection.close();

            this.active = false;
            this.closing = false;
            this.reportCloseComplete = _.noop;
        }
    }

    *
    processTask(msg) {
        if (!this.active) {
            return;
        }

        this.currentTasks++;

        let task = helpers.convertBufferToJSON(msg.content);

        yield this.runTask(task);

        this.channel.ack(msg);

        this.currentTasks--;

        if (this.closing) {
            if (this.currentTasks === 0) {
                this.reportCloseComplete();
            }
        }
    }

    *
    runTask(task) {
        let response = {
            start: moment().valueOf(),
            task
        };

        try {
            if (!_.hasIn(this.taskFunctions, task.type)) {
                throw new Error('unknown task type given');
            }

            let result = yield this.taskFunctions[task.type](task, this);

            response.success = true;
            response.result = result;
        }
        catch (err) {
            response.success = false;
            response.error = err.message;
        }

        if (this.active) {
            this.channel.publish(this.connectionOptions.resultExchange, '', helpers.convertJSONToBuffer(response));
        }
    }
}

module.exports = asyncClass.wrap(RemoteWorkQueueWorker);
