'use strict';

const _ = require('lodash');
const asyncClass = require('async-class');
const uuid = require('uuid');

const ADDITIONAL_SUBTASK_PRIORITY = 128;
const MAX_PRIORITY = 255;

class RemoteWorkQueueTaskHelpers {
    constructor(worker, task) {
        this.worker = worker;
        this.task = task;
    }

    * suspendTask() {
        if (!this.worker.activeTasks.has(this.task)) {
            if (this.worker.suspendedTasks.has(this.task)) {
                return;
            }
            else {
                throw new Error('cannot suspend task');
            }
        }

        this.worker.activeTasks.delete(this.task);
        this.worker.suspendedTasks.add(this.task);
    }

    * unsuspendTask() {
        if (!this.worker.suspendedTasks.has(this.task)) {
            if (this.worker.activeTasks.has(this.task)) {
                return;
            }
            else {
                throw new Error('cannot unsuspend task');
            }
        }

        this.worker.suspendedTasks.delete(this.task);
        this.worker.activeTasks.add(this.task);
    }

    * waitForPromise(promise) {
        yield this.suspendTask();

        try {
            let result = yield promise;
            return result;
        }
        finally {
            yield this.unsuspendTask();
        }
    }

    * waitForSubtasks(subtasks = []) {
        let subtasksPromised;

        let defaultSubtaskPriority = _.clamp(this.task.priority + ADDITIONAL_SUBTASK_PRIORITY, MAX_PRIORITY);

        if (this.worker.useSubtaskClient && !this.worker.closing) {
            // use worker client

            yield this.worker.openClient();

            subtasksPromised = this.worker.client.queueJob(subtasks, {
                priority: defaultSubtaskPriority,
                unique: !!this.task.id,
                retries: 0
            });
        }
        else {
            // manually add tasks to this worker

            subtasksPromised = [];

            for (let subtask of subtasks) {
                let transformedSubtask = {
                    type: subtask.type,
                    detail: subtask.detail,
                    id: (subtask.unique || this.task.id) ? uuid.v4() : undefined,
                    priority: _.defaultTo(subtask.priority, defaultSubtaskPriority),
                    mergeWithActiveTask: false
                };
                subtasksPromised.push(new Promise(function(resolve, reject) {
                    transformedSubtask.resolve = resolve;
                    transformedSubtask.reject = reject;
                }));

                yield this.worker.processTask(transformedSubtask, false);
            }

            yield this.worker.maintainTasks();
        }

        let results = yield this.waitForPromise(subtasksPromised);
        return results;
    }
}

module.exports = asyncClass.wrap(RemoteWorkQueueTaskHelpers);
