const _ = require('lodash');
const asyncClass = require('async-class');
const chai = require('chai');
const co = require('co');

chai.use(require('chai-as-promised'));
const expect = chai.expect;

const Client = require('../client');
const Worker = require('../worker');

const TestFunctions = asyncClass.wrap(class {
    constructor() {
        this.called = 0;
    }

    basicWait({
        timeout = 2000,
        value = 'hello world',
        success = true,
        returnOrder = false
    } = {}) {
        return new Promise(_.bind(function(resolve, reject) {
            this.called++;

            if (returnOrder) {
                value = this.called;
            }

            setTimeout(function() {
                if (success) {
                    resolve(value);
                }
                else {
                    reject(new Error(value));
                }
            }, timeout);
        }, this));
    }
});

describe('remote work queue', function() {
    this.timeout(10000);

    let basicClient;
    let basicWorker;
    let testFunctions;

    beforeEach(co.wrap(function*() {
        testFunctions = new TestFunctions();

        basicClient = new Client();
        yield basicClient.initialize();

        basicWorker = new Worker(testFunctions);
        yield basicWorker.initialize();
    }));

    it('should process basic tasks', co.wrap(function*() {
        expect(basicClient.queueJob([{
            type: 'basicWait'
        }])).to.be.fulfilled;
    }));

    it('should report failures properly', co.wrap(function*() {
        expect(basicClient.queueJob([{
            type: 'basicWait',
            success: false
        }])).to.be.rejectedWith('hello world');
    }));

    it('should apply results across non-unique jobs', co.wrap(function*() {
        expect(basicClient.queueJob([{
            type: 'basicWait',
            timeout: 4000
        }])).to.be.fulfilled;
        expect(basicClient.queueJob([{
            type: 'basicWait',
            timeout: 4000
        }])).to.be.fulfilled;
        expect(basicClient.queueJob([{
            type: 'basicWait',
            timeout: 4000
        }])).to.be.fulfilled;
        expect(basicClient.queueJob([{
            type: 'basicWait',
            timeout: 4000
        }])).to.be.fulfilled;
        expect(basicClient.queueJob([{
            type: 'basicWait',
            timeout: 4000
        }])).to.be.fulfilled;
    }));

    it('should run jobs concurrently', co.wrap(function*() {
        let superClient = new Client({
            concurrentJobs: 3
        });
        yield superClient.initialize();

        let superWorker = new Worker(testFunctions, {
            concurrentTasks: 3
        });
        yield superWorker.initialize();

        expect(superClient.queueJob([{
            type: 'basicWait',
            timeout: 3000
        }])).to.be.fulfilled;
        expect(superClient.queueJob([{
            type: 'basicWait',
            timeout: 4000
        }])).to.be.fulfilled;
        expect(superClient.queueJob([{
            type: 'basicWait',
            timeout: 5000
        }])).to.be.fulfilled;
    }));

    it('should run higher-priority jobs first', co.wrap(function*() {
        expect(basicClient.queueJob([{
            type: 'basicWait'
        }])).to.be.fulfilled;
        expect(basicClient.queueJob([{
            type: 'basicWait',
            returnOrder: true
        }], {
            priority: 20
        })).to.eventually.equal(4);
        expect(basicClient.queueJob([{
            type: 'basicWait',
            returnOrder: true
        }], {
            priority: 50
        })).to.eventually.equal(3);
        expect(basicClient.queueJob([{
            type: 'basicWait',
            returnOrder: true
        }], {
            priority: 100
        })).to.eventually.equal(2);
    }));

    afterEach(co.wrap(function*() {
        yield basicClient.close();
        yield basicWorker.close();
    }));
});
