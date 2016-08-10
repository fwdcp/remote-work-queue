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
        timeout = 1000,
        value = 'hello world',
        success = true
    } = {}) {
        return new Promise(_.bind(function(resolve, reject) {
            this.called++;

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
        let results = yield basicClient.queueJob([{
            type: 'basicWait'
        }]);

        expect(results).to.have.length(1);
        expect(results[0]).to.equal('hello world');
    }));

    it('should report failures properly', co.wrap(function*() {
        expect(basicClient.queueJob([{
            type: 'basicWait',
            success: false
        }])).to.be.rejectedWith('hello world');
    }));

    it('should apply results to non-unique jobs', co.wrap(function*() {
        expect(basicClient.queueJob([{
            type: 'basicWait',
            timeout: 2000
        }])).to.be.fulfilled;
        expect(basicClient.queueJob([{
            type: 'basicWait',
            timeout: 2000
        }])).to.be.fulfilled;
        expect(basicClient.queueJob([{
            type: 'basicWait',
            timeout: 2000
        }])).to.be.fulfilled;
        expect(basicClient.queueJob([{
            type: 'basicWait',
            timeout: 2000
        }])).to.be.fulfilled;
        expect(basicClient.queueJob([{
            type: 'basicWait',
            timeout: 2000
        }])).to.be.fulfilled;
    }));

    afterEach(co.wrap(function*() {
        yield basicClient.close();
        yield basicWorker.close();
    }));
});
