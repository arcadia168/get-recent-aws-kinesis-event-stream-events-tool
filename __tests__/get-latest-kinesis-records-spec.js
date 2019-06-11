/* eslint-disable no-restricted-syntax */
const fs = require('fs');
const AWS = require('aws-sdk');
const path = require('path');
const MockDate = require('mockdate');
const getLatestKinesisRecords = require('../get-latest-kinesis-records');

jest.mock('aws-sdk', () => ({
    Kinesis: jest.fn(),
}));

let kinesisMock;
const latestShardIterator = {
    ShardIterator: 'AAAAAAAAAAFGkT9M784sSF8aOqWcQqjhPieoWxhrkv+Yf3nxtXh9ozYd2+P2gPYH21w6Ak3VtSuBfDA0+X1DXnoKpjlFA8pGp4oc5iinqWCdsH28Y2E2mh4vA0P2qKrLvjeBWDnEsRbPVx4d5ksgRKicu4el5zL04UkAFBGyS007BfwcLsKuaJUsB31+EnyD/oVJVe3kKwsfSXDiiW1zEICX3+9i2C3whjyb0KQhz/RNGKHP3BU3tA==',
};
const latestGetRecordsResponse = JSON.parse(fs.readFileSync(path.join(__dirname, './mocks/getRecordsResponse.json'), 'utf8'));

for (let i = 0; i < latestGetRecordsResponse.Records.length; i += 1) {
    const rawData = latestGetRecordsResponse.Records[i].Data;
    latestGetRecordsResponse.Records[i].Data = JSON.stringify(rawData);
}

describe('getLatestKinesisRecords', () => {
    beforeEach(() => {
        kinesisMock = {
            getShardIterator: jest.fn(() => ({
                promise: () => Promise.resolve(latestShardIterator),
            })),
            getRecords: jest.fn(() => ({
                promise: () => Promise.resolve(latestGetRecordsResponse),
            })),
        };

        AWS.Kinesis = function awsKinesisConstructor() {
            return kinesisMock;
        };
        MockDate.set(1434319925275);
    });

    it('makes a getShardIterator request', async () => {
        await getLatestKinesisRecords.getLatestKinesisRecordsTool();
        expect(kinesisMock.getShardIterator).toHaveBeenCalled();
    });

    it('calls the getShardIterator request with default stream name parameter', async () => {
        await getLatestKinesisRecords.getLatestKinesisRecordsTool();
        expect(kinesisMock.getShardIterator.mock.calls[0][0].StreamName).toBe('experiments-eventstream-dev');
    });

    it('calls the getShardIterator request with default shard ID parameter', async () => {
        await getLatestKinesisRecords.getLatestKinesisRecordsTool();
        expect(kinesisMock.getShardIterator.mock.calls[0][0].ShardId).toBe('shardId-000000000000');
    });

    it('calls the getShardIterator request with default shard iterator type parameter', async () => {
        await getLatestKinesisRecords.getLatestKinesisRecordsTool();
        expect(kinesisMock.getShardIterator.mock.calls[0][0].ShardIteratorType).toBe('AT_TIMESTAMP');
    });

    it('calls the getShardIterator request with default timestamp parameter', async () => {
        await getLatestKinesisRecords.getLatestKinesisRecordsTool();
        const getRecordsTimestamp = new Date();
        getRecordsTimestamp.setMinutes(getRecordsTimestamp.getMinutes() - 5);
        const unixTimestamp = getRecordsTimestamp.getTime() / 1000;
        expect(kinesisMock.getShardIterator.mock.calls[0][0].Timestamp).toBe(unixTimestamp);
    });

    it('calls the getShardIterator request with QA event stream name if paramater passed', async () => {
        await getLatestKinesisRecords.getLatestKinesisRecordsTool('qa');
        expect(kinesisMock.getShardIterator.mock.calls[0][0].StreamName).toBe('experiments-eventstream-qa');
    });

    it('calls the getShardIterator request with PROD event stream name if paramater passed', async () => {
        await getLatestKinesisRecords.getLatestKinesisRecordsTool('prod');
        expect(kinesisMock.getShardIterator.mock.calls[0][0].StreamName).toBe('experiments-eventstream-prod');
    });

    it('calls the getShardIterator request with timestamp from designated number of minutes in the past', async () => {
        await getLatestKinesisRecords.getLatestKinesisRecordsTool('dev', 10);
        const getRecordsTimestamp = new Date();
        getRecordsTimestamp.setMinutes(getRecordsTimestamp.getMinutes() - 10);
        const unixTimestamp = getRecordsTimestamp.getTime() / 1000;
        expect(kinesisMock.getShardIterator.mock.calls[0][0].Timestamp).toBe(unixTimestamp);
    });

    it('calls the getRecords request', async () => {
        await getLatestKinesisRecords.getLatestKinesisRecordsTool();
        expect(kinesisMock.getRecords).toBeCalled();
    });

    it('calls the getRecords request with correct default shardIterator value', async () => {
        await getLatestKinesisRecords.getLatestKinesisRecordsTool();
        expect(kinesisMock.getRecords.mock.calls[0][0].ShardIterator).toBe(latestShardIterator.ShardIterator);
    });

    it('calls the getRecords request and gets a response', async () => {
        const response = await getLatestKinesisRecords.getLatestKinesisRecordsTool();
        const expectedResponses = latestGetRecordsResponse.Records.map(rawResponse => JSON.parse(rawResponse.Data));
        expect(response).toEqual(expectedResponses);
    });
});
