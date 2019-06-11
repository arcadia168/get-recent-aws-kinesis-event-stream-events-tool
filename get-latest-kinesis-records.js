const AWS = require('aws-sdk');

const getKinesisRecords = async (kinesis, shardIterator) => {
    const getRecordsParams = {
        ShardIterator: shardIterator,
    };

    let getRecordsResponse;
    try {
        getRecordsResponse = await kinesis.getRecords(getRecordsParams).promise();
    } catch (getRecordsError) {
        if (getRecordsError) {
            throw new Error(
                `getRecords request failed.
                \n params: ${JSON.stringify(getRecordsParams)}
                \n error: ${JSON.stringify(getRecordsError)}`);
        }
    }

    const retrievedEvents = getRecordsResponse.Records;
    if (retrievedEvents && retrievedEvents.length > 0) {
        const parsedRecordEventData = [];
        getRecordsResponse.Records.forEach(record => {
            parsedRecordEventData.push(JSON.parse(record.Data));
        });
        return parsedRecordEventData;
    }
};

const getShardIterator = async (kinesis, streamName, shardId, shardIteratorType = 'LATEST', unixTimestamp) => {
    const getShardIteratorParams = {
        StreamName: streamName,
        ShardId: shardId,
        ShardIteratorType: shardIteratorType,
    };

    if (shardIteratorType === 'AT_TIMESTAMP' && unixTimestamp) {
        getShardIteratorParams.Timestamp = unixTimestamp;
    }

    let shardIterator;
    try {
        shardIterator = await kinesis.getShardIterator(getShardIteratorParams).promise();
    } catch (getShardIteratorError) {
        throw new Error(
            `getShardIterator request failed.
            \n error: ${getShardIteratorError}`,
        );
    }

    return shardIterator;
};

const getLatestKinesisRecordsTool = async (envToQuery, minsAgoToReadFrom, streamName, shardId) => {

    console.info(`envToQuery is: ${envToQuery}`);
    console.log(`minsAgoToReadFrom is: ${minsAgoToReadFrom}`);
    console.log(`streamName is: ${streamName}`);
    console.log(`shardId is: ${shardId}`);

    let defaultRegion;

    if (envToQuery === 'dev') {
        defaultRegion = 'us-west-2'; // Oregon
    } else if (envToQuery === 'qa') {
        defaultRegion = 'eu-west-1'; // Ireland
    } else if (envToQuery === 'prod') {
        defaultRegion = 'eu-west-1'; // Ireland
    }

    const kinesis = new AWS.Kinesis({
        region: defaultRegion,
    });

    const getRecordsTimestamp = new Date();
    console.log(`getRecordsTimestamp is: ${getRecordsTimestamp}`);

    const defaultMinsAgoToReadFrom = minsAgoToReadFrom ? minsAgoToReadFrom : 5;
    console.log(`The defaultMinsAgoToReadFrom is: ${defaultMinsAgoToReadFrom}`);

    getRecordsTimestamp.setMinutes(getRecordsTimestamp.getMinutes() - defaultMinsAgoToReadFrom);
    console.log(`The getRecordsTimestamp is: ${getRecordsTimestamp}`);

    const unixTimestamp = getRecordsTimestamp.getTime() / 1000;
    console.log(`The unixTimestamp is: ${unixTimestamp}`);

    const defaultStreamNamePrefix = streamName ? streamName : 'experiments-eventstream-'
    let streamNameSuffix;
    if (!envToQuery || envToQuery === 'dev') {
        streamNameSuffix = 'dev';
    } else if (envToQuery === 'qa') {
        streamNameSuffix = 'qa';
    } else if (envToQuery === 'prod') {
        streamNameSuffix = 'prod';
    }
    console.log(`The streamNameSuffix is: ${streamNameSuffix}`);

    const fullStreamName = defaultStreamNamePrefix + streamNameSuffix;
    console.log(`The fullStreamName is: ${fullStreamName}`);

    const defaultShardId = shardId ? shardId : 'shardId-000000000000';

    try {
        const shardIterator = await getShardIterator(kinesis, fullStreamName, defaultShardId, 'AT_TIMESTAMP', unixTimestamp);
        console.log(`shardIterator is: ${JSON.stringify(shardIterator)}`);
        const latestRecords = await getKinesisRecords(kinesis, shardIterator.ShardIterator);
        console.log(`latestRecords are: ${JSON.stringify(latestRecords)}`);
        return latestRecords;
    } catch (error) {
        throw new Error(`getLatestKinesisRecordsTool error: ${JSON.stringify(error)}`);
    }
};

module.exports.getLatestKinesisRecordsTool = getLatestKinesisRecordsTool;
