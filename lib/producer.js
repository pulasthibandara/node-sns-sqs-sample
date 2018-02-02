
const { SNS, SQS, config: AWSconfig } = require('aws-sdk');
AWSconfig.update({region:'ap-southeast-2'});

const sns = new SNS();

async function getOrCreateTopic() {
  const { TopicArn } = await sns.createTopic({
      Name: 'test-topic',
    })
    .promise();

  return TopicArn;
}

async function publishToTopic() {
  const TopicArn = await getOrCreateTopic();
  return sns.publish({ Message: 'this is a test message', TopicArn }).promise();
}

publishToTopic()
  .catch(err => console.log(err));
