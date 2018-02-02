const { SNS, SQS, config: AWSconfig } = require('aws-sdk');
AWSconfig.update({region:'ap-southeast-2'});

const sns = new SNS();
const sqs = new SQS();

const workerQueue = 'worker-a';

async function getOrCreateQueue() {
  const { TopicArn } = await sns.createTopic({
      Name: 'test-topic',
    })
    .promise();

  const { QueueUrl } = await sqs.createQueue({
      QueueName: workerQueue,
      Attributes: {

      }
    })
    .promise();

  const { Attributes: { QueueArn } } = await sqs.getQueueAttributes({
      QueueUrl,
      AttributeNames: ['QueueArn']
    })
    .promise();

  const { SubscriptionArn } = await sns.subscribe({
    TopicArn,
    Protocol: 'sqs',
    Endpoint: QueueArn,
  }).promise();

  return QueueUrl;
}

async function recieveMessages() {
  const QueueUrl = await getOrCreateQueue();

  const { Messages } = await sqs.receiveMessage({
    QueueUrl,
  }).promise();

  console.log('messages: ', Messages);

  await recieveMessages();
}


recieveMessages()
  .catch(error => console.error(error));