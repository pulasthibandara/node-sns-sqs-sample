const { SNS, SQS, config: AWSconfig, STS } = require('aws-sdk');
const { attachQueueToTopic } = require('./utils');
AWSconfig.update({region:'ap-southeast-2'});

const args = ['name', 'group'];

const { name, group } = args.reduce((acc, arg) => {
  const idx = process.argv.indexOf(`--${arg}`);
  const value = idx === -1 || process.argv[idx + 1].startsWith('--') ?
    '' :
    process.argv[idx + 1];

  return { ...acc, [arg]: value };
}, {});


const sns = new SNS();
const sqs = new SQS();
const sts = new STS();

const workerQueue = `test-worker-${group}`;

async function getOrCreateQueue() {
  const { Account } = await sts.getCallerIdentity().promise()

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

  await attachQueueToTopic(QueueUrl, TopicArn);

  return QueueUrl;
}

async function recieveMessages() {
  const QueueUrl = await getOrCreateQueue();

  const { Messages = [] } = await sqs.receiveMessage({
    QueueUrl,
  }).promise();

  Messages.forEach(({ Body }) => {
    const { Message } = JSON.parse(Body);
    console.log('group: ', group, 'worker: ', name, 'recieved: ', Message);
  });

  await recieveMessages();
}


recieveMessages()
  .catch(error => console.error(error));