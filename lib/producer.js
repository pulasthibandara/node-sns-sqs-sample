
const { SNS, SQS, config: AWSconfig } = require('aws-sdk');
AWSconfig.update({region:'ap-southeast-2'});

const args = ['name'];

const { name } = args.reduce((acc, arg) => {
  const idx = process.argv.indexOf(`--${arg}`);
  const value = idx === -1 || process.argv[idx + 1].startsWith('--') ?
    '' :
    process.argv[idx + 1];

  return { ...acc, [arg]: value };
}, {});


const sns = new SNS();
let counter = 0;

async function getOrCreateTopic() {
  const { TopicArn } = await sns.createTopic({
      Name: 'test-topic',
    })
    .promise();

  return TopicArn;
}

async function publishToTopic() {
  const TopicArn = await getOrCreateTopic();
  setInterval(() => {
    sns.publish({ Message: `Message: ${counter} Producer: ${name}`, TopicArn })
      .promise()
      .catch(e => console.error(e));
    counter++;
  }, 10000);
}

publishToTopic()
  .catch(err => console.log(err));
