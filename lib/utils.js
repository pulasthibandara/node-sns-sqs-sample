const { SNS, SQS, config: AWSconfig, STS } = require('aws-sdk');
AWSconfig.update({ region:'ap-southeast-2' });

const sns = new SNS();
const sqs = new SQS();

const getQueuePolicySid = (TopicArn) => {
  return `${TopicArn}-topic-queue`;
}

const getPolicyStatement = (TopicArn, QueueArn) => ({
  Sid: getQueuePolicySid(TopicArn),
  Effect: 'Allow',
  Principal: '*',
  Action: '*',
  Resource: QueueArn,
  Condition: { 
    ArnEquals: {
      'aws:SourceArn': TopicArn,
    }
  }
});

const mergePolicies = (TopicArn, QueueArn, Policy = {}) => {
  const { Statement = [], Version = '2012-10-17' } = Policy;
  const policyExists = Statement.includes(
    ({ Sid = '' }) => Sid == getQueuePolicySid(TopicArn)
  );

  return {
    Version,
    Statement: [
      ...Statement,
      ...(policyExists ? [] : getPolicyStatement(TopicArn, QueueArn))
    ]
  }
}

const getQueueAttributes = async (QueueUrl, AttributeNames = ['QueueArn', 'Policy']) => {
  const { Attributes } = await sqs.getQueueAttributes({
    QueueUrl,
    AttributeNames,
  })
  .promise();

  return Attributes;
}

const addTopicToQueuePolicy = async (QueueUrl, TopicArn, QueueAttributes) => {
  const { QueueArn, Policy } = QueueAttributes;

  const Policy = mergePolicies(TopicArn, QueueArn, Policy);

  return sqs.setQueueAttributes({
    QueueUrl,
    Attributes: { Policy: JSON.stringify(Policy) },
  })
  .promise();
}

/**
 * @param {string} QueueArn
 * @param {string} TopicArn
 * @return {string} SubscriptionArn
 */
const subscribeQueueToTopic = async (QueueArn, TopicArn) => {
  const { SubscriptionArn } = sns.subscribe({
      TopicArn,
      Protocol: 'sqs',
      Endpoint: QueueArn,
    }).promise();

  return SubscriptionArn;
}

const attachQueueToTopic = async (QueueUrl, TopicArn) => {
  const Attributes = await getQueueAttributes(QueueUrl);
  await addTopicToQueuePolicy(QueueUrl, TopicArn, QueueAttributes);
  await subscribeQueueToTopic(Attributes.QueueArn, TopicArn);
}
