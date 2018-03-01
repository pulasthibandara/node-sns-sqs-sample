const { SNS, SQS, config: AWSconfig } = require('aws-sdk');
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
  Action: 'SQS:*',
  Resource: QueueArn,
  Condition: { 
    ArnEquals: {
      'aws:SourceArn': TopicArn,
    }
  }
});

const mergePolicies = (TopicArn, QueueArn, Policy = {}) => {
  const { Statement = [], Version = '2012-10-17' } = Policy;
  const policyExists = Statement.some(
    ({ Sid = '' }) => Sid === getQueuePolicySid(TopicArn)
  );

  return {
    Version,
    Statement: [
      ...Statement,
      ...(policyExists ? [] : [getPolicyStatement(TopicArn, QueueArn)])
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

  const MergedPolicy = mergePolicies(TopicArn, QueueArn, JSON.parse(Policy));

  return sqs.setQueueAttributes({
    QueueUrl,
    Attributes: { Policy: JSON.stringify(MergedPolicy) },
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
  const QueueAttributes = await getQueueAttributes(QueueUrl);
  await addTopicToQueuePolicy(QueueUrl, TopicArn, QueueAttributes);
  await subscribeQueueToTopic(QueueAttributes.QueueArn, TopicArn);
}

const _pollSQS = Symbol('_pollSQS');

class Message {
  /**
   * 
   * @param {AWS.SQS.Message} Message
   */
  constructor(Message, QueueUrl) {
    const { Body, ReceiptHandle } = Message;
    let parsed;

    try {
      parsed = JSON.parse(Body);
    } catch {
      parsed = Body;
    }

    this.body = parsed;
    this.receiptHandle = ReceiptHandle;
    this.queueUrl = QueueUrl;
  }

  async ack() {
    return sqs.deleteMessage({
      ReceiptHandle: this.receiptHandle,
      QueueUrl: this.queueUrl,
    })
    .promise();
  }
}

class Subscriber {
  constructor(QueueUrl) {
    this.QueueUrl = QueueUrl
  }

  /**
   * subscribe to events from the queue. Internally this polls SQS queue every
   * 100ms.
   * @param {(message) => Promise<any>} cb 
   */
  async subscribe(cb, Options) {
    this.timer = setTimeout(() => this[_pollSQS](cb), 100);
  }

  /**
   * stops polling the SQS queue. doesn't unsubscribe the actual queue from the
   * topic.
   */
  unsubscribe() {
    clearTimeout(this.timer);
  }

  async [_pollSQS](cb) {
    const { Messages = [] } = await sqs.receiveMessage({
      QueueUrl: this.QueueUrl,
    }).promise();

    Messages.forEach((Message) => {
      cb(new Message(Message, this.QueueUrl));
    });
  }
}

/**
 * Creates a queue with the name of *workerGrpup* and subscribe that queue to
 * the specified *topicName*.
 * @param {String} topicName the topic to subsctibe to.
 * @param {String} workerGroup a topic will round-rob messages to workers in the
 * group.
 */
const createSubscriber = async (topicName, workerGroup, Options = {}) => {
  const { TopicArn } = await sns
    .createTopic({ Name: 'test-topic' })
    .promise();
  const { QueueUrl } = await sqs
    .createQueue({ QueueName: workerQueue, Attributes: Options })
    .promise();

  await attachQueueToTopic(TopicArn, QueueUrl);

  return new Subscriber(QueueUrl, Options);
}

module.exports = {
  attachQueueToTopic,
  createSubscriber,
}
