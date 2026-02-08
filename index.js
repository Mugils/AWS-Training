import { PublishCommand } from '@aws-sdk/client-sns';
import { snsClient } from './utilities.js';

const processSqsMessage = async (event) => {
    let eventArray = [];
    console.log('Received SQS event:', event);
    // Process all records in the array
    for (const record of event.Records) {
    const message = JSON.parse(record.body);
    message.Records.forEach((s3Event) => {
      const extractedData = {
        awsRegion: s3Event.awsRegion,
        eventName: s3Event.eventName,
        s3Object: {
          bucket: s3Event.s3.bucket.name,
          key: s3Event.s3.object.key
        }
      };
      console.log('Extracted S3 event data:', extractedData);
      eventArray.push(extractedData);
    });
    
    }
    return eventArray;
};



const sendNotification = async (event) => {
    const fetchdatafromSQS = await processSqsMessage(event);
    
    // Process each S3 event and send SNS notification
    const notificationPromises = fetchdatafromSQS.map(async ({ awsRegion, s3Object, eventName }) => {
        const { bucket, key } = s3Object;
        const formatMsg = `File ${key} has been ${eventName === 'ObjectCreated:Put' ? 'uploaded' : 'deleted'} in bucket ${bucket} in region ${awsRegion}`;
        
        snsClient.config.region = awsRegion;
        const params = {
            TopicArn: process.env.SNS_TOPIC_ARN,
            Message: formatMsg,
        };

        const response = await snsClient.send(new PublishCommand(params));
        console.log('Published message:', response);
        return response;
    });
    
    return await Promise.all(notificationPromises);
};

export const handler = async (event) => {
    try {
        const sendEvent = await sendNotification(event);
        return sendEvent;
    } catch (error) {
        console.error('Error sending notification:', error);
        throw error;
    }
};