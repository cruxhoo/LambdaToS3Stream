import {GetObjectCommand, S3Client} from '@aws-sdk/client-s3';
import {getSignedUrl} from '@aws-sdk/s3-request-presigner';
import {Upload} from '@aws-sdk/lib-storage';
import axios from 'axios';
import {PassThrough} from 'node:stream';
import {v4} from 'uuid';

const s3Client = new S3Client({region: process.env.AWS_DEFAULT_REGION});
const Bucket = 'testing-lambda-to-s3';

/**
 * Will Upload a file to S3 and generate a signed url
 *
 * @param readableStream
 * @param filename
 * @param contentType
 * @returns {Promise<string>}
 */
async function uploadToS3(readableStream, filename, contentType) {
    const passThroughStream = new PassThrough();
    const Key = `${v4()}-${filename}`;
    console.log(`Using filename: ${Key}`);
    try {
        const parallelUploads3 = new Upload({
            client: s3Client,
            params: {
                Bucket,
                Key,
                Body: passThroughStream,
                ContentType: contentType
            },
            queueSize: 4,
            partSize: 1024 * 1024 * 5,
            leavePartsOnError: false,
        });
        console.log(`Uploading file to S3 ....`)
        readableStream.pipe(passThroughStream);
        const res = await parallelUploads3.done();
        console.log(`Upload complete.`)

        // Get the presigned url
        console.log(`Generating signed url`)
        const getObjectCommand = new GetObjectCommand({
            Bucket,
            Key,
        });
        return await getSignedUrl(s3Client, getObjectCommand, {expiresIn: 3600});
    } catch (e) {
        console.log(e);
    }
}

export const lambdaHandler = async (event, context) => {
    console.log('Started handling the app');
    const extractFilename = (url) => {
        const baseUrl = url.split('?')[0];
        const parts = baseUrl.split('/');
        return parts[parts.length - 1];
    };
    const url = event['url'];
    const filename = extractFilename(url);
    const response = await axios.get(url, {responseType: 'stream'});
    const contentType = response.headers.getContentType();
    if (response.status !== 200) {
        return {
            statusCode: 500,
            body: JSON.stringify({
                message: `didn't get a good status response ${response} - ${response.status} - ${response.data}`,
            }),
        };
    }
    console.log(`The url responded with ${response.status}`);
    let signedUrl;
    try {
        signedUrl = await uploadToS3(response.data, filename, contentType);
    } catch (err) {
        console.error(`Application failed with ${JSON.stringify(err)}`);
        console.log(err);
        return err;
    }

    return {
        'statusCode': 200,
        'body': JSON.stringify({
            message: signedUrl,
        })
    }
};