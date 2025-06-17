import time
from pathlib import Path
from detect import run
import yaml
from loguru import logger
import json
import os
import requests
# Define aws related modules
import boto3
from botocore.exceptions import ClientError
# Define polybot microservices related modules
import pymongo


def get_secret():
    secret_name = "barrotem/polybot/k8s-project"
    region_name = "eu-north-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        raise e
    secret = get_secret_value_response[
        'SecretString']  #secret is a dictionary of all secrets defined within the manager
    return secret


# Load secrets from AWS Secret Manager
secrets_dict = json.loads(get_secret())
# Access secrets loaded from secret manager
IMAGES_BUCKET = secrets_dict["IMAGES_BUCKET"]
QUEUE_NAME = secrets_dict["POLYBOT_QUEUE"]
REGION_NAME = secrets_dict["DEPLOYED_REGION"]
# Initialize polybot microservices related variables
MONGO_URI = f'mongodb://{secrets_dict["MONGODB_HOSTS"]}/{secrets_dict["MONGODB_NAME"]}?replicaSet={secrets_dict["MONGODB_RS_NAME"]}'
mongo_client = pymongo.MongoClient(MONGO_URI)

# Initialize AWS clients for image processing
s3_client = boto3.client('s3')
sqs_client = boto3.client('sqs', region_name=REGION_NAME)

with open("data/coco128.yaml", "r") as stream:
    names = yaml.safe_load(stream)['names']


def consume():
    while True:
        response = sqs_client.receive_message(QueueUrl=QUEUE_NAME, MaxNumberOfMessages=1, WaitTimeSeconds=5)
        # Response should contain a message with information regarding a raw image waiting to be processed in S3

        if 'Messages' in response:
            message = response['Messages'][0]['Body']
            receipt_handle = response['Messages'][0]['ReceiptHandle']

            # Use the ReceiptHandle as a prediction UUID
            prediction_id = response['Messages'][0]['MessageId']

            # Log relevant information to the console
            logger.info(f'prediction: {prediction_id}. start processing')
            logger.info(f'message: {message}, receipt_handle : {receipt_handle}')

            # The SQS message contains metadata wih information regarding a raw image waiting to be processed in S3
            # message_dict is built by the following syntax :
            # {"text": "A new image was uploaded to the s3 bucket", "img_name": s3_photo_key, "chat_id": chat_id}
            message_dict = json.loads(message)
            img_name = message_dict["img_name"]
            chat_id = message_dict["chat_id"]

            # Download image from s3
            folder_name = img_name.split('/')[0]
            if not os.path.exists(folder_name):
                os.makedirs(folder_name)
            s3_client.download_file(Bucket=IMAGES_BUCKET, Key=img_name, Filename=img_name)
            original_img_path = img_name
            original_img_name = original_img_path.split("/")[
                1]  # Represents the image's final path component - it's name
            logger.info(f'prediction: {prediction_id}/{original_img_path}. Download img completed')

            # Predicts the objects in the image
            run(
                weights='yolov5s.pt',
                data='data/coco128.yaml',
                source=original_img_path,
                project='static/data',
                name=prediction_id,
                save_txt=True
            )
            logger.info(f'prediction: {prediction_id}/{original_img_path}. done')

            # This is the path for the predicted image with labels
            # The predicted image typically includes bounding boxes drawn around the detected objects,
            # along with class labels and possibly confidence scores.
            predicted_img_path = Path(
                f'static/data/{prediction_id}/{original_img_name}')  # Yolo5 saves predicted image to a path determined by image filename only

            # Upload the predicted image to s3
            predicted_img_key = f'predictions/{original_img_name}'  # Taking img_name suffix into account, this creates : "prediction/filename.filetype"
            s3_client.upload_file(Filename=predicted_img_path, Bucket=IMAGES_BUCKET, Key=predicted_img_key)
            logger.info(
                f'prediction: {prediction_id}/{original_img_path} uploaded to s3 with the key: {predicted_img_key}.')

            # Parse prediction labels and create a summary
            pred_summary_path = Path(f'static/data/{prediction_id}/labels/{original_img_name.split(".")[0]}.txt')
            if pred_summary_path.exists():
                with open(pred_summary_path) as f:
                    labels = f.read().splitlines()
                    labels = [line.split(' ') for line in labels]
                    labels = [{
                        'class': names[int(l[0])],
                        'cx': float(l[1]),
                        'cy': float(l[2]),
                        'width': float(l[3]),
                        'height': float(l[4]),
                    } for l in labels]

                logger.info(f'prediction: {prediction_id}/{original_img_path}. prediction summary:\n\n{labels}.')

                prediction_summary = {
                    'prediction_id': prediction_id,
                    'original_img_path': original_img_path,
                    'predicted_img_path': str(predicted_img_path),
                    's3_img_path': predicted_img_key,
                    'labels': labels,
                    'time': time.time()
                }

                # Store the prediction_summary in a MongoDB table
                logger.info(f'storing prediction_summary in mongodb...')
                test_db_client = mongo_client["test"]
                predictions_collection = test_db_client["predictions"]
                # Store a prediction inside the predictions collections
                document = {"_id": prediction_id,
                            "prediction_summary": prediction_summary}  # Allow fast indexing using prediction_id as the table's primary_key
                write_result = predictions_collection.insert_one(document)
                logger.info(f'Mongodb write result: {write_result}.')

            # The following block is executed whether predictions were made or not. Therefore, the user should be updated accordingly
            # Perform a POST request to Polybot to `/results` endpoint.
            # Polybot will inform the user on prediction operation status according to the value of prediction_id
            logger.info(f'Image processing finished. Sending a POST request to polybot.')
            requests.post(url=f'http://polybot-service:8443/results?predictionId={prediction_id}&chatId={chat_id}')

            # Delete the message from the queue as the job is considered as DONE
            logger.info(f'Deleting message {receipt_handle} from sqs, image processed successfully')
            sqs_client.delete_message(QueueUrl=QUEUE_NAME, ReceiptHandle=receipt_handle)


if __name__ == "__main__":
    try:
        consume()
    except Exception as e:
        # Except exceptions, log for development purposes
        print("General exception occurred. Yolo5 pod terminated", e)
