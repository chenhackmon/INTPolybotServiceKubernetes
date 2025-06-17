# Define required modules
import flask
from flask import request
from loguru import logger
from bot import ObjectDetectionBot
# Define aws related modules
import boto3
from botocore.exceptions import ClientError
import json
# Define polybot microservices related modules
import pymongo

app = flask.Flask(__name__)


# Code copied from aws credentials manager
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
TELEGRAM_TOKEN = secrets_dict["TELEGRAM_TOKEN"]
TELEGRAM_APP_URL = secrets_dict["TELEGRAM_APP_URL"]
IMAGES_BUCKET = secrets_dict["IMAGES_BUCKET"]
POLYBOT_QUEUE = secrets_dict["POLYBOT_QUEUE"]
# Initialize polybot microservices related variables
MONGO_URI = f'mongodb://{secrets_dict["MONGODB_HOSTS"]}/{secrets_dict["MONGODB_NAME"]}?replicaSet={secrets_dict["MONGODB_RS_NAME"]}'
mongo_client = pymongo.MongoClient(MONGO_URI)


@app.route('/', methods=['GET'])
def index():
    return 'Ok'


@app.route(f'/{TELEGRAM_TOKEN}/', methods=['POST'])
def webhook():
    req = request.get_json()
    bot.handle_message(req['message'])
    return 'Ok'


@app.route(f'/results', methods=['POST'])
def results():
    logger.info(f'Received a POST request from Yolo5...')
    # Use the prediction_id to retrieve results from MongoDB and send to the end-user
    prediction_id = request.args.get('predictionId')
    chat_id = request.args.get('chatId')

    test_db_client = mongo_client["test"]
    predictions_collection = test_db_client["predictions"]
    # Store a prediction inside the predictions collections
    document = predictions_collection.find_one({"_id": prediction_id})
    if document is not None:
        # Prediction image was created. Send the text results and image to the user
        # Format the text results :
        logger.info(f'Returning prediction summary for prediction {prediction_id} from chat id {chat_id}')
        # Generate prediction class counts, reformat and send to user
        predictions = document['prediction_summary']['labels'] # Predictions is a list of dicts representing detected objects within the image
        predictions_classes = {}
        for prediction in predictions:
            if prediction['class'] in predictions_classes:
                # This prediction class was encountered before, add 1 to the occurrence counter
                predictions_classes[prediction['class']] += 1
            else:
                # Detected object is NEW within the image. Initialize a counter for it
                predictions_classes[prediction['class']] = 1

        # Format a text message to send to the user
        predicted_object_counts = "The following objects were detected in the image :\n"
        for predicted_class, counter in predictions_classes.items():
            predicted_object_counts += f'{predicted_class} : {counter}\n'

        # Send the predicted photo to the user
        predicted_img_path = bot.download_s3_image(document['prediction_summary']['s3_img_path'])
        bot.send_photo(chat_id,predicted_img_path)
        text_results = predicted_object_counts
    else:
        logger.info(f'No prediction could be made for prediction {prediction_id} from chat id {chat_id}')
        text_results = "No prediction could be made for the given image. Please try a different image"

    bot.send_text(chat_id, text_results)
    return 'Ok'


@app.route(f'/loadTest/', methods=['POST'])
def load_test():
    req = request.get_json()
    bot.handle_message(req['message'])
    return 'Ok'


if __name__ == "__main__":
    bot = ObjectDetectionBot(TELEGRAM_TOKEN, TELEGRAM_APP_URL, IMAGES_BUCKET, POLYBOT_QUEUE)

    app.run(host='0.0.0.0', port=8443)
