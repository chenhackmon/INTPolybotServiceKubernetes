import telebot
from loguru import logger
import os
import time
from telebot.types import InputFile
import json
# Define aws related modules
import boto3


class Bot:

    def __init__(self, token, telegram_chat_url):
        # create a new instance of the TeleBot class.
        # all communication with Telegram servers are done using self.telegram_bot_client
        self.telegram_bot_client = telebot.TeleBot(token)

        # remove any existing webhooks configured in Telegram servers
        self.telegram_bot_client.remove_webhook()
        time.sleep(0.5)

        # set the webhook URL
        self.telegram_bot_client.set_webhook(url=f'{telegram_chat_url}/{token}/', timeout=60)

        logger.info(f'Telegram Bot information\n\n{self.telegram_bot_client.get_me()}')

    def send_text(self, chat_id, text):
        self.telegram_bot_client.send_message(chat_id, text)

    def send_text_with_quote(self, chat_id, text, quoted_msg_id):
        self.telegram_bot_client.send_message(chat_id, text, reply_to_message_id=quoted_msg_id)

    def is_current_msg_photo(self, msg):
        return 'photo' in msg or 'document' in msg

    def download_user_photo(self, msg):
        # Downloads the photos that are sent to the bog to 'photos' directory
        logger.info(f'Received a new photo !')
        # Get photo files depending on uploading source - phone / desktop
        if 'photo' in msg:
            file_info = self.telegram_bot_client.get_file(msg['photo'][-1]['file_id'])
        elif 'document' in msg:
            file_info = self.telegram_bot_client.get_file(msg['document']['thumbnail']['file_id'])
        photo_caption = msg['caption'] if 'caption' in msg else None
        data = self.telegram_bot_client.download_file(file_info.file_path)
        folder_name = file_info.file_path.split('/')[0]
        # Add informative logs regarding files' metadata
        logger.info(f'Downloaded received photo to the following path : {file_info.file_path}')

        if not os.path.exists(folder_name):
            os.makedirs(folder_name)

        with open(file_info.file_path, 'wb') as photo:
            photo.write(data)  # Actually write the photo data to the file_path

        return file_info.file_path, photo_caption

    def send_photo(self, chat_id, img_path):
        if not os.path.exists(img_path):
            raise RuntimeError("Image path doesn't exist")

        self.telegram_bot_client.send_photo(
            chat_id,
            InputFile(img_path)
        )

    def handle_message(self, msg):
        """Bot Main message handler"""
        logger.info(f'Incoming message: {msg}')
        self.send_text(msg['chat']['id'], f'Your original message: {msg["text"]}')


class ObjectDetectionBot(Bot):
    def __init__(self, token, telegram_chat_url, images_bucket, polybot_queue):
        super().__init__(token, telegram_chat_url)
        # Add specific implementation code :
        # Initialize s3 related variables
        self.s3_client = boto3.client('s3')
        # Initialize sqs related variables
        self.sqs_client = boto3.client('sqs', region_name="eu-north-1")
        self.sqs_name = polybot_queue
        # Initialize s3 related variables
        self.images_bucket = images_bucket
        self.about_message = """
Barrotem's polybot-k8s service
---------------------------------------------
Hello there, thank you for using me !
I am a Telegram bot able to perform object detection and classification on images that you send me.

To interact with me, simply attach an image.
I will return the classified image with the detected objects, and textual representation.
        """

    def handle_message(self, msg):
        logger.info(f'Incoming message: {msg}')
        chat_id = msg['chat']['id']

        if self.is_current_msg_photo(msg):
            #Download the photo to local pod storage
            photo_path, photo_caption = self.download_user_photo(msg)

            # Handle image filename
            supported_image_formats = ['bmp', 'dng', 'jpeg', 'jpg', 'mpo', 'png', 'tif', 'tiff', 'webp']
            s3_photo_key = f'images/{photo_caption}' if photo_caption is not None else f'images/{photo_path.split("/")[1]}'
            # Check the photo's file extension. Blit .jpg if none - existent
            s3_photo_key_file_extension = s3_photo_key.split(".")[-1]
            s3_photo_key = s3_photo_key if s3_photo_key_file_extension in supported_image_formats else s3_photo_key + '.jpg'
            logger.info(f'Set received photo key to : {s3_photo_key}')

            # Upload the photo to S3
            self.s3_client.upload_file(Filename=photo_path, Bucket=self.images_bucket, Key=s3_photo_key)
            logger.info(
                f'Successfully uploaded {photo_path} to "{self.images_bucket}" with the caption "{s3_photo_key}"')

            # Send a job to the SQS queue
            sqs_message_body = {"text": "A new image was uploaded to the s3 bucket", "img_name": s3_photo_key,
                                "chat_id": chat_id}
            sqs_message_body = json.dumps(sqs_message_body)
            logger.info(f'Sending a job to sqs')
            self.sqs_client.send_message(QueueUrl=self.sqs_name, MessageBody=sqs_message_body)

            # Send message to the Telegram end-user, to be notified of image processing
            self.send_text(chat_id, "Image received and is being processed. Please wait...")

        else:
            # Message doesn't contain an image
            self.send_text(chat_id, self.about_message)

    def download_s3_image(self, s3_img_path):
        # Download image from s3
        folder_name = s3_img_path.split('/')[0]
        if not os.path.exists(folder_name):
            os.makedirs(folder_name)

        self.s3_client.download_file(Bucket=self.images_bucket, Key=s3_img_path, Filename=s3_img_path)
        return s3_img_path
