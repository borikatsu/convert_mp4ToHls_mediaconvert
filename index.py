#coding: UTF-8
import os
import logging
import urllib.parse
import boto3

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# MediaConvertのエンドポイントURL
MEDIACONVERT_ENDPOINT = os.environ['MEDIACONVERT_ENDPOINT']

# MediaConvert用のロールのARN
MEDIACONVERT_ROLE = os.environ['MEDIACONVERT_ROLE']

# 利用するジョブテンプレートの名前
JOB_TEMPLATE_NAME = os.environ['JOB_TEMPLATE_NAME']

def lambda_handler(event, context):
    # イベントから受け取るアップロードされたObjectとバケット
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')

    # MediaConvertのテンプレートに対して設定を上書く
    settings = make_settings(bucket, key)
    user_metadata = {
        'JobCreatedBy': 'Lambda',
    }

    # ジョブ予約
    client = boto3.client('mediaconvert', endpoint_url = MEDIACONVERT_ENDPOINT)
    result = client.create_job(
        Role         = MEDIACONVERT_ROLE,
        JobTemplate  = JOB_TEMPLATE_NAME,
        Settings     = settings,
        UserMetadata = user_metadata,
    )

    logger.info(str(result))

def make_settings(bucket, key):
    # アップロードされたファイルのID取得のために分割
    basename = os.path.basename(key).split('.')[0]
    keys = key.split('/')

    return {
        'Inputs': [
            {
                'FileInput': f"s3://{bucket}/{key}",
            }
        ],
        'OutputGroups': [
            {
                'Name': 'Apple HLS',
                'OutputGroupSettings': {
                    'Type': 'HLS_GROUP_SETTINGS',
                    'HlsGroupSettings': {
                        'Destination': f"s3://{bucket}/uploads/contents/{keys[1]}/hls/",
                    },
                },
            },
        ],
    }
