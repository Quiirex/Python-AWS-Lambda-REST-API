import boto3
import json
import logging
from custom_encoder import CustomEncoder

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamo_db_table_name = 'product-inventory'
dynamo_db = boto3.resource('dynamodb')
table = dynamo_db.Table(dynamo_db_table_name)

get_method = 'GET'
post_method = 'POST'
patch_method = 'PATCH'
delete_method = 'DELETE'

health_path = '/health'
product_path = '/product'
products_path = '/products'


def build_response(status_code, body=None):
    response = {
        'statusCode': status_code,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        }
    }
    if body is not None:
        response['body'] = json.dumps(body, cls=CustomEncoder)
    return response


def get_product(productId):
    try:
        response = table.get_item(
            Key={
                'productId': productId
            }
        )
        if 'Item' in response:
            return build_response(200, response['Item'])
        else:
            return build_response(404, {'Message': 'Product not found'})
    except:
        logger.exception('Exception thrown!')


def get_products():
    try:
        response = table.scan()
        result = response['Items']

        while 'LastEvaluatedKey' in response:
            response = table.scan(
                ExclusiveStartKey=response('LastEvaluatedKey'))
            result.extend(response['Items'])

        body = {
            'products': result
        }
        return build_response(200, body)
    except:
        logger.exception('Exception thrown!')


def save_product(requestBody):
    try:
        table.put_item(Item=requestBody)
        body = {
            'Operation': 'SAVE',
            'Message': 'SUCCESS',
            'Item': requestBody
        }
        return build_response(200, body)
    except:
        logger.exception('Exception thrown!')


def modify_product(productId, updateKey, updateValue):
    try:
        response = table.update_item(
            Key={
                'productId': productId
            },
            ExpressionAttributeValues={
                ':value': updateValue
            },
            ReturnValues='UPDATED_NEW'
        )
        body = {
            'Operation': 'UPDATE',
            'Message': 'SUCCESS',
            'UpdatedAttributes': response
        }
        return build_response(200, body)
    except:
        logger.exception('Exception thrown!')


def delete_product(productId):
    try:
        response = table.delete_item(
            Key={
                'productId': productId
            },
            ReturnValues='ALL_OLD'
        )
        body = {
            'Operation': 'DELETE',
            'Message': 'SUCCESS',
            'deletedItem': response
        }
        return build_response(200, body)
    except:
        logger.exception('Exception thrown!')


def lambda_handler(event, context):
    logger.info(event)
    http_method = event['httpMethod']
    path = event['path']

    if http_method == get_method and path == health_path:
        response = build_response(200)
    elif http_method == get_method and path == product_path:
        response = get_product(event['queryStringParameters']['productId'])
    elif http_method == get_method and path == products_path:
        response = get_products()
    elif http_method == post_method and path == product_path:
        response = save_product(json.loads(event['body']))
    elif http_method == patch_method and path == product_path:
        request_body = json.loads(event['body'])
        response = modify_product(
            request_body['productId'], request_body['updateKey'], request_body['updateValue'])
    elif http_method == delete_method and path == product_path:
        request_body = json.loads(event['body'])
        response = delete_product(request_body['productId'])
    else:
        response = build_response(404, 'Not found')

    return response
