from fastapi import Body, FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

from starlette.responses import Response

import settings

import random
import string
import time

# S3
import boto3
from botocore.config import Config
from mypy_boto3_s3.client import S3Client

# Init API
app = FastAPI(
    title='Demo S3 MultiPart Uploads API',
    version='1.0.0'
)

s3_client: S3Client = {}

@app.on_event("startup")
async def startup_event():
    global s3_client
    client_config = Config(
        signature_version='v4'
    )
    s3_session = boto3.session.Session(aws_access_key_id=settings.S3_ACCESS_KEY,
                                       aws_secret_access_key=settings.S3_SECRET_KEY,
                                       region_name=settings.S3_REGION)
    
    s3_client = s3_session.client('s3', config = client_config)

@app.get('/')
async def read_root():
    """
    Test endpoint.

    Returns:
        dict: Hello world
    """
    return {'Hello': 'World'}

@app.get('/uploads/start')
async def initiate_multipart_upload(fileName: str):
    """Initiate a MultiPart upload and return the upload ID.

    Args:
        fileName (str): The name of the file. This should include the whole path inside your bucket.

    Returns:
        str: Upload identifier for the created MultiPart upload.
    """

    upload_response = s3_client.create_multipart_upload(
        Bucket = settings.S3_BUCKET,
        Key = fileName
    )
   
    upload_id = upload_response['UploadId']
    
    return upload_id

@app.get('/uploads/sign/part')
async def sign_upload_url(fileName: str, uploadId: str, partNumber: int):
    """Get a presigned url for a part of a MultiPart upload.

    Args:
        fileName (str): The name of the file. This should include the whole path inside your bucket.
        uploadId (str): The identifier of the MultiPart upload.
        partNumber (int): The number of the part.

    Returns:
        str: Presigned url for the current part.
    """
    print('Signing url for part', i, 'for multipart upload with ID:', uploadId)

    presigned_url = s3_client.generate_presigned_url(
        ClientMethod = 'upload_part',
        Params = {
            'Bucket': settings.S3_BUCKET,
            'Key': fileName,
            'UploadId': uploadId,
            'PartNumber': partNumber,
            }
    )
    return presigned_url

@app.post('/uploads/complete')
async def complete_multipart_upload(fileName: str,  uploadId: str, parts: list = Body(...)):
    """Complete the MultiPart upload once every part has been uploaded succesfully.

    Args:
        fileName (str): The name of the file. This should include the whole path inside your bucket.
        uploadId (str): The identifier of the MultiPart upload.
        parts (list): An array containing the information about the parts uploaded.

    Returns:
        str: The url of the resource.
    """
    print('Completed multipart upload with ID:', uploadId)

    response = s3_client.complete_multipart_upload(
        Bucket = settings.S3_BUCKET,
        Key = fileName,
        MultipartUpload = {'Parts': parts},
        UploadId = uploadId
    )    
    return response['Location']

@app.post('/uploads/abort')
async def abort_multipart_upload(fileName: str, uploadId: str):
    """Abort a MultiPart upload to free up S3 resources in case of a problem.

    Args:
        fileName (str): The name of the file. This should include the whole path inside your bucket.
        uploadId (str): The identifier of the MultiPart upload.
    """

    print('Aborting multipart upload with ID:', uploadId)
    
    response = s3_client.abort_multipart_upload(
        Bucket = settings.S3_BUCKET,
        Key = fileName,
        UploadId = uploadId
    )
        
    return {'status': 'OK'}


@app.middleware("http")
async def log_requests(request: Request, call_next):
    """
    Log every request with a unique Request id and, timestamp and time to completion.

    Args:
        request (Request): The incoming request to be intercepted by the middleware
        call_next (_type_): Passes the request to the following step

    Returns:
        Response: Returns the response the incoming request generated
    """
    idem = ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))
    logger.info(f"rid={idem} start request path={request.url.path}")
    start_time = time.time()
    
    response = await call_next(request)
    
    process_time = (time.time() - start_time) * 1000
    formatted_process_time = '{0:.2f}'.format(process_time)
    logger.info(f"rid={idem} completed_in={formatted_process_time}ms status_code={response.status_code}")
    
    return response

