import concurrent
import time
import pandas as pd

start_time = time.time()
print(f"""start time  {start_time}""")

import boto3
import botocore

client_region = boto3.Session().region_name

source_object_key = "*** Source object key ***"
dest_object_key = "xx"
source_bucket_name = '<source-account-bucket-name>'
dest_bucket_name = '<destination-account-bucket-name>'



# Create a boto3 session
session = boto3.Session(
    aws_access_key_id='xxxxxx',
    aws_secret_access_key='xxxxxx',
    region_name='eu-west-1'
)

# Connect to the S3 service
s3_client = session.client('s3')


def multipart_upload(src_key,processed):
    global response
    src_file = src_key
    dest_object_key = src_file
    init_result = s3_client.create_multipart_upload(Bucket=dest_bucket_name, Key=dest_object_key)
    print(f""" src_file {src_file}""")
    # Get the object size to track the end of the copy operation
    metadata_result = s3_client.head_object(Bucket=source_bucket_name, Key=src_file)
    object_size = metadata_result['ContentLength']
    print(f""" object_size {object_size}""")
    # Copy the object using 5 MB parts
    part_size = 25 * 1024 * 1024
    byte_position = 0
    part_num = 1
    etags = []
    copy_responses = []
    while byte_position < object_size:
        #     # The last part might be smaller than part_size, so check to make sure that last_byte isn't beyond the end of the object
        last_byte = min(byte_position + part_size - 1, object_size - 1)
        #
        #     # Copy this part
        copy_result = s3_client.upload_part_copy(Bucket=dest_bucket_name,
                                                 CopySource={"Bucket": source_bucket_name, "Key": src_file},
                                                 Key=dest_object_key, UploadId=init_result['UploadId'],
                                                 PartNumber=part_num,
                                                 CopySourceRange=f'bytes={byte_position}-{last_byte}')
        copy_responses.append(copy_result)
        byte_position += part_size
        etags.append({'ETag': copy_result['CopyPartResult']['ETag'], 'PartNumber': part_num})
        part_num += 1
    # Complete the upload request to concatenate all uploaded parts and make the copied object available
    for response in etags:
        print(f""" response{response}""")
    s3_client.complete_multipart_upload(Bucket=dest_bucket_name, Key=dest_object_key, UploadId=init_result['UploadId'],
                                        MultipartUpload={"Parts": etags})
    processed.append(src_file)
    print("Multipart copy complete.")


try:
    

    def retrieve_keys(bucket_name, prefix,continuation_token=None):
        keys = []
        kwargs = {'Bucket': bucket_name, 'Prefix': f"{prefix}"}
        if continuation_token:
            kwargs['ContinuationToken'] = continuation_token
        while True:
            response = s3_client.list_objects_v2(**kwargs)
            keys.extend([content['Key'] for content in response.get('Contents', [])])
            if not response.get('IsTruncated'):
                break
            kwargs['ContinuationToken'] = response.get('NextContinuationToken')
        return keys

    result = retrieve_keys(bucket_name=source_bucket_name, prefix="as-your-wish-prefix")

    processed = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=7) as executor:
        # Submit a task for each object in the source bucket
        futures = [executor.submit(multipart_upload, obj,processed) for obj in result ]
        # Wait for all tasks to complete
        concurrent.futures.wait(futures)
    df = pd.DataFrame(processed)
    df.to_csv("checkit")
    end_time =time.time() - start_time
    print(f"""end time  {end_time}""")

except botocore.exceptions.ClientError as e:
    print(f"Error: {e}")
