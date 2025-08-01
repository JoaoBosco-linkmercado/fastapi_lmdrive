import boto3
from pathlib import Path
import unicodedata

def remove_accents(input_str):
    # Normalize the string to decompose combined characters into base characters and diacritics
    normalized_str = unicodedata.normalize('NFD', input_str)
    
    # Filter out the diacritics
    return ''.join(char for char in normalized_str if unicodedata.category(char) != 'Mn')

#boto3.set_stream_logger('')

def create_client(type:str,endpoint_url:str, access_key_id:str, secret_access_key:str) -> boto3.client:
    if type == 'iam':
        return boto3.client('iam',
                        endpoint_url = endpoint_url,
                        aws_access_key_id = access_key_id,
                        aws_secret_access_key = secret_access_key,
                        region_name = 'us-east-1')
    elif type == 's3':
        return boto3.client('s3',
                        endpoint_url = endpoint_url,
                        aws_access_key_id = access_key_id,
                        aws_secret_access_key = secret_access_key)
    else:
        return None

def list_buckets(s3_client:boto3.client) -> list:
    return s3_client.list_buckets().get('Buckets')


def list_policies(iam_client:boto3.client, scope:str='Local') -> dict:
    return iam_client.list_policies(Scope=scope)


def attach_user_policy(iam_client:boto3.client, user_name:str, policy_arn) -> None:
    return iam_client.attach_user_policy(UserName=user_name, PolicyArn=policy_arn)


def get_object(s3_client:boto3.client, bucket_name:str, bucket_dir, filename) -> dict:
    try:
        return s3_client.get_object(Bucket=bucket_name, Key=bucket_dir + filename)
    except:
        return None


def put_object(s3_client:boto3.client, bucket_name:str, key_name:str, body:any=None) -> dict:
    if body:
        return s3_client.put_object(Bucket=bucket_name, Key=key_name, Body=body)
    else:
        return s3_client.put_object(Bucket=bucket_name, Key=key_name)


def delete_object(s3_client:boto3.client, bucket_name:str, key_name:str) -> dict:
    return s3_client.delete_object(Bucket=bucket_name, Key=key_name)


def delete_objects(s3_client:boto3.client, bucket_name:str, del_objects) -> dict:
    if del_objects:
        return s3_client.delete_objects(Bucket=bucket_name, Delete={'Objects': del_objects, 'Quiet': False})
    return dict()


def copy_objects(s3_client:boto3.client, bucket_name_from:str, bucket_name_to:str, from_object:str, to_object:str) -> dict:
    return s3_client.copy_object(
                        Bucket=bucket_name_to,
                        Key=to_object,
                        CopySource={
                            'Bucket': bucket_name_from,
                            'Key': from_object
                        }
                    )

def donwload_file(s3_client:boto3.client, bucket_name, bucket_dir, file_dir, filename) -> None:
    try:
        s3_client.download_file(Filename=file_dir + filename, Bucket=bucket_name ,Key=bucket_dir + filename)
    except:
        # file not found
        print(f"{bucket_dir}{filename} not found")
        pass


def upload_file(s3_client:boto3.client, bucket_name, bucket_dir, file_dir, filename) -> None:
    s3_client.upload_file(Filename=file_dir + filename,Bucket=bucket_name,Key=bucket_dir + filename)
    #with open(file_dir + filename, "rb") as f:
    #    s3_client.upload_fileobj(f, bucket_name, bucket_dir + filename)


def list_objects(s3_client:boto3.client, bucket_name:str, root:str, folder_name:str='/') -> list:
    def ensure_folder_ends(folder_name:str) -> str: 
       # Ensure the folder name ends with a '/'
        if folder_name:
            return folder_name if folder_name.endswith('/') else (folder_name + '/')
        else:
            return folder_name
        
    folder_name = ensure_folder_ends(folder_name)  
    if folder_name != '/':
        prefix = f"{ensure_folder_ends(root)}{ensure_folder_ends(folder_name)}"
    else:
        prefix = ensure_folder_ends(root)
    return s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)


def list_folder_contents(s3_client:boto3.client, bucket_name:str, root:str, folder_name:str='/') -> list:
    objects = []
    kwargs = {'Bucket': bucket_name}
    if folder_name != '/':
        kwargs['Prefix'] = root + '/' + folder_name
    else:
        kwargs['Prefix'] = root + '/'
    folder_name = kwargs['Prefix']

    kwargs['Delimiter'] = '/'
    existe_folder = False
    while True:
        resp = s3_client.list_objects_v2(**kwargs)
        # files
        for obj in resp.get('Contents',[]):
            existe_folder = True
            if obj['Key'] != folder_name:
                objects.append({ 'obj':obj['Key'], 'name':Path(obj['Key']).stem, 'type':Path(obj['Key']).suffix[1:], 'size':obj['Size'], 'isdir':False, 'modified':obj['LastModified']})
        # subdir
        for obj in resp.get('CommonPrefixes',[]):
            existe_folder = True
            if obj['Prefix'] != folder_name:
                objects.append({ 'obj':obj['Prefix'], 'name':obj['Prefix'][len(folder_name) if folder_name else 0:-1], 'isdir':True })
              
        try:
            kwargs['ContinuationToken'] = resp['NextContinuationToken']
        except KeyError:
            break

    if existe_folder:
        objects.append({ 'obj':folder_name, 'name':'.', 'isdir':True })
        if root != folder_name and folder_name:
            objects.append({ 'obj':folder_name[:folder_name[:-1].rfind('/')+1], 'name':'..', 'isdir':True })

    return objects

def get_tags(s3_client:boto3.client, bucket_name:str, file_path:str) -> list:
    resp = s3_client.get_object_tagging(Bucket=bucket_name, Key=file_path)
    return resp.get('TagSet', [])


def get_tag(s3_client:boto3.client, bucket_name:str, file_path:str, tag_key_to_query:str=None) -> str:
    tags = get_tags(s3_client, bucket_name, file_path)
    for tag in tags:
        if tag['Key'] == tag_key_to_query:
            return tag['Value']
    return ""


def put_tag(s3_client:boto3.client, bucket_name:str, file_path:str, key:str, value:str) -> dict:
    tags = {
        'TagSet': [
            {
                'Key': key,
                'Value': remove_accents(value)
            },
        ]
    }
    return s3_client.put_object_tagging(Bucket=bucket_name, Key=file_path, Tagging=tags)

def update_tag(s3_client:boto3.client, bucket_name:str, file_path:str,  tag_key_to_update:str, new_tag_value:str) -> None:
    tags = get_tags(s3_client=s3_client, bucket_name=bucket_name, file_path=file_path)
    tag_updated = False
    for tag in tags:
        if tag['Key'] == tag_key_to_update:
            tag['Value'] = remove_accents(new_tag_value)  # Update the tag value
            tag_updated = True
            break
    # If the tag was not found, you can choose to add it
    if not tag_updated:
        tags.append({'Key': tag_key_to_update, 'Value': new_tag_value})
    # Update the tags on the object
    s3_client.put_object_tagging(
        Bucket=bucket_name,
        Key=file_path,
        Tagging={'TagSet': tags}
    )