import json
from dal import dal_wasabi

def ensure_folder_ends(folder_name:str) -> str: 
       # Ensure the folder name ends with a '/'
    if folder_name:
        return folder_name if folder_name.endswith('/') else (folder_name + '/')
    else:
        return folder_name
 
def ensure_bucket_dir(parent_dir:str, folder_name:str) -> str:
    return f"{ensure_folder_ends(parent_dir)}{ensure_folder_ends(folder_name)}" if folder_name and folder_name != "/" else ensure_folder_ends(parent_dir)


aws_access_key_id= "YN0X2VVQ3TGFJSSTECKF"
aws_secret_access_key = "GYI3rWenpq7eL0UXJSuKaAyOcLfzBnehebINVP5Y"

IAM = dal_wasabi.create_client('iam',
                endpoint_url = 'https://iam.wasabisys.com',
                access_key_id = aws_access_key_id,
                secret_access_key = aws_secret_access_key)

S3 = dal_wasabi.create_client('s3',
                endpoint_url = 'https://s3.wasabisys.com',
                access_key_id = aws_access_key_id,
                secret_access_key = aws_secret_access_key)

class Wasabi(object): 

    bucket_root = 'linkm'
    
    def __init__(self, root:str=""):
        self.root = root
        self.root_dir = ensure_folder_ends(self.root)
        self.client_policy_name =f"{self.root}-ÁreaDoCliente" if '/ÁreaDoCliente/' not in self.root_dir else None
        self.iam = IAM
        self.s3 = S3
        
    def initializa_folder(self):  
        if self.root:
            # certifica que folder root existe
            dirlist = dal_wasabi.list_folder_contents(self.s3, bucket_name=self.bucket_root, root=self.root)
            if not dirlist:
                dal_wasabi.put_object(self.s3, bucket_name=self.bucket_root, key_name=self.root_dir)
                if '/ÁreaDoCliente/' not in self.root_dir:
                    self.create_folder('ÁreaDoCliente/')

    def list_folder(self, folder_name:str) -> list:
        lista = dal_wasabi.list_folder_contents(self.s3, bucket_name=self.bucket_root, root=self.root, folder_name=ensure_folder_ends(folder_name))
        for l in lista:
            l['user'] = dal_wasabi.get_tag(self.s3, bucket_name=self.bucket_root, file_path=l['obj'], tag_key_to_query='username') if not l['isdir'] else ''
            l['obj'] = l['obj'].replace(ensure_folder_ends(self.root),'')
        return lista
    
    def create_folder(self, subfolder_path:str):
        # Create an empty object with the subfolder key
        dal_wasabi.put_object(self.s3, bucket_name=self.bucket_root, key_name=ensure_bucket_dir(self.root, subfolder_path))

    def delete_folder(self, subfolder_path:str):
        deletados = []
        dir_content = self.list_folder(subfolder_path)
        files_to_delete = [{'Key': f"{self.root}/{obj['obj']}"} for obj in dir_content if not obj.get('isdir',False)]
        r = dal_wasabi.delete_objects(self.s3, bucket_name=self.bucket_root, del_objects=files_to_delete)
        if r:
            deletados.extend(r['Deleted'])
        folders_to_delete = [{'Key': obj['obj']} for obj in dir_content if obj.get('isdir',False) and obj['obj'].startswith(subfolder_path) and obj['obj'] != subfolder_path]
        for f in folders_to_delete:
            deletados.append(self.delete_folder(f['Key']))
        dal_wasabi.delete_object(self.s3, bucket_name=self.bucket_root, key_name=ensure_bucket_dir(self.root, subfolder_path))
        return deletados
    
    def move_folder(self, old_subfolder_path:str, new_subfolder_path:str) -> bool:
        def new_root_folder(object_list, old_dir, new_dir):
            for f in object_list['Contents']:
                if f['Size'] == 0:
                    folder_name = str(f['Key'])
                    folder_name = folder_name.replace(old_dir, new_dir)
                    x = dal_wasabi.put_object(s3_client=self.s3, bucket_name=self.bucket_root, key_name=folder_name)
                    print(x)

        def move_files(object_list, old_dir, new_dir):
            for f in object_list['Contents']:
                old_file = str(f['Key'])
                if f['Size'] > 0 and old_file.__contains__(old_dir):
                    file_name = old_file.replace(old_dir, new_dir)
                    x = dal_wasabi.copy_objects(s3_client=self.s3, bucket_name_from=self.bucket_root, bucket_name_to=self.bucket_root, from_object=old_file, to_object=file_name)
                    print(x, old_file, file_name)
        
        def recursive_folder_delete(object_list, old_dir):
            for f in object_list['Contents']:
                old_file = str(f['Key'])
                old_folder = ensure_folder_ends(old_dir)

                if old_file.__contains__(old_folder):
                    dal_wasabi.delete_object(s3_client=self.s3, bucket_name=self.bucket_root, key_name=old_file)

        if old_subfolder_path != new_subfolder_path:
            objects = dal_wasabi.list_objects(self.s3, bucket_name=self.bucket_root, root=self.root_dir, folder_name=old_subfolder_path)
            new_root_folder(objects, old_subfolder_path, new_subfolder_path)
            move_files(objects, old_subfolder_path, new_subfolder_path)
            recursive_folder_delete(objects, old_subfolder_path)
        return True

    def move_file(self, origin_subfolder_path:str, dest_subfolder_path:str, filename:str, override:bool=False) -> bool:
        if origin_subfolder_path != dest_subfolder_path:
            old_file = ensure_bucket_dir(self.root, origin_subfolder_path)+filename
            new_file = ensure_bucket_dir(self.root, dest_subfolder_path)+filename
            if not override:
                # já existe o destino 
                file = dal_wasabi.get_object(s3_client=self.s3, bucket_name=self.bucket_root, bucket_dir= ensure_bucket_dir(self.root, dest_subfolder_path), filename=filename)
                if file:
                    return False
            dal_wasabi.copy_objects(s3_client=self.s3, bucket_name_from=self.bucket_root, bucket_name_to=self.bucket_root, from_object=old_file, to_object=new_file)
            dal_wasabi.delete_object(s3_client=self.s3, bucket_name=self.bucket_root, key_name=old_file)
        return True


    def rename_file(self, subfolder_path:str, filename:str, new_filename:str, override:bool=False) -> bool:
        if filename != new_filename:
            old_file = ensure_bucket_dir(self.root, subfolder_path)+filename
            new_file = ensure_bucket_dir(self.root, subfolder_path)+new_filename
            if not override:
                # já existe o destino 
                file = dal_wasabi.get_object(s3_client=self.s3, bucket_name=self.bucket_root, bucket_dir=ensure_bucket_dir(self.root, subfolder_path), filename=new_filename)
                if file:
                    return False
            dal_wasabi.copy_objects(s3_client=self.s3, bucket_name_from=self.bucket_root, bucket_name_to=self.bucket_root, from_object=old_file, to_object=new_file)
            dal_wasabi.delete_object(s3_client=self.s3, bucket_name=self.bucket_root, key_name=old_file)
        return True


    def create_s3_external_client(self, user_name:str):
        # Create the IAM user
        self.iam.create_user(UserName=user_name)
        self.iam.attach_user_policy(UserName=user_name, PolicyArn=self.get_iam_policy(self.client_policy_name))
        
    def get_iam_policy(self, policy_name:str):
        # get the policy
        try:
            # Check if the policy already exists
            existing_policies = dal_wasabi.list_policies(self.iam, scope='Local')
            for policy in existing_policies['Policies']:
                if policy['PolicyName'] == policy_name:
                    return policy['Arn']
        except Exception as e:
            print(f"Error: {e}")
        return None

    def cretate_iam_policy(self, policy_name:str, folder_name:str=None):
        # Define the policy
        policy_document = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": [ "s3:*" ],
                    "Resource": []
                }
            ]
        }
        policy_document["Statement"][0]["Resource"].append(f"arn:aws:s3:::{self.bucket_root}/{ensure_bucket_dir(self.root,folder_name)}*")
        #if folder_name:
        #    policy_document["Statement"][0]["Resource"].append(f"arn:aws:s3:::{self.bucket_root}/{ensure_folder_ends(folder_name)}*")
        #else:
        #    policy_document["Statement"][0]["Resource"].append(f"arn:aws:s3:::{self.bucket_root}/*")

        # Create or update the policy
        try:
            # Check if the policy already exists
            existing_policies = dal_wasabi.list_policies(self.iam, scope='Local')
            for policy in existing_policies['Policies']:
                if policy['PolicyName'] == policy_name:
                    # If the policy exists, attach it to the user
                    self.iam.attach_user_policy(UserName=folder_name, PolicyArn=policy['Arn'])
                    print(f"Policy '{policy_name}' already exists. Attached to user '{folder_name}'.")
                    return
            # Create a new policy if it doesn't exist
            self.iam.create_policy(
                PolicyName=policy_name,
                PolicyDocument=json.dumps(policy_document)
            )
        except Exception as e:
            print(f"Error: {e}")

    def grant_access_to_s3(self, user_name:str, bucket_name:str, folder_name:str=None):
        # Define the policy
        policy_name = f"{user_name}-S3AccessPolicy"
        policy_document = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": [ "s3:*" ],
                    "Resource": []
                }
            ]
        }
        policy_document["Statement"][0]["Resource"].append(f"arn:aws:s3:::{self.bucket_root}/{ensure_bucket_dir(self.root,folder_name)}*")
        
        #if folder_name:
        #    policy_document["Statement"][0]["Resource"].append(f"arn:aws:s3:::{bucket_name}/{ensure_folder_ends(folder_name)}*")
        #else:
        #    policy_document["Statement"][0]["Resource"].append(f"arn:aws:s3:::{bucket_name}/*")

        # Create or update the policy
        try:
            # Check if the policy already exists
            existing_policies = dal_wasabi.list_policies(self.iam, scope='Local')
            for policy in existing_policies['Policies']:
                if policy['PolicyName'] == policy_name:
                    # If the policy exists, attach it to the user
                    self.iam.attach_user_policy(UserName=user_name, PolicyArn=policy['Arn'])
                    print(f"Policy '{policy_name}' already exists. Attached to user '{user_name}'.")
                    return
            # Create a new policy if it doesn't exist
            response = self.iam.create_policy(
                PolicyName=policy_name,
                PolicyDocument=json.dumps(policy_document)
            )
            policy_arn = response['Policy']['Arn']
            # Attach the newly created policy to the user
            dal_wasabi.attach_user_policy(self.iam, user_name=user_name, policy_arn=policy_arn)
            print(f"Policy '{policy_name}' created and attached to user '{user_name}'.")
        except Exception as e:
            print(f"Error: {e}")

    def get_object(self, bucket_dir:str, file_name:str) -> dict:
        return dal_wasabi.get_object(self.s3, bucket_name=self.bucket_root, bucket_dir=ensure_bucket_dir(self.root,bucket_dir), filename=file_name)
    
    def put_object(self, bucket_dir:str, obj_name:str, obj_data:any, user:str=None) -> dict:
        resp = dal_wasabi.put_object(self.s3, bucket_name=self.bucket_root, key_name=f"{ensure_bucket_dir(self.root,bucket_dir)}{obj_name}", body=obj_data)
        if user:
            dal_wasabi.put_tag(self.s3, bucket_name=self.bucket_root, file_path=f"{ensure_bucket_dir(self.root,bucket_dir)}{obj_name}", key="username", value=user)
        return resp

    def upload(self, bucket_dir:str, file_dir:str, file_name:str):
        dal_wasabi.upload_file(self.s3, bucket_name=self.bucket_root, bucket_dir=ensure_bucket_dir(self.root,bucket_dir), file_dir=ensure_folder_ends(file_dir), filename=file_name)        

    def download(self, bucket_dir:str, file_dir:str, file_name:str):
        dal_wasabi.donwload_file(self.s3, bucket_name=self.bucket_root, bucket_dir=ensure_bucket_dir(self.root,bucket_dir), file_dir=ensure_folder_ends(file_dir), filename=file_name)        
        
    def delete(self, bucket_dir:str, file_name:str):
        dal_wasabi.delete_object(self.s3, bucket_name=self.bucket_root, key_name=f"{ensure_bucket_dir(self.root,bucket_dir)}{file_name}")

    def delete_objects(self, key_names):
        dal_wasabi.delete_objects(self.s3, bucket_name=self.bucket_root, Delete={'Objects': key_names})