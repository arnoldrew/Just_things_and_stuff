import requests
import boto3
import io
import logging
from botocore.exceptions import ClientError

# Creates a bucket
def createbucket (name, region):
    
    try: 
        s3_client = boto3.client ('s3', region_name=region)
        location = {'LocationConstraint': region}
        s3_client.create_bucket(Bucket=name, CreateBucketConfiguration=location)   
    except ClientError as e:
        logging.error(e)
        return False
    
    return True

# Lists all buckets
def listbuckets ():
    try:        
        s3_client = boto3.client('s3')        
        response = s3_client.list_buckets()    
    except ClientError as e:        
        logging.error(e)        
        return False   
        
    # Output the bucket names
    for bucket in response['Buckets']:        
            print(f'  {bucket["Name"]}')    
    
    return True

# Uploads a file
def uploadfile(bucket_name, file_name, object_name):
    try:        
        s3_client = boto3.client('s3')        
        s3_client.upload_file(file_name, bucket_name, object_name)    
    except ClientError as e:        
        logging.error(e)        
        return False    
    
    return True

# Lists all files in a bucket
def listfiles(bucket_name):
    try:        
        s3_client = boto3.client('s3')        
        paginator = s3_client.get_paginator("list_objects_v2")        
        for page in paginator.paginate(Bucket=bucket_name):            
            for content in page["Contents"]:                
                key = content['Key']                    
                print(key)    
    except ClientError as e:        
        logging.error(e)        
        return False    
        
    return True

# Deletes a file
def deletefile(bucket_name, object_name):
    try:        
        s3_client = boto3.client('s3')        
        s3_client.delete_object(Bucket=bucket_name, Key=object_name)    
    
    except ClientError as e:        
        logging.error(e)        
        return False    
    
    return True 

# Deletes a bucket
def deletebucket(bucket_name):
    try:        
        s3_client = boto3.client('s3')        
        s3_client.delete_bucket(Bucket=bucket_name)    
    
    except ClientError as e:        
        logging.error(e)        
        return False    
    
    return True

# Glues everything together. Allows the user to choose from a variety of S3 functions.
def main():
    choice = int
    bucket = str
    file_name = str
    obj_name = str
    onoff = "yes"
    success = bool

    # This will list the user's choices and run whichever function they choose, then ask if they wish to run another function.
    while onoff == "yes":
        print("Please select from the following choices:")
        print("1. Create bucket")
        print("2. List all buckets")
        print("3. Upload a file")
        print("4. List all files in a bucket")
        print("5. Delete a file")
        print("6. Delete a bucket")
        choice = int(input("Make your selection (1-6): "))

        if choice == 1:
            bucket = input("what do you want to call your bucket?")
            success = createbucket(bucket, "us-east-2")
            
            if success == True:
                print("It worked!")
            else:
                print("It did not work...")

        elif choice == 2:
            success = listbuckets()

            if success == True:
                print("It worked!")
            else:
                print("It did not work...")

        elif choice == 3:
            bucket = input("What bucket do you want to upload a file to? ")
            file_name = input("What file do you want to upload? ")
            obj_name = input("What do you want to name the object? ")

            success = uploadfile(bucket, file_name, obj_name)

            if success == True:
                print("It worked!")
            else:
                print("It did not work...")

        elif choice == 4:
            bucket = input("What bucket would you like to list the contents of? ")
            success = listfiles(bucket)

            if success == True:
                print("It worked!")
            else:
                print("It did not work...")

        elif choice == 5:
            bucket = input("What bucket do you want to delete a file from? ")
            obj_name = input("What file do you want to delete? ")
            
            success = deletefile(bucket, obj_name)
            
            if success == True:
                print("It worked!")
            else:
                print("It did not work...")

        elif choice == 6:
            bucket = input("What bucket do you want to delete? ")

            success = deletebucket(bucket)    

            if success == True:
                print("It worked!")
            else:
                print("It did not work...")

        else:
            print("That is not a valid choice")

        onoff = input("Do you wish to do something else (yes or no)? ")


main()