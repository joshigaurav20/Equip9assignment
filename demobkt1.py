import os
import json
import boto3
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Create S3 client
s3 = boto3.client("s3")

def list_s3_contents(bucket_name, prefix=""):
    """
    List the contents of an S3 bucket at the specified path.
    If no path is provided, it lists the top-level contents.
    
    :param bucket_name: The name of the S3 bucket.
    :param prefix: The path prefix to list contents for.
    :return: A dictionary containing directories and files.
    """
    # Add trailing slash to prefix if it's a directory
    if prefix and not prefix.endswith("/"):
        prefix += "/"
    
    # List objects within the specified path
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix, Delimiter='/')
    
    # Initialize lists for directories and files
    directories = []
    files = []
    
    # Add common prefixes (subdirectories) to the directory list
    if 'CommonPrefixes' in response:
        for dir_prefix in response['CommonPrefixes']:
            directories.append(dir_prefix['Prefix'].replace(prefix, '').rstrip('/'))
    
    # Add files to the file list
    if 'Contents' in response:
        for obj in response['Contents']:
            key = obj['Key'].replace(prefix, '')
            if key:  # Only add if it's not an empty key (which happens if Prefix matches exactly)
                files.append(key)
    
    # Return the result in JSON format
    return {
        "content": directories + files
    }

# Example usage:
bucket_name = "demobucket390"
# Specify the path, e.g., "dir2/" or leave it empty for top-level
path = "dir1/"  # Change this to the path you want to list or "" for top-level

# List the contents of the bucket for the specified path
contents = list_s3_contents(bucket_name, prefix=path)

# Print the result in JSON format
print(json.dumps(contents, indent=4))
