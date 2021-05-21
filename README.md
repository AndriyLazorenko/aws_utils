## AWS utils

### Usage

To download a file from S3:
```
s3_orm = S3ORM(
temp_folder="/home/andriy/Code/PycharmProjects/AWS_utils/resources",
bucket_name=BUCKET_NAME
)
s3_orm.download_file_from_s3(s3_filepath="filepath/on/s3")
```

### Installation

Step 0. Obtain access to AWS credentials and configure AWS CLI tools using them with the guide:

https://cloudacademy.com/blog/how-to-use-aws-cli/

Step 1. Create a conda environment from a terminal with:

```
conda create --name aws_utils python=3.8
```

Step 2. Activate the environment:
```
source activate aws_utils
```
Step 3. Install all dependencies:
```
pip install -r requirements.txt
```

