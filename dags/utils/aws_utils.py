
import boto3
from datetime import datetime
import logging

def send_failure_sns_alert(context):
    logger = logging.getLogger("airflow.task")

    try:
        task_instance = context.get('task_instance')
        dag_id = context.get('dag').dag_id
        task_id = task_instance.task_id
        execution_date = context.get('execution_date')
        log_url = task_instance.log_url

        message = f"""
            Airflow Task Failed

            DAG: {dag_id}
            Task: {task_id}
            Execution Time: {execution_date}
            Log URL: {log_url}
            """

        sns = boto3.client('sns')
        topic_arn = 'arn:aws:sns:ap-south-1:781108903177:sendemail'

        sns.publish(
            TopicArn=topic_arn,
            Subject=f"Airflow Task Failure: {dag_id}.{task_id}",
            Message=message.strip()
        )

        logger.info(f"SNS alert sent for failed task: {dag_id}.{task_id}")

    except Exception as e:
        # Log the error but do not raise
        logger.error(f"Failed to send SNS alert: {str(e)}", exc_info=True)


def archive_and_clean_s3_folder(bucket_name, data_prefix, archive_base_prefix):
    s3 = boto3.client('s3')

    # Generate archive prefix with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    archive_prefix = f"{archive_base_prefix}{timestamp}/"

    print(f"Archiving from: s3://{bucket_name}/{data_prefix} to s3://{bucket_name}/{archive_prefix}")

    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket_name, Prefix=data_prefix)

    for page in pages:
        for obj in page.get('Contents', []):
            key = obj['Key']

            # Skip if it's a "folder"
            if key.endswith('/'):
                continue

            # Create archive key path by replacing data_prefix with archive_prefix
            archive_key = key.replace(data_prefix, archive_prefix, 1)

            # Copy to archive
            s3.copy_object(
                Bucket=bucket_name,
                CopySource={'Bucket': bucket_name, 'Key': key},
                Key=archive_key
            )

            # Delete original object
            s3.delete_object(Bucket=bucket_name, Key=key)
            print(f"Archived and deleted: {key} → {archive_key}")


