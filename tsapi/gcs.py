from google.cloud import storage
from datetime import timedelta


def generate_signed_url(bucket_name, blob_name, expiration_minutes=15):
    """
    Generate a signed URL for a blob in Google Cloud Storage.

    :param bucket_name: The name of the bucket.
    :param blob_name: The name of the blob.
    :param expiration_minutes: The duration in minutes for which the signed URL should be valid.
    :return: The signed URL as a string.
    """
    # Initialize a client for Google Cloud Storage
    client = storage.Client()

    # Get the bucket that the blob is a part of
    bucket = client.bucket(bucket_name)

    # Get the blob from the bucket
    blob = bucket.blob(blob_name)

    # Generate a signed URL for the blob
    signed_url = blob.generate_signed_url(
        version='v4',
        expiration=timedelta(minutes=expiration_minutes),
        method='PUT',
        content_type="application/octet-stream"
    )

    return signed_url
