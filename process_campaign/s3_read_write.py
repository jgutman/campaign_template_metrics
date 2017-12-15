from io import BytesIO, StringIO
import pandas as pd
import boto3
import os, logging


class S3ReadWrite:

    def __init__(self, bucket, folder):
        self.client = boto3.client('s3')
        self.resource = boto3.resource('s3')
        self.folder = folder
        self.bucket = bucket

    def __str__(self):
        msg = '(folder: {}, bucket: {})'.format(
            str(self.folder), str(self.bucket))
        return msg

    def __eq__(self, other):
        if self.bucket == other.bucket and self.folder == other.folder:
            return True
        else:
            return False

    @property
    def client(self):
        return self._client

    @client.setter
    def client(self, new_client):
        self._client = new_client

    @property
    def resource(self):
        return self._resource

    @resource.setter
    def resource(self, new_resource):
        self._resource = new_resource

    @property
    def folder(self):
        return self._folder

    @folder.setter
    def folder(self, new_folder):
        self._folder = new_folder

    @property
    def bucket(self):
        return self._bucket

    @bucket.setter
    def bucket(self, new_bucket):
        self._bucket = new_bucket

    def read_from_S3_csv(self, csv_name, **read_csv_kwargs):
        value = self.client.get_object(
                Bucket = self.bucket,
                Key ='{folder}/{csv_path}/{csv_name}.csv'.format(
                        folder = self.folder,
                        csv_path = csv_path,
                        csv_name = csv_name)
                )['Body'].read()
        return pd.read_csv(BytesIO(value),**read_csv_kwargs)

    def read_from_S3_csv_with_path(self, csv_path, csv_name):
        value = self.client.get_object(
                Bucket=self.bucket,
                Key= '{folder}/{csv_path}/{csv_name}.csv'.format(
                        folder = self.folder,
                        csv_path = csv_path,
                        csv_name = csv_name)
                )['Body'].read()
        return pd.read_csv(BytesIO(value))

    def put_dataframe_to_S3(
            self,
            csv_path,
            csv_name,
            dataframe):
        csv_buffer = StringIO()
        dataframe.to_csv(csv_buffer, index=False, header=True)
        self.resource.Bucket(
            self.bucket).put_object(
            Key= '{folder}/{csv_path}/{csv_name}.csv'.format(
                folder = self.folder,
                csv_path = csv_path,
                csv_name = csv_name),
            Body=csv_buffer.getvalue())

    def put_to_S3(self, key, body):
        self.resource.Bucket(
            self.bucket).put_object(
            Key=self.folder +
            key,
            Body=body)

    def append_to_csv(self, dataframe, csv_name):
        try:
            data = pd.concat([self.read_from_S3_csv(
                csv_name), dataframe], ignore_index=True)
        except Exception as e:
            data = dataframe

        csv_buffer = StringIO()
        data.to_csv(csv_buffer, index=False)
        self.resource.Bucket(
            self.bucket).put_object(
            Key='{folder}/{csv_name}.csv'.format(
                folder = self.folder,
                csv_name = csv_name),
            Body=csv_buffer.getvalue())
