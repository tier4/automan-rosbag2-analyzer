import os
import requests
from core.storages import BaseStorageClient


class S3StorageClient(BaseStorageClient):

    def __init__(self, storage_config):
        super(S3StorageClient, self).__init__(storage_config)
        os.mkdir('/s3')
        self.rosbag_path = '/s3/rosbag.db3'
        self.sub_file_path = '/s3/metadata.yaml'
        self.target_url = storage_config['target_url']
        self.sub_file_url = storage_config['sub_file_url']

    def __download_file(self, url, path):
        req = requests.get(url, stream=True)
        if req.status_code == 200:
            with open(path, 'wb') as f:
                f.write(req.content)
        else:
            print('status_code = ' + str(req.status_code))

    def download(self, url=None):
        #if url is None:
        #    url = self.target_url
        #self.__download_file(url, self.rosbag_path)

        url = self.sub_file_url
        self.__download_file(url, self.sub_file_path)

    def upload(self, path):
        pass

    def list(self):
        pass

    def get_local_path(self):
        return self.rosbag_path

    def get_metadata_local_path(self):
        return self.sub_file_path
