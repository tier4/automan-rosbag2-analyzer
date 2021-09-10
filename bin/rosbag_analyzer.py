#!/usr/bin/env python
import argparse
import json
import os
from rosbag.bag import Bag
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '../libs'))
from core.storage_client_factory import StorageClientFactory
from core.automan_client import AutomanClient
import yaml

MSG_DATA_TYPE_MAP = {
    'sensor_msgs/msg/CompressedImage': 'IMAGE',
    'sensor_msgs/msg/Image': 'IMAGE',
    'sensor_msgs/msg/PointCloud2': 'PCD'
}


class RosbagAnalyzer(object):

    @classmethod
    def analyze(cls, file_path, metadata_path, label_type):
        try:
            with open(metadata_path) as f:
                obj = yaml.safe_load(f)
                info = obj['rosbag2_bagfile_information']

            dataset_candidates = []
            for topic in info['topics_with_message_count']:
                meta = topic['topic_metadata']
                frame_count = topic['message_count']
                topic_type = meta['type']
                if topic_type  in MSG_DATA_TYPE_MAP.keys():
                    candidate = {
                        "analyzed_info": {
                            "topic_name": meta['name'],
                            "msg_type": topic_type,
                        },
                        "data_type": MSG_DATA_TYPE_MAP[topic_type],
                        "frame_count": frame_count
                    }
                    dataset_candidates.append(candidate)

            if cls.__is_label_type_valid(dataset_candidates, label_type):
                return dataset_candidates, 'analyzed'
            return [], 'invalid'
        except Exception as e:
            # FIXME
            raise(e)

    @classmethod
    def __is_label_type_valid(cls, candidates, label_type):
        if label_type == 'BB2D':
            return cls.__has_type(candidates, 'IMAGE')
        elif label_type == 'BB2D3D':
            return cls.__has_type(candidates, 'IMAGE') & cls.__has_type(candidates, 'PCD')

    @staticmethod
    def __has_type(candidates, _type):
        for candidate in candidates:
            if candidate['data_type'] == _type:
                return True
        return False


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--storage_type', required=True)
    parser.add_argument('--storage_info', required=True)
    parser.add_argument('--automan_info', required=True)
    args = parser.parse_args()

    storage_client = StorageClientFactory.create(
        args.storage_type,
        json.loads(args.storage_info)
    )
    storage_client.download()
    path = storage_client.get_local_path()
    metadata_path = storage_client.get_metadata_local_path()
    label_type = json.loads(args.automan_info)['label_type']
    results, status = RosbagAnalyzer.analyze(path, metadata_path, label_type)
    AutomanClient.send_analyzer_result(json.loads(args.automan_info), results, status)
