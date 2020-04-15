import os
import glob
from dotenv import find_dotenv
import lzma
import json
import logging as log
import re
import pandas as pd
from datetime import datetime


project_dir = find_dotenv().split('.')[0]
data_dir = os.path.join(project_dir, 'data')
raw_data_dir = os.path.join(data_dir, 'raw')
processed_data_dir = os.path.join(data_dir, 'processed')
os.chdir(project_dir)


class post:
    """An Instagram post.

    Attributes:
        id: An integer representing the a single post.
        likes: Number of likes the post received.
        comments: Number of comments on the post.
        timestamp: Date and time the post was made.
        caption: Picture(s) caption for the post.
        hashtags: Hashtags in the post caption.
        pic_dim: Height and width of the first picture in the post.
        pic_url: URL of the first picture or video in the post.
    """

    
    def __init__(self, node):
        self.id = int(node['id'])
        self.likes = int(node['edge_media_preview_like']['count'])
        self.comments = int(node['edge_media_to_comment']['count'])
        self.timestamp = datetime.fromtimestamp(int(node['taken_at_timestamp']))
        
        def get_caption(node):
            try:
                node_caption = node['edge_media_to_caption']['edges'][0]['node']['text']
            except:
                node_caption = ''
            return node_caption
        self.caption = get_caption(node)
        
        self.hashtags = re.findall(r'(?<=#)\w+', self.caption)
        self.pic_dim = node['dimensions']
        self.pic_url = node['display_url']
        

def json_to_str(file):

    """Converts the json.xz file downloaded for each post from
    Instagram and converts it into a dictionary that can
    be interpreted by the post class.
    """
    
    bytes_data = lzma.open(file).read()
    json_str = bytes_data.decode('utf-8')
    post_str = json.loads(json_str)
    return post_str


def json_files_to_csv(input_dir, output_dir, filename):
    """Combines metadata in json.xz files into one csv file.

    Arguments:
        input_dir: Directory where json.xz files live.
        output_dir: Directory to save csv file to.
        filename: Name of csv output file.
    """

    all_posts_df = pd.DataFrame()
    for file in glob.glob(os.path.join(input_dir, '*.json.xz')):
        post_str = json_to_str(file)
        if post_str['instaloader']['node_type'] == 'Post':
            node_info = post_str['node']
            post_dict = post(node_info).__dict__
            post_df = pd.DataFrame([post_dict])
            all_posts_df = pd.concat([all_posts_df,post_df], axis=0)
    all_posts_df.reset_index(inplace=True, drop=True)
    all_posts_df['date'] = all_posts_df['timestamp'].dt.date

    output_path = os.path.join(output_dir, filename)
    all_posts_df.to_csv(output_path, index=False)


def main():
    json_files_to_csv(input_dir=raw_data_dir,
                      output_dir=processed_data_dir,
                      filename='cattownposts.csv')


if __name__ == "__main__":
    log.basicConfig(level=log.DEBUG,
                    format='%(asctime)s %(message)s',
                    datefmt="%b %d %H:%M:%S %Z")
    main()