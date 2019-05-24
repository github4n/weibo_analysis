#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name:down_user_posts
   Author:jasonhaven
   date:19-4-23
-------------------------------------------------
   Change Activity:19-4-23:
-------------------------------------------------
"""
import time
import json
import math
import loguru
import random
import requests
import pymongo
from tqdm import tqdm

logger = loguru.logger

headers = {
    'User-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36',
    'Host': 'm.weibo.cn',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3',
    'Accept-Encoding': 'gzip, deflate, br',
    'Referer': 'https://m.weibo.cn',
    'Cookie': "SUB=_2A25xuF6_DeRhGeFO71YT8SbEyj6IHXVTQ2L3rDV6PUJbkdAKLU39kW1NQX7CgYrX60fGGtm1i6iemR52JSQlwv7f; SUHB=0rrrYEQIdfaXig; SCF=AqEMR7DjEq8IoKbjhAVfKBVnwlb_liwr_1DYNDiBOupIChRW9rUU9-vfXrhq477vr0qbs6SINikWRMZW9b2MFRQ.; _T_WM=28248364669; MLOGIN=1; XSRF-TOKEN=0cf891; WEIBOCN_FROM=1110006030; M_WEIBOCN_PARAMS=luicode%3D20000174%26lfid%3D231016_-_selffans%26uicode%3D20000174",
    'Connection': 'keep-alive',
}

url_weibocn_profile = 'https://weibo.cn/{}/info'
url_mweibo_profile = "https://m.weibo.cn/profile/info?uid={}"
url_mweibo = "https://m.weibo.cn"
url_mweibo_posts = "https://m.weibo.cn/api/container/getIndex?containerid={}&page={}"
url_mweibo_fans = "https://m.weibo.cn/api/container/getIndex?containerid=231051_-_fans_-_{}&page={}"
url_mweibo_following = "https://m.weibo.cn/api/container/getIndex?containerid=231051_-_followers_-_{}&page={}"
url_mweibo_comments = "https://m.weibo.cn/comments/hotflow?id={}&mid={}&max_id={}&max_id_type=0"
url_mweibo_like_users = 'https://m.weibo.cn/api/statuses/repostTimeline?id={}&page={}'
url_mweibo_report_users = 'https://m.weibo.cn/api/attitudes/show?id={}&page={}'


def download_html(url, retry=3):
    try:
        # if not os.path.exists('./proxy_hellper/proxies.txt'):
        #     raise FileNotFoundError('proxy_hellper/proxies.txt')
        # proxy = random.choice(
        #     [{'http': '//'.join(['http:', l.strip()])} for l in open('./proxy_hellper/proxies.txt').readlines()])
        resp = requests.get(url=url, headers=headers)
        if resp.status_code != 200:
            logger.error('url open error: {}'.format(url))
        return resp.text
    except Exception as e:
        logger.debug("failed and retry: {}".format(url))
        print(e)
        if retry > 1:
            time.sleep(1)
            retry -= 1
            return download_html(url, retry)

def get_all_comments(pid, max_num=10):
    result = []
    url = "https://m.weibo.cn/comments/hotflow?id={}&mid={}&max_id_type=0".format(
        pid, pid)
    logger.info(url)
    time.sleep(5+random.random()*8)
    text = download_html(url)
    try:
        dct = json.loads(text)
        total_num = dct['data']['total_number']
        if not isinstance(total_num,int):
            total_num = max_num
        logger.info('total comments:{}'.format(total_num))
    except:
            return []
    error_count = 5
    while total_num > 0 and len(result) < max_num and error_count > 0:
        logger.info(url)
        time.sleep(3+random.random()*10)
        text = download_html(url)
        try:
            dct = json.loads(text)
        except:
            continue
        comments = parse_comments(pid, dct)
        if comments == []:
            error_count -= 1
            logger.error('comments == []')
            continue
        error_count = 5
        result.extend(comments)
        max_id = dct['data']['max_id']
        url = url_mweibo_comments.format(pid, pid, max_id)
    return result


def parse_comments(pid, data):
    result = []
    if 'data' not in data or 'data' not in data['data']:
        logger.error('key not exists!')
        return result
    for comment in data['data']['data']:
        dct = {}
        dct['pid'] = pid
        dct['_id'] = int(comment['id'])
        dct['created_at'] = comment['created_at']
        dct['text'] = comment['text']
        dct['uid'] = comment['user']['id']
        dct['like_count'] = comment['like_count']
        dct['comments'] = []
        try:
            if isinstance(comment['comments'], list):
                dct['comments'] = parse_comments_of_comment(
                comment['comments'])
        except Exception as e:
            logger.error('parse comment failed!')
            print(e)
        result.append(dct)
    return result


def parse_comments_of_comment(comments: list):
    result = []
    for card in comments:
        dct = {}
        dct['created_at'] = card['created_at']
        dct['text'] = card['text']
        dct['uid'] = card['user']['id']
        result.append(dct)
    return result


if __name__ == '__main__':
    client = pymongo.MongoClient(
        "mongodb://10.108.17.25:27017/")  # 10.108.17.25
    weibo_db = client["weibo"]
    comment_table = weibo_db['comment']
    post_table = weibo_db['post']

    posts = list(post_table.find())
    random.shuffle(posts)

    for post in tqdm(posts):
        pid = post['_id']
        logger.info("request comments for pid = {}".format(pid))
        
        rst = comment_table.find_one({'pid': pid})  # 必须是int
        if rst is not None:
            logger.info('pid = {} has saved!'.format(pid))
            continue
        
        time.sleep(random.randrange(5, 10))
        comments = get_all_comments(pid, 50)
        
        # 保存数据
        for comment in comments:
            try:
                rst = comment_table.insert_one(comment)
                if str(rst.inserted_id) == str(comment['_id']):
                    logger.info(
                        'insert successful cid = {}'.format(comment['_id']))
                else:
                    logger.info(
                        'insert comment faild cid={}'.format(comment['_id']))
            except Exception as e:
                logger.error(
                    'insert comment faild pid = {}'.format(comment['_id']))
                print(e)
