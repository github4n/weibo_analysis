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


def get_user_posts(uid, statuses_count=0):
	result = []
	text = download_html(url_mweibo_profile.format(uid))
	dct = json.loads(text)
	page_num = math.ceil(statuses_count / 10)
	for i in range(page_num):
		time.sleep(5 + random.random() * 10)
		url_posts = url_mweibo_posts.format(dct['data']['more'][3:], i + 1)
		logger.info(url_posts)
		posts = parse_url_posts(uid, url_posts)
		result.extend(posts)
	return result


def parse_url_posts(uid, url_posts):
	result = []
	text = download_html(url_posts)
	data = json.loads(text)
	for card in data['data']['cards'][1:]:
		dct = {}
		dct['uid'] = int(uid)

		if 'mblog' not in card:
			logger.error('no "mblog" in card! {}'.format(url_posts))
			break

		dct['_id'] = int(card['mblog']['id'])
		dct['created_at'] = card['mblog']['created_at']
		dct['text'] = card['mblog']['text']
		dct['source'] = card['mblog']['source']
		dct['reposts_count'] = card['mblog']['reposts_count']
		dct['comments_count'] = card['mblog']['comments_count']
		dct['attitudes_count'] = card['mblog']['attitudes_count']
		dct['isLongText'] = card['mblog']['isLongText']

		if 'page_info' in card['mblog']:
			dct['page_info'] = card['mblog']['page_info']
		result.append(dct)
	return result


if __name__ == '__main__':
	client = pymongo.MongoClient("mongodb://10.108.17.25:27017/")  # 10.108.17.25
	weibo_db = client["weibo"]
	user_table = weibo_db['user']
	post_table = weibo_db['post']

	# followings = user_table.find_one({'_id': 7044218812})['following']
	
	followings = []

	for uid in tqdm(followings):
		logger.info("request posts for uid = {}".format(uid))
		time.sleep(random.randrange(3, 8))
		posts = get_user_posts(uid, statuses_count=20)

		# 保存数据
		for post in posts:
			try:
				rst = post_table.find_one({'_id': int(post['_id'])})  # 必须是int
				if rst is not None:
					logger.info('pid = {} has saved!'.format(post['_id']))
					continue
				rst = post_table.insert_one(post)
				if str(rst.inserted_id) == str(post['_id']):
					logger.info('insert successful pid = {}'.format(post['_id']))
				else:
					logger.info('insert post faild pid={}'.format(post['_id']))
			except Exception as e:
				logger.error('insert post faild pid = {}'.format(post['_id']))
				print(e)
