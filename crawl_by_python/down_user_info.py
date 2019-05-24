#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name:down_user_info
   Author:jasonhaven
   date:19-4-23
-------------------------------------------------
   Change Activity:19-4-23:
-------------------------------------------------
"""
import time
import json
import math
import os
import loguru
import random
import requests
from tqdm import tqdm
from queue import Queue
import pymongo

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
url_mweibo_fans = "https://m.weibo.cn/api/container/getIndex?containerid=231051_-_fans_-_{}&since_id={}"
url_mweibo_following = "https://m.weibo.cn/api/container/getIndex?containerid=231051_-_followers_-_{}&page={}"
url_mweibo_comments = "https://m.weibo.cn/comments/hotflow?id={}&mid={}&max_id={}&max_id_type=0"
url_mweibo_like_users = 'https://m.weibo.cn/api/statuses/repostTimeline?id={}&page={}'
url_mweibo_report_users = 'https://m.weibo.cn/api/attitudes/show?id={}&page={}'


def download_html(url, retry=3):
    try:
        if not os.path.exists('./proxy_hellper/proxies.txt'):
            raise FileNotFoundError('proxy_hellper/proxies.txt')
        proxy = random.choice(
            [{'http': '//'.join(['http:', l.strip()])} for l in open('./proxy_hellper/proxies.txt').readlines()])
        resp = requests.get(url=url, headers=headers, proxies=proxy)
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


def get_user_info_from_mweibo(uid):
    text = download_html(url_mweibo_profile.format(uid))
    data = json.loads(text)
    data = data['data']['user']
    infobox = {}
    infobox['_id'] = data['id']
    infobox['screen_name'] = data['screen_name'].strip()
    infobox['statuses_count'] = data['statuses_count']
    infobox['followers_count'] = data['followers_count']
    infobox['follow_count'] = data['follow_count']
    infobox['description'] = data['description'].strip()
    infobox['gender'] = '男' if data['gender'] == 'm' else '女'

    infobox['verified'] = data['verified']
    infobox['verified_type'] = data['description']
    infobox['verified_reason'] = data['description'].strip()
    infobox['following'] = []
    infobox['follower'] = []
    return infobox


def parse_url_friends(url):
    result = []
    text = download_html(url)
    dct = json.loads(text)
    for card in dct['data']['cards']:
        for group in card['card_group']:
            infobox = {}
            if 'desc1' in group:
                infobox['desc1'] = group['desc1']
            if 'desc2' in group:
                infobox['desc2'] = group['desc2']
            data = group['user']
            infobox['_id'] = int(data['id'])
            infobox['screen_name'] = data['screen_name'].strip()
            try:
                if data['followers_count'] < 50 and data['follow_count'] < 100:
                    continue
                infobox['statuses_count'] = data['statuses_count']
                infobox['followers_count'] = data['followers_count']
                infobox['follow_count'] = data['follow_count']
                infobox['description'] = data['description'].strip()
                infobox['gender'] = '男' if data['gender'] == 'm' else '女'
                infobox['verified'] = data['verified']
                infobox['verified_type'] = data['description']
                infobox['verified_reason'] = data['description'].strip()
                infobox['following'] = []
                infobox['follower'] = []
            except:
                logger.error(card['user'])
            result.append(infobox)
    return result


def parse_first_page(url, flag):
    '''
    flag: '全部关注','全部粉丝'
    '''
    result = []
    text = download_html(url)
    dct = json.loads(text)
    for card in dct['data']['cards']:
        if 'title' in card and card['title'].endswith(flag):
            for group in card['card_group']:
                infobox = {}
                if 'desc1' in group:
                    infobox['desc1'] = group['desc1']
                if 'desc2' in group:
                    infobox['desc2'] = group['desc2']
                data = group['user']
                infobox['_id'] = int(data['id'])
                infobox['screen_name'] = data['screen_name'].strip()
                try:
                    if data['followers_count'] < 50 and data['follow_count'] < 100:
                        continue
                    infobox['statuses_count'] = data['statuses_count']
                    infobox['followers_count'] = data['followers_count']
                    infobox['follow_count'] = data['follow_count']
                    infobox['description'] = data['description'].strip()
                    infobox['gender'] = '男' if data['gender'] == 'm' else '女'
                    infobox['verified'] = data['verified']
                    infobox['verified_type'] = data['description']
                    infobox['verified_reason'] = data['description'].strip()
                    infobox['following'] = []
                    infobox['follower'] = []
                except:
                    logger.error(card['user'])
                result.append(infobox)
    return result


# 关注列表
def get_all_followings(uid, follow_count=0):
    result = []
    page_num = math.ceil(follow_count / 20)
    for i in range(page_num):
        time.sleep(random.random() * 10)
        url_following = url_mweibo_following.format(uid, i + 1)
        logger.info(url_following)

        if i == 0:
            followings = parse_first_page(url_following, '全部关注')
        else:
            followings = parse_url_friends(url_following)
        result.extend(followings)
    return result


# 粉丝列表
def get_all_fans(uid, follow_count=0):
    result = []
    page_num = math.ceil(follow_count / 20)
    for i in range(page_num):
        time.sleep(random.random() * 10)
        url_follower = url_mweibo_fans.format(uid, i + 1)
        logger.info(url_follower)

        if i == 0:
            followings = parse_first_page(url_follower, '全部粉丝')
        else:
            followings = parse_url_friends(url_follower)
        result.extend(followings)
    return result


def down_from_root_uid(uid, once=False):
    queue = Queue()
    queue.put(uid)

    while not queue.empty():
        logger.info('qsize = {}'.format(queue.qsize()))
        uid = queue.get()

        rst = user_table.find_one({'_id': int(uid)})  # 必须是int

        if rst is not None:  # 已经保存
            logger.info('uid = {} has saved!'.format(uid))
            if 'following' in rst and rst['following'] != [] and rst['follower'] != []:
                continue
            logger.info('but following and fans is none!')

        # 基本信息
        try:
            logger.info('request for uid = {}'.format(uid))
            time.sleep(random.randrange(6, 10))
            user = get_user_info_from_mweibo(uid)
            user['following'] = set()
            user['follower'] = set()
        except Exception as e:
            logger.error('get_user_info faild uid = {}'.format(uid))
            print(e)
            continue
        if '_id' not in user:
            logger.error('no _id in user uid = {}'.format(uid))
            break

        # 关注列表
        try:
            time.sleep(random.randrange(6, 10))
            follow_count = int(user.get('follow_count'))
            if follow_count > 400:
                follow_count = 400

            followings = get_all_followings(uid, follow_count)
            for u in followings[:100]:
                queue.put(u['_id'])
                user['following'].add(int(u["_id"]))
        except Exception as e:
            logger.error('get_all_followings faild uid = {}'.format(uid))
            print(e)

        # 粉丝列表
        try:
            time.sleep(random.randrange(6, 10))
            follower_count = int(user.get('followers_count'))
            if follower_count > 200:
                follower_count = 200
            fans = get_all_fans(uid, follower_count)

            if queue.qsize() < 10000:
                for u in fans[:50]:
                    # queue.put(u['_id'])
                    user['follower'].add(int(u["_id"]))
        except Exception as e:
            logger.error('get_all_fans faild uid = {}'.format(uid))
            print(e)

        # 保存数据
        try:
            user['_id'] = int(user['_id'])  # 类型转换
            user['following'] = list(user['following'])
            user['follower'] = list(user['follower'])
            if rst is not None:  # 已经保存
                user_table.update_one(rst, {"$set": user})  # $操作符
                logger.info('update uid = {}'.format(uid))
            else:
                rst = user_table.insert_one(user)
                if str(rst.inserted_id) == str(uid):
                    logger.info('insert successful uid = {}\t{}'.format(
                        uid, user['screen_name']))
                else:
                    logger.info('insert faild {}!={}'.format(
                        uid, rst.inserted_id))
        except Exception as e:
            logger.error('insert user faild uid = {}'.format(uid))
            print(e)
        if once:
            break


def down_from_uid_candidates(uids):
    for uid in tqdm(uids):
        logger.info('size = {}'.format(len(uids)))

        rst = user_table.find_one({'_id': int(uid)})  # 必须是int

        if rst is not None:  # 已经保存
            logger.info('uid = {} has saved!'.format(uid))
            if 'following' in rst and rst['following'] != [] and 'follower' in rst and rst['follower'] != []:
                continue
            logger.info('but following and fans is none!')

        # 基本信息
        try:
            logger.info('request for uid = {}'.format(uid))
            time.sleep(random.randrange(4, 8))
            user = get_user_info_from_mweibo(uid)
            user['following'] = set()
            user['follower'] = set()
        except Exception as e:
            logger.error('get_user_info faild uid = {}'.format(uid))
            print(e)
            continue
        if '_id' not in user:
            logger.error('no _id in user uid = {}'.format(uid))
            break

        # 关注列表
        try:
            time.sleep(random.randrange(2, 5))
            follow_count = int(user.get('follow_count'))
            if follow_count > 200:
                follow_count = 200

            followings = get_all_followings(uid, follow_count)
            for u in followings[:100]:
                user['following'].add(int(u["_id"]))
        except Exception as e:
            logger.error('get_all_followings faild uid = {}'.format(uid))
            print(e)

        # 粉丝列表
        try:
            time.sleep(random.randrange(2, 5))
            follower_count = int(user.get('followers_count'))
            if follower_count > 50:
                follower_count = 50

            fans = get_all_fans(uid, follower_count)
            for u in fans[:50]:
                user['follower'].add(int(u["_id"]))
        except Exception as e:
            logger.error('get_all_fans faild uid = {}'.format(uid))
            print(e)

        # 保存数据
        try:
            if rst is not None:  # 已经保存
                # $操作符
                user_table.update_one(
                    rst, {"$set": {'following': list(user['following']), 'follower': list(user['follower'])}})
                logger.info('update uid = {}'.format(uid))
            else:
                user['_id'] = int(user['_id'])  # 类型转换
                user['following'] = list(user['following'])
                user['follower'] = list(user['follower'])
                rst = user_table.insert_one(user)
                if str(rst.inserted_id) == str(uid):
                    logger.info('insert successful uid = {}\t{}'.format(
                        uid, user['screen_name']))
                else:
                    logger.info('insert faild {}!={}'.format(
                        uid, rst.inserted_id))
        except Exception as e:
            logger.error('insert user faild uid = {}'.format(uid))
            print(e)


if __name__ == '__main__':
    client = pymongo.MongoClient(
        "mongodb://10.108.17.25:27017/")  # 10.108.17.25
    weibo_db = client["weibo"]
    user_table = weibo_db['user']
    # down_from_root_uid(7044218812, once=True)
    followings = user_table.find_one({'_id': 7044218812})['following']
    print(followings[:])
    down_from_uid_candidates(followings[:])
