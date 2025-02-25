import os
import shutil
import datetime
import random
import string
import time
import threading
# import traceback
import json

import pydgraph

CONSTANT_USER_NUM = 500
CONSTANT_TWEET_NUM_PER_USER = 1
CONSTANT_TWEET_NUM_LIMIT = 1000
CONSTANT_THREAD_NUM = 24
CONSTANT_TXN_NUM_PER_THREAD = 20900


def get_timestamp_from_response(response):
    response = str(response).strip()
    timestamps = response.split('\n')
    start_ts = int(timestamps[0].split(': ')[1])
    commit_ts = int(timestamps[1].split(': ')[1])
    return start_ts, commit_ts


def random_string(length):
    characters = string.ascii_letters + string.digits
    return ''.join(random.choice(characters) for _ in range(length))


def create_client_stub():
    return pydgraph.DgraphClientStub("localhost:9080")


def create_client(client_stub):
    return pydgraph.DgraphClient(client_stub)


def drop_all(client):
    return client.alter(pydgraph.Operation(drop_all=True))


def set_schema(client):
    schema = """
    u_user_id: int @index(int) .
    u_user_name: string .
    u_user_info: string .
    type User {
        u_user_id
        u_user_name
        u_user_info
    }

    t_tweet_id: int @index(int) .
    t_user_id: int @index(int) .
    t_tweet_data: string .
    type Tweet {
        t_tweet_id
        t_user_id
        t_tweet_data
    }

    lt_user_id: int @index(int) .
    lt_tweet_id: int @index(int) .
    type LastTweet {
        lt_user_id
        lt_tweet_id
    }

    fr_src_user_id: int @index(int) .
    fr_dst_user_id: int @index(int) .
    fr_follow_time: int @index(int) .
    fr_follow: int @index(int) .
    type Follower {
        fr_src_user_id
        fr_dst_user_id
        fr_follow_time
        fr_follow
    }

    fg_src_user_id: int @index(int) .
    fg_dst_user_id: int @index(int) .
    fg_follow_time: int @index(int) .
    fg_follow: int @index(int) .
    type Following {
        fg_src_user_id
        fg_dst_user_id
        fg_follow_time
        fg_follow
    }

    fl_src_user_id: int @index(int) .
    fl_dst_user_id: int @index(int) .
    type FollowList {
        fl_src_user_id
        fl_dst_user_id
    }
    
    x_user_id: int @index(int) .
    x_conflict: string .
    type Conflict {
        x_user_id
        x_conflict
    }
    """
    return client.alter(pydgraph.Operation(schema=schema))


def prepare(client):
    last_tweet_id = -1
    t0_list = []
    for i in range(CONSTANT_USER_NUM):
        user_id = i
        user_name = random_string(10)
        user_info = random_string(200)
        txn = client.txn()
        try:
            user = {
                'dgraph.type': "User",
                'u_user_id': user_id,
                'u_user_name': user_name,
                'u_user_info': user_info,
            }
            txn.mutate(set_obj=user)
            tweet_id_list = []
            tweet_data_list = []
            for j in range(CONSTANT_TWEET_NUM_PER_USER):
                tweet_id = i * CONSTANT_TWEET_NUM_PER_USER + j
                tweet_data = random_string(100)
                tweet = {
                    'dgraph.type': "Tweet",
                    't_tweet_id': tweet_id,
                    't_user_id': user_id,
                    't_tweet_data': tweet_data,
                }
                txn.mutate(set_obj=tweet)
                tweet_id_list.append(tweet_id)
                tweet_data_list.append(tweet_data)
            last_tweet = {
                'dgraph.type': "LastTweet",
                'lt_user_id': user_id,
                'lt_tweet_id': (user_id + 1) * CONSTANT_TWEET_NUM_PER_USER - 1,
            }
            txn.mutate(set_obj=last_tweet)
            txn.mutate(set_obj={
                'dgraph.type': "FollowList",
                'fl_src_user_id': user_id,
                'fl_dst_user_id': user_id,
            })
            txn.mutate(set_obj={
                'dgraph.type': "Follower",
                'fr_src_user_id': user_id,
                'fr_dst_user_id': user_id,
                'fr_follow_time': 0,
                'fr_follow': 1,
            })
            txn.mutate(set_obj={
                'dgraph.type': "Following",
                'fg_src_user_id': user_id,
                'fg_dst_user_id': user_id,
                'fg_follow_time': 0,
                'fg_follow': 1,
            })
            txn.mutate(set_obj={
                'dgraph.type': "Conflict",
                'x_user_id': user_id,
                'x_conflict': str(user_id) + '@' + str(time.time()),
            })
            commit_response = txn.commit()
            txn2check = prepare_collect(commit_response, user_id, tweet_id_list, tweet_data_list)
            t0_list.append(txn2check)
            last_tweet_id = (user_id + 1) * CONSTANT_TWEET_NUM_PER_USER - 1
        except Exception as e:
            print(e)
            txn.discard()
    return last_tweet_id, t0_list


def prepare_collect(response, user_id, tweet_id_list, tweet_data_list):
    start_ts, commit_ts = get_timestamp_from_response(response)
    txn = {
        "sid": 0,
        "sts": {"p": start_ts, "l": 0},
        "cts": {"p": commit_ts, "l": 0},
        "ops": [],
        "mts": int(round(time.time() * 1000)),
    }
    for i in range(len(tweet_id_list)):
        tweet_id = tweet_id_list[i]
        tweet_data = tweet_data_list[i]
        txn['ops'].append({"t": "w", "k": "tweet:" + str(tweet_id), "v": str(user_id) + "#" + tweet_data})
    txn['ops'].append(
        {"t": "w", "k": "last_tweet:" + str(user_id), "v": (user_id + 1) * CONSTANT_TWEET_NUM_PER_USER - 1})
    txn['ops'].append({"t": "w", "k": "follow_list:" + str(user_id), "v": str(user_id)})
    return txn


def txn_new_tweet(client, sid, user_id, last_tweet_id):
    tweet_data = random_string(100)
    txn = client.txn()
    try:
        tweet = {
            'dgraph.type': "Tweet",
            't_tweet_id': last_tweet_id + 1,
            't_user_id': user_id,
            't_tweet_data': tweet_data,
        }
        txn.mutate(set_obj=tweet)

        query_last_tweet = """{
            u as var(func: eq(lt_user_id, %d))
        }""" % user_id
        nquad_last_tweet = """
            uid(u) <dgraph.type> "LastTweet" .
            uid(u) <lt_user_id> "%d" .
            uid(u) <lt_tweet_id> "%d" .
        """ % (user_id, (last_tweet_id + 1))
        mutation_last_tweet = txn.create_mutation(set_nquads=nquad_last_tweet)
        request_last_tweet = txn.create_request(query=query_last_tweet, mutations=[mutation_last_tweet])
        txn.do_request(request_last_tweet)

        commit_response = txn.commit()
        txn2check = txn_new_tweet_collect(commit_response, sid, user_id, last_tweet_id, tweet_data)
        return True, txn2check
    except Exception as e:
        txn.discard()
        print(e)
        return False, None


def txn_new_tweet_collect(response, sid, user_id, last_tweet_id, tweet_data):
    start_ts, commit_ts = get_timestamp_from_response(response)
    txn = {
        "sid": sid,
        "sts": {"p": start_ts, "l": 0},
        "cts": {"p": commit_ts, "l": 0},
        "ops": [
            {"t": "w", "k": "tweet:" + str(last_tweet_id + 1), "v": str(user_id) + "#" + tweet_data},
            {"t": "w", "k": "last_tweet:" + str(user_id), "v": last_tweet_id + 1},
        ],
        "mts": int(round(time.time() * 1000)),
    }
    return txn


def txn_update_last_tweet(client, sid, user_id):
    txn = client.txn()
    try:
        query_last_tweet = """query all($a: int) {
            all(func: eq(lt_user_id, $a)) {
                lt_tweet_id
            }
        }"""
        variables = {"$a": str(user_id)}
        res = txn.query(query_last_tweet, variables=variables)
        last_tweet = json.loads(res.json)['all'][0]
        last_tweet_id = last_tweet['lt_tweet_id']
        query_tweet = """{
            u as var(func: eq(t_tweet_id, %d))
        }""" % last_tweet_id
        tweet_data = random_string(100)
        nquad_tweet = """
            uid(u) <dgraph.type> "Tweet" .
            uid(u) <t_tweet_data> "%s" .
        """ % tweet_data
        mutation_tweet = txn.create_mutation(set_nquads=nquad_tweet)
        request_tweet = txn.create_request(query=query_tweet, mutations=[mutation_tweet])
        txn.do_request(request_tweet)

        commit_response = txn.commit()
        txn2check = txn_update_last_tweet_collect(commit_response, sid, user_id, last_tweet_id, tweet_data)
        return True, txn2check
    except Exception as e:
        txn.discard()
        print(e)
        return False, None


def txn_update_last_tweet_collect(response, sid, user_id, last_tweet_id, tweet_data):
    start_ts, commit_ts = get_timestamp_from_response(response)
    txn = {
        "sid": sid,
        "sts": {"p": start_ts, "l": 0},
        "cts": {"p": commit_ts, "l": 0},
        "ops": [
            {"t": "r", "k": "last_tweet:" + str(user_id), "v": last_tweet_id},
            {"t": "w", "k": "tweet:" + str(last_tweet_id), "v": str(user_id) + "#" + tweet_data},
        ],
        "mts": int(round(time.time() * 1000)),
    }
    return txn


def txn_follow(client, sid, user_id, follow_id, follow):
    follow_time = int(round(time.time() * 1000))
    txn = client.txn()
    try:
        query_follower = """{
            u as var(func: eq(fr_src_user_id, %d)) @filter(eq(fr_dst_user_id, %d))
        }""" % (follow_id, user_id)
        nquad_follower = """
            uid(u) <dgraph.type> "Follower" .
            uid(u) <fr_src_user_id> "%d" .
            uid(u) <fr_dst_user_id> "%d" .
            uid(u) <fr_follow_time> "%d" .
            uid(u) <fr_follow> "%d" .
        """ % (follow_id, user_id, follow_time, follow)
        mutation_follower = txn.create_mutation(set_nquads=nquad_follower)
        request_follower = txn.create_request(query=query_follower, mutations=[mutation_follower])
        txn.do_request(request_follower)

        query_following = """{
            u as var(func: eq(fg_src_user_id, %d)) @filter(eq(fg_dst_user_id, %d))
        }""" % (user_id, follow_id)
        nquad_following = """
            uid(u) <dgraph.type> "Following" .
            uid(u) <fg_src_user_id> "%d" .
            uid(u) <fg_dst_user_id> "%d" .
            uid(u) <fg_follow_time> "%d" .
            uid(u) <fg_follow> "%d" .
        """ % (user_id, follow_id, follow_time, follow)
        mutation_following = txn.create_mutation(set_nquads=nquad_following)
        request_following = txn.create_request(query=query_following, mutations=[mutation_following])
        txn.do_request(request_following)

        pre_query_follow_list = """query all($a: int) {
            all(func: eq(fl_src_user_id, $a)) {
                fl_dst_user_id
            }
        }"""
        variables = {"$a": str(user_id)}
        res = txn.query(pre_query_follow_list, variables=variables)
        pre_follow_list = json.loads(res.json)
        pre_follow_id_list = []
        for pre_follow in pre_follow_list['all']:
            pre_follow_id_list.append(pre_follow['fl_dst_user_id'])
        pre_follow_id_list.sort()

        if follow == 1:
            query_follow_list = """{
                u as var(func: eq(fl_src_user_id, %d)) @filter(eq(fl_dst_user_id, %d))
            }""" % (user_id, follow_id)
            nquad_follow_list = """
                uid(u) <dgraph.type> "FollowList" .
                uid(u) <fl_src_user_id> "%d" .
                uid(u) <fl_dst_user_id> "%d" .
            """ % (user_id, follow_id)
            mutation_follow_list = txn.create_mutation(set_nquads=nquad_follow_list)
            request_follow_list = txn.create_request(query=query_follow_list, mutations=[mutation_follow_list])
            txn.do_request(request_follow_list)
        else:
            query_follow_list = """query all($a: int, $b: int) {
                all(func: eq(fl_src_user_id, $a)) @filter(eq(fl_dst_user_id, $b)) {
                    uid
                }
            }"""
            variables = {"$a": str(user_id), "$b": str(follow_id)}
            res = txn.query(query_follow_list, variables=variables)
            follow_list = json.loads(res.json)
            if len(follow_list['all']) > 0:
                txn.mutate(del_obj=follow_list['all'][0])

        query_conflict = """{
            u as var(func: eq(x_user_id, %d))
        }""" % user_id
        nquad_conflict = """
            uid(u) <x_user_id> "%d" .
            uid(u) <x_conflict> "%s" .
        """ % (user_id, str(user_id) + '@' + str(time.time()))
        mutation_conflict = txn.create_mutation(set_nquads=nquad_conflict)
        request_conflict = txn.create_request(query=query_conflict, mutations=[mutation_conflict])
        txn.do_request(request_conflict)

        commit_response = txn.commit()
        txn2check = txn_follow_collect(commit_response, sid, user_id, follow_id, follow, pre_follow_id_list)
        return True, txn2check
    except Exception as e:
        txn.discard()
        print(e)
        return False, None


def txn_follow_collect(response, sid, user_id, follow_id, follow, pre_follow_id_list):
    start_ts, commit_ts = get_timestamp_from_response(response)
    pre_value = ''
    for i in range(len(pre_follow_id_list) - 1):
        pre_value += str(pre_follow_id_list[i]) + ','
    if len(pre_follow_id_list) > 0:
        pre_value += str(pre_follow_id_list[-1])
    if follow == 1:
        pre_follow_id_list.append(follow_id)
        pre_follow_id_list = sorted(list(set(pre_follow_id_list)))
    else:
        pre_follow_id_list = [x for x in pre_follow_id_list if x != follow_id]
    post_value = ''
    for i in range(len(pre_follow_id_list) - 1):
        post_value += str(pre_follow_id_list[i]) + ','
    if len(pre_follow_id_list) > 0:
        post_value += str(pre_follow_id_list[-1])
    txn = {
        "sid": sid,
        "sts": {"p": start_ts, "l": 0},
        "cts": {"p": commit_ts, "l": 0},
        "ops": [
            {"t": "r", "k": "follow_list:" + str(user_id), "v": pre_value},
            {"t": "w", "k": "follow_list:" + str(user_id), "v": post_value},
        ],
        "mts": int(round(time.time() * 1000)),
    }
    return txn


def txn_timeline(client, sid, user_id):
    txn = client.txn()
    try:
        query_follow_list = """query all($a: int) {
            all(func: eq(fl_src_user_id, $a)) {
                fl_dst_user_id
            }
        }"""
        variables = {"$a": str(user_id)}
        res = txn.query(query_follow_list, variables=variables)
        follow_list = json.loads(res.json)
        user_id_list = []
        all_user_id_list = []
        for follow in follow_list['all']:
            all_user_id_list.append(follow['fl_dst_user_id'])
            if len(user_id_list) <= 20:
                user_id_list.append(follow['fl_dst_user_id'])
        all_user_id_list.sort()
        tweet_id_list = []
        tweet_data_list = []
        for user_id1 in user_id_list:
            query_last_tweet = """query all($a: int) {
                all(func: eq(lt_user_id, $a)) {
                    lt_tweet_id
                }
            }"""
            variables = {"$a": str(user_id1)}
            res = txn.query(query_last_tweet, variables=variables)
            tweet_id = json.loads(res.json)['all'][0]['lt_tweet_id']
            query_tweet = """query all($a: int) {
                all(func: eq(t_tweet_id, $a)) {
                    t_tweet_data
                }
            }"""
            variables = {"$a": str(tweet_id)}
            res = txn.query(query_tweet, variables=variables)
            tweet_data = json.loads(res.json)['all'][0]['t_tweet_data']
            tweet_id_list.append(tweet_id)
            tweet_data_list.append(tweet_data)
        txn.commit()
        txn2check = txn_timeline_collect(txn._ctx.start_ts, sid, user_id, user_id_list,
                                         all_user_id_list, tweet_id_list, tweet_data_list)
        return True, txn2check
    except Exception as e:
        txn.discard()
        print(e)
        return False, None


def txn_timeline_collect(start_ts, sid, user_id, user_id_list, all_user_id_list, tweet_id_list, tweet_data_list):
    value = ''
    for i in range(len(all_user_id_list) - 1):
        value += str(all_user_id_list[i]) + ','
    if len(all_user_id_list) > 0:
        value += str(all_user_id_list[-1])
    txn = {
        "sid": sid,
        "sts": {"p": start_ts, "l": 0},
        "cts": {"p": start_ts, "l": 0},
        "ops": [{"t": "r", "k": "follow_list:" + str(user_id), "v": value}],
        "mts": int(round(time.time() * 1000)),
    }
    for i in range(len(user_id_list)):
        follow_id = user_id_list[i]
        tweet_id = tweet_id_list[i]
        tweet_data = tweet_data_list[i]
        txn['ops'].append({"t": "r", "k": "last_tweet:" + str(follow_id), "v": tweet_id})
        txn['ops'].append({"t": "r", "k": "tweet:" + str(tweet_id), "v": str(follow_id) + "#" + tweet_data})
    return txn


def txn_show_follow(client, sid, user_id):
    txn = client.txn()
    try:
        query_follow_list = """query all($a: int) {
                    all(func: eq(fl_src_user_id, $a)) {
                        fl_dst_user_id
                    }
                }"""
        variables = {"$a": str(user_id)}
        res = txn.query(query_follow_list, variables=variables)
        follow_list = json.loads(res.json)
        all_user_id_list = []
        for follow in follow_list['all']:
            all_user_id_list.append(follow['fl_dst_user_id'])
        all_user_id_list.sort()
        txn.commit()
        txn2check = txn_show_follow_collect(txn._ctx.start_ts, sid, user_id, all_user_id_list)
        return True, txn2check
    except Exception as e:
        txn.discard()
        print(e)
        return False, None


def txn_show_follow_collect(start_ts, sid, user_id, all_user_id_list):
    value = ''
    for i in range(len(all_user_id_list) - 1):
        value += str(all_user_id_list[i]) + ','
    if len(all_user_id_list) > 0:
        value += str(all_user_id_list[-1])
    txn = {
        "sid": sid,
        "sts": {"p": start_ts, "l": 0},
        "cts": {"p": start_ts, "l": 0},
        "ops": [{"t": "r", "k": "follow_list:" + str(user_id), "v": value}],
        "mts": int(round(time.time() * 1000)),
    }
    return txn


def txn_show_tweets(client, sid, user_id):
    txn = client.txn()
    try:
        query_last_tweet = """query all($a: int) {
            all(func: eq(lt_user_id, $a)) {
                lt_tweet_id
            }
        }"""
        variables = {"$a": str(user_id)}
        res = txn.query(query_last_tweet, variables=variables)
        last_tweet_id = json.loads(res.json)['all'][0]['lt_tweet_id']
        i = last_tweet_id - 10 if last_tweet_id >= 10 else 0
        tweet_id_list = []
        user_id_list = []
        tweet_data_list = []
        while i <= last_tweet_id:
            query_tweet = """query all($a: int) {
                all(func: eq(t_tweet_id, $a)) {
                    t_user_id
                    t_tweet_data
                }
            }"""
            variables = {"$a": str(i)}
            res = txn.query(query_tweet, variables=variables)
            if len(json.loads(res.json)['all']) == 0:
                i += 1
                continue
            user_id = json.loads(res.json)['all'][0]['t_user_id']
            tweet_data = json.loads(res.json)['all'][0]['t_tweet_data']
            tweet_id_list.append(i)
            user_id_list.append(user_id)
            tweet_data_list.append(tweet_data)
            i += 1
        txn.commit()
        txn2check = txn_show_tweets_collect(txn._ctx.start_ts, sid, user_id, last_tweet_id,
                                            tweet_id_list, user_id_list, tweet_data_list)
        return True, txn2check
    except Exception as e:
        txn.discard()
        print(e)
        return False, None


def txn_show_tweets_collect(start_ts, sid, user_id, last_tweet_id, tweet_id_list, user_id_list, tweet_data_list):
    txn = {
        "sid": sid,
        "sts": {"p": start_ts, "l": 0},
        "cts": {"p": start_ts, "l": 0},
        "ops": [{"t": "r", "k": "last_tweet:" + str(user_id), "v": last_tweet_id}],
        "mts": int(round(time.time() * 1000)),
    }
    for i in range(len(tweet_id_list)):
        tweet_id = tweet_id_list[i]
        user_id = user_id_list[i]
        tweet_data = tweet_data_list[i]
        txn['ops'].append({"t": "r", "k": "tweet:" + str(tweet_id), "v": str(user_id) + "#" + tweet_data})
    return txn


class TwitterTask(threading.Thread):
    def __init__(self, sid, txns2run):
        threading.Thread.__init__(self)
        self.sid = sid
        self.txns2run = txns2run
        self.client_stub = create_client_stub()
        self.client = create_client(self.client_stub)

    def run(self):
        committed_count = 0
        aborted_count = 0
        txns2check = []
        for txn2run in self.txns2run:
            txn_type = txn2run[0]
            if txn_type == 0:
                success, txn2check = txn_new_tweet(self.client, self.sid, txn2run[1], txn2run[2])
            elif txn_type == 0.5:
                success, txn2check = txn_update_last_tweet(self.client, self.sid, txn2run[1])
            elif txn_type == 1:
                success, txn2check = txn_follow(self.client, self.sid, txn2run[1], txn2run[2], txn2run[3])
            elif txn_type == 2:
                success, txn2check = txn_timeline(self.client, self.sid, txn2run[1])
            elif txn_type == 3:
                success, txn2check = txn_show_follow(self.client, self.sid, txn2run[1])
            else:
                success, txn2check = txn_show_tweets(self.client, self.sid, txn2run[1])
            if success:
                committed_count += 1
                txns2check.append(txn2check)
            else:
                aborted_count += 1
        print("Thread %d: Committed %d, Aborted %d" % (self.sid, committed_count, aborted_count))
        print(datetime.datetime.now())
        # save to file
        with open('./twitter-log/' + str(self.sid) + '.json', 'w') as f:
            json.dump(txns2check, f)


def main():
    if os.path.exists('./twitter-log/'):
        shutil.rmtree('./twitter-log/')
    os.makedirs('./twitter-log/')

    print(datetime.datetime.now())
    client_stub = create_client_stub()
    client = create_client(client_stub)
    drop_all(client)
    set_schema(client)
    last_tweet_id, t0_list = prepare(client)
    client_stub.close()
    print(datetime.datetime.now())

    t0 = {
        "sid": 0,
        "sts": {"p": 0, "l": 0},
        "cts": {"p": 0, "l": 0},
        "ops": [],
        "mts": int(round(time.time() * 1000)),
    }
    for t in t0_list:
        for op in t['ops']:
            t0['ops'].append(op)
    with open('./twitter-log/0.json', 'w') as f:
        json.dump([t0], f)

    txns2run_list = []
    for i in range(CONSTANT_THREAD_NUM):
        txns2run_list.append([])
    for i in range(CONSTANT_THREAD_NUM * CONSTANT_TXN_NUM_PER_THREAD):
        thread_id = i % CONSTANT_THREAD_NUM
        rand_num = random.random()
        user_id = random.randint(0, CONSTANT_USER_NUM - 1)
        if rand_num < 0.2:
            if last_tweet_id < CONSTANT_TWEET_NUM_LIMIT - 1:
                txns2run_list[thread_id].append([0, user_id, last_tweet_id])
                last_tweet_id += 1
            else:
                txns2run_list[thread_id].append([0.5, user_id])
        elif rand_num < 0.4:
            follow_id = random.randint(0, CONSTANT_USER_NUM - 1)
            txns2run_list[thread_id].append([1, user_id, follow_id, 1])
        elif rand_num < 0.6:
            follow_id = random.randint(0, CONSTANT_USER_NUM - 1)
            txns2run_list[thread_id].append([1, user_id, follow_id, 0])
        elif rand_num < 0.7:
            txns2run_list[thread_id].append([2, user_id])
        elif rand_num < 0.8:
            txns2run_list[thread_id].append([3, user_id])
        else:
            txns2run_list[thread_id].append([4, user_id])
    tasks = []
    for i in range(CONSTANT_THREAD_NUM):
        task = TwitterTask(i + 1, txns2run_list[i])
        tasks.append(task)
    for task in tasks:
        task.start()


if __name__ == '__main__':
    main()
