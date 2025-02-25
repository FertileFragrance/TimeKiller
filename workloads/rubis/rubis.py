import datetime
import os
import shutil
import threading
import time
import random
import string
import json
import traceback

import pydgraph

CONSTANT_REGION_NUM = 62
CONSTANT_USER_NUM = 200
CONSTANT_ITEM_NUM_PER_USER = 4
CONSTANT_THREAD_NUM = 24
CONSTANT_TXN_NUM_PER_THREAD = 28000


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
    r_region_id: int @index(int) .
    r_region_name: string .
    type Region {
        r_region_id
        r_region_name
    }

    u_user_id: int @index(int) .
    u_first_name: string .
    u_last_name: string .
    u_nick_name: string .
    u_email: string .
    u_password: string .
    u_balance: int .
    u_creation_date: int .
    type User {
        u_user_id
        u_first_name
        u_last_name
        u_nick_name
        u_email
        u_password
        u_balance
        u_creation_date
    }

    rt_user_id: int @index(int) .
    rt_rating: int .
    type Rating {
        rt_user_id
        rt_rating
    }

    i_item_id: int @index(int) .
    i_name: string .
    i_description: string .
    i_initial_price: int .
    i_quantity: int .
    i_reserve_price: int .
    i_buy_now: int .
    i_nb_of_bids: int .
    i_max_bid: int .
    i_seller_id: int .
    i_category: int .
    type Item {
        i_item_id
        i_name
        i_description
        i_initial_price
        i_quantity
        i_reserve_price
        i_buy_now
        i_nb_of_bids
        i_max_bid
        i_category
    }

    c_comment_id: int @index(int) .
    c_from_id: int .
    c_to_id: int .
    c_item_id: int .
    c_rating: int .
    c_comment: string .
    type Comment {
        c_comment_id
        c_from_id
        c_to_id
        c_item_id
        c_rating
        c_comment
    }

    bn_buy_id: int @index(int) .
    bn_user_id: int .
    bn_item_id: int .
    bn_quantity: int .
    type BuyNow {
        bn_buy_id
        bn_user_id
        bn_item_id
        bn_quantity
    }
    """
    return client.alter(pydgraph.Operation(schema=schema))


def op_rating(d, r_or_w):
    return {"t": r_or_w, "k": "rating:" + str(d["rt_user_id"]), "v": d["rt_rating"]}


def op_item(i, r_or_w):
    return {"t": r_or_w, "k": "item:" + str(i["i_item_id"]), "v": i["i_quantity"]}


def prepare(client):
    ops = []
    txn = client.txn()
    for i in range(CONSTANT_REGION_NUM):
        try:
            region = {
                "dgraph.type": "Region",
                "r_region_id": i,
                "r_region_name": "Region" + str(i)
            }
            txn.mutate(set_obj=region)
        except Exception as e:
            print(e)
            traceback.print_exc()
            txn.discard()
            exit(1)
    for i in range(CONSTANT_USER_NUM):
        try:
            user = {
                "dgraph.type": "User",
                "u_user_id": i,
                "u_first_name": "Great" + str(i),
                "u_last_name": "User" + str(i),
                "u_nick_name": "user" + str(i),
                "u_email": "Great" + str(i) + "." + "User" + str(i) + "@rubis.com",
                "u_password": "password" + str(i),
                "u_balance": int(i * 2241215.1241),
                "u_creation_date": int(round(time.time() * 1000))
            }
            txn.mutate(set_obj=user)
            rating = {
                "dgraph.type": "Rating",
                "rt_user_id": i,
                "rt_rating": i % 5
            }
            ops.append(op_rating(rating, "w"))
            txn.mutate(set_obj=rating)
        except Exception as e:
            print(e)
            traceback.print_exc()
            txn.discard()
            exit(1)
    for i in range(CONSTANT_USER_NUM * CONSTANT_ITEM_NUM_PER_USER):
        user_id = (int)(i / CONSTANT_ITEM_NUM_PER_USER)
        try:
            item = {
                "dgraph.type": "Item",
                "i_item_id": i,
                "i_name": random_string(10),
                "i_description": random_string(40),
                "i_initial_price": random.randint(10, 20),
                "i_quantity": random.randint(10, 100),
                "i_reserve_price": 20 + random.randint(5, 15),
                "i_buy_now": random.randint(10, 20),
                "i_nb_of_bids": 0,
                "i_max_bid": 0,
                "i_seller_id": user_id,
                "i_category": 0
            }
            ops.append(op_item(item, "w"))
            txn.mutate(set_obj=item)
        except Exception as e:
            print(e)
            txn.discard()
            traceback.print_exc()
            exit(1)
    for i in range(CONSTANT_USER_NUM * CONSTANT_ITEM_NUM_PER_USER):
        user_id = (int)(i / CONSTANT_ITEM_NUM_PER_USER)
        try:
            comment = {
                "dgraph.type": "Comment",
                "c_comment_id": i * 2,
                "c_from_id": random.randint(0, CONSTANT_USER_NUM - 1),
                "c_to_id": user_id,
                "c_item_id": i,
                "c_rating": i * 2 % 5,
                "c_comment": "This is a comment on an item which nobody will ever read"
            }
            txn.mutate(set_obj=comment)
            comment["c_comment_id"] = i * 2 + 1
            comment["c_from_id"] = random.randint(0, CONSTANT_USER_NUM - 1)
            comment["c_rating"] = (i * 2 + 1) % 5
            txn.mutate(set_obj=comment)
        except Exception as e:
            print(e)
            txn.discard()
            traceback.print_exc()
            exit(1)
    try:
        txn.commit()
    except Exception as e:
        print(e)
        txn.discard()
        traceback.print_exc()
        exit(1)
    return {
        "sid": 0,
        "sts": {"p": 0, "l": 0},
        "cts": {"p": 0, "l": 0},
        "ops": ops,
        "mts": int(round(time.time() * 1000))
    }


def get_timestamp_from_response(response):
    response = str(response).strip()
    timestamps = response.split('\n')
    start_ts = int(timestamps[0].split(': ')[1])
    commit_ts = int(timestamps[1].split(': ')[1])
    return start_ts, commit_ts


def get_txn_from_response(response, sid, ops):
    start_ts, commit_ts = get_timestamp_from_response(response)
    return {
        "sid": sid,
        "sts": {"p": start_ts, "l": 0},
        "cts": {"p": commit_ts, "l": 0},
        "ops": ops,
        "mts": int(round(time.time() * 1000))
    }


def get_read_only_txn(start_ts, sid, ops):
    return {
        "sid": sid,
        "sts": {"p": start_ts, "l": 0},
        "cts": {"p": start_ts, "l": 0},
        "ops": ops,
        "mts": int(round(time.time() * 1000))
    }


def txn_register_user(client, sid, user_id):
    ops = []
    txn = client.txn()
    try:
        user = {
            "dgraph.type": "User",
            "u_user_id": user_id,
            "u_first_name": "Great" + str(user_id),
            "u_last_name": "User" + str(user_id),
            "u_nick_name": "user" + str(user_id),
            "u_email": "Great" + str(user_id) + "." + "User" + str(user_id) + "@rubis.com",
            "u_password": "password" + str(user_id),
            "u_balance": int(user_id * 2241215.1241),
            "u_creation_date": int(round(time.time() * 1000))
        }
        txn.mutate(set_obj=user)
        rating = {
            "dgraph.type": "Rating",
            "rt_user_id": user_id,
            "rt_rating": user_id % 5
        }
        txn.mutate(set_obj=rating)
        ops.append(op_rating(rating, "w"))
        commit_response = txn.commit()
        txn2check = get_txn_from_response(commit_response, sid, ops)
        return True, txn2check
    except Exception as e:
        print('register user ', end='')
        print(e)
        txn.discard()
        return False, None


def txn_register_item(client, sid, item_id, user_id):
    ops = []
    txn = client.txn()
    try:
        item = {
            "dgraph.type": "Item",
            "i_item_id": item_id,
            "i_name": random_string(10),
            "i_description": random_string(40),
            "i_initial_price": random.randint(10, 20),
            "i_quantity": random.randint(10, 100),
            "i_reserve_price": 20 + random.randint(5, 15),
            "i_buy_now": random.randint(10, 20),
            "i_nb_of_bids": 0,
            "i_max_bid": 0,
            "i_seller_id": user_id,
            "i_category": 0
        }
        txn.mutate(set_obj=item)
        ops.append(op_item(item, "w"))
        commit_response = txn.commit()
        txn2check = get_txn_from_response(commit_response, sid, ops)
        return True, txn2check
    except Exception as e:
        print('register item ', end='')
        print(e)
        txn.discard()
        return False, None


def txn_store_buy_now(client, sid, buy_id, item_id, user_id):
    ops = []
    txn = client.txn()
    try:
        query = """query all($a: int) {
            all(func: eq(i_item_id, $a)) {
                i_uid: uid
                i_item_id
                i_quantity
            }
        }"""
        res = txn.query(query, variables={"$a": str(item_id)})
        items = json.loads(res.json)['all']
        if len(items) == 0:
            txn.discard()
            return False, None
        item = items[0]
        ops.append(op_item(item, "r"))
        qty = random.randint(1, 5)
        item['i_quantity'] -= qty
        item_update = {
            "uid": item['i_uid'],
            "i_quantity": item['i_quantity']
        }
        txn.mutate(set_obj=item_update)
        ops.append(op_item(item, "w"))
        buy_now = {
            "dgraph.type": "BuyNow",
            "bn_buy_id": buy_id,
            "bn_user_id": user_id,
            "bn_item_id": item_id,
            "bn_quantity": qty
        }
        txn.mutate(set_obj=buy_now)
        commit_response = txn.commit()
        txn2check = get_txn_from_response(commit_response, sid, ops)
        return True, txn2check
    except Exception as e:
        print('store buy now ', end='')
        print(e)
        txn.discard()
        return False, None


def txn_store_comment(client, sid, comment_id, item_id, from_id, to_id):
    ops = []
    txn = client.txn()
    try:
        comment = {
            "dgraph.type": "Comment",
            "c_comment_id": comment_id,
            "c_from_id": from_id,
            "c_to_id": to_id,
            "c_item_id": item_id,
            "c_rating": comment_id % 5,
            "c_comment": "This is a comment on an item which nobody will ever read"
        }
        txn.mutate(set_obj=comment)
        query = """query all($a: int) {
            all(func: eq(rt_user_id, $a)) {
                rt_uid: uid
                rt_user_id
                rt_rating
            }
        }"""
        res = txn.query(query, variables={"$a": str(to_id)})
        ratings = json.loads(res.json)['all']
        if len(ratings) == 0:
            txn.discard()
            return False, None
        rating = ratings[0]
        ops.append(op_rating(rating, "r"))
        rating['rt_rating'] = comment_id % 5
        rating_update = {
            "uid": rating['rt_uid'],
            "rt_rating": rating['rt_rating']
        }
        txn.mutate(set_obj=rating_update)
        ops.append(op_rating(rating, "w"))
        commit_response = txn.commit()
        txn2check = get_txn_from_response(commit_response, sid, ops)
        return True, txn2check
    except Exception as e:
        print('store comment ', end='')
        print(e)
        txn.discard()
        return False, None


def txn_random_brose(client, sid, item_id_list, rating_id_list):
    ops = []
    txn = client.txn()
    try:
        for item_id in item_id_list:
            query = """query all($a: int) {
                all(func: eq(i_item_id, $a)) {
                    i_item_id
                    i_quantity
                }
            }"""
            res = txn.query(query, variables={"$a": str(item_id)})
            items = json.loads(res.json)['all']
            if len(items) == 0:
                continue
            item = items[0]
            ops.append(op_item(item, "r"))
        for rating_id in rating_id_list:
            query = """query all($a: int) {
                all(func: eq(rt_user_id, $a)) {
                    rt_user_id
                    rt_rating
                }
            }"""
            res = txn.query(query, variables={"$a": str(rating_id)})
            ratings = json.loads(res.json)['all']
            if len(ratings) == 0:
                continue
            rating = ratings[0]
            ops.append(op_rating(rating, "r"))
        txn.commit()
        txn2check = get_read_only_txn(txn._ctx.start_ts, sid, ops)
        return True, txn2check
    except Exception as e:
        print('random browse ', end='')
        print(e)
        txn.discard()
        return False, None


class RubisTask(threading.Thread):
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
                success, txn2check = txn_register_user(self.client, self.sid, txn2run[1])
            elif txn_type == 1:
                success, txn2check = txn_register_item(self.client, self.sid, txn2run[1], txn2run[2])
            elif txn_type == 2:
                success, txn2check = txn_store_buy_now(self.client, self.sid, txn2run[1], txn2run[2], txn2run[3])
            elif txn_type == 3:
                success, txn2check = txn_store_comment(self.client, self.sid, txn2run[1], txn2run[2], txn2run[3], txn2run[4])
            else:
                success, txn2check = txn_random_brose(self.client, self.sid, txn2run[1], txn2run[2])
            if success:
                committed_count += 1
                if txn_type >= 2:
                    txns2check.append(txn2check)
            else:
                aborted_count += 1
        print("Thread %d: Committed %d, Aborted %d" % (self.sid, committed_count, aborted_count))
        print(datetime.datetime.now())
        # save to file
        with open('./rubis-log/' + str(self.sid) + '.json', 'w') as f:
            json.dump(txns2check, f)


def main():
    if os.path.exists('./rubis-log/'):
        shutil.rmtree('./rubis-log/')
    os.makedirs('./rubis-log/')

    print(datetime.datetime.now())
    client_stub = create_client_stub()
    client = create_client(client_stub)
    drop_all(client)
    set_schema(client)
    t0 = prepare(client)
    client_stub.close()
    print(datetime.datetime.now())

    with open('./rubis-log/0.json', 'w') as f:
        json.dump([t0], f)

    txns2run_list = []
    for i in range(CONSTANT_THREAD_NUM):
        txns2run_list.append([])
    next_user_id = CONSTANT_USER_NUM
    next_item_id = CONSTANT_USER_NUM * CONSTANT_ITEM_NUM_PER_USER
    next_buy_id = 0
    next_comment_id = CONSTANT_USER_NUM * CONSTANT_ITEM_NUM_PER_USER * 2
    for i in range(CONSTANT_THREAD_NUM * CONSTANT_TXN_NUM_PER_THREAD):
        thread_id = i % CONSTANT_THREAD_NUM
        rand_num = random.random()
        if rand_num < 0.1:
            txns2run_list[thread_id].append([0, next_user_id])
            next_user_id += 1
        elif rand_num < 0.25:
            user_id = random.randint(0, CONSTANT_USER_NUM - 1)
            txns2run_list[thread_id].append([1, next_item_id, user_id])
            next_item_id += 1
        elif rand_num < 0.4:
            item_id = random.randint(0, CONSTANT_USER_NUM * CONSTANT_ITEM_NUM_PER_USER - 1)
            user_id = random.randint(0, CONSTANT_USER_NUM - 1)
            txns2run_list[thread_id].append([2, next_buy_id, item_id, user_id])
            next_buy_id += 1
        elif rand_num < 0.6:
            item_id = random.randint(0, CONSTANT_USER_NUM * CONSTANT_ITEM_NUM_PER_USER - 1)
            from_id = random.randint(0, CONSTANT_USER_NUM - 1)
            to_id = random.randint(0, CONSTANT_USER_NUM - 1)
            txns2run_list[thread_id].append([3, next_comment_id, item_id, from_id, to_id])
            next_comment_id += 1
        else:
            item_id_list = []
            for j in range(5):
                item_id_list.append(random.randint(0, CONSTANT_USER_NUM * CONSTANT_ITEM_NUM_PER_USER - 1))
            rating_id_list = []
            while len(rating_id_list) < 3:
                rating_id = random.randint(0, CONSTANT_USER_NUM - 1)
                if rating_id not in rating_id_list:
                    rating_id_list.append(rating_id)
            txns2run_list[thread_id].append([4, item_id_list, rating_id_list])
    tasks = []
    for i in range(CONSTANT_THREAD_NUM):
        tasks.append(RubisTask(i + 1, txns2run_list[i]))
    for task in tasks:
        task.start()


if __name__ == '__main__':
    main()
