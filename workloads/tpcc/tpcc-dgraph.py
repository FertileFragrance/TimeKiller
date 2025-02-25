import json
import random

from pydgraph import DgraphClient
from datetime import datetime
import pydgraph

import deal_response
from load_data import TPCCConstants

TOTAL_TXN_NUM = 500000


def new_order_txn(client: DgraphClient, w_id: int, d_id: int, c_id: int, items: list):
    ops = []
    txn = client.txn()
    try:
        # ---------------------- 1. query customer and warehouse ----------------------
        query = '''
        query q($w_id: int, $c_id: int, $d_id: int) {
            warehouse(func: eq(w_id, $w_id)) {
                w_uid: uid
                w_id
                w_tax
                w_ytd
            }
            customer(func: eq(c_id, $c_id)) @filter(eq(c_w_id, $w_id) AND eq(c_d_id, $d_id)) {
                c_uid: uid
                c_w_id
                c_d_id
                c_id
                c_balance
                c_delivery_cnt
                c_discount
                c_last
            }
        }
        '''
        res = txn.query(query, variables={'$w_id': str(w_id), '$c_id': str(c_id), '$d_id': str(d_id)})
        data = json.loads(res.json)
        w_tax = data['warehouse'][0]['w_tax']
        w_tax = w_tax / 100.0
        c_discount = data['customer'][0]['c_discount']

        ops.append(deal_response.op_warehouse(data['warehouse'][0], "r"))
        ops.append(deal_response.op_customer(data['customer'][0], "r"))

        # ---------------------- 2. query and update district ----------------------
        district_query = '''
        query q($w_id: int, $d_id: int) {
            district(func: eq(d_id, $d_id)) @filter(eq(d_w_id, $w_id)) {
                d_uid: uid
                d_w_id
                d_id
                d_ytd
                d_next_o_id
                d_tax
            }
        }
        '''
        res = txn.query(district_query, variables={'$w_id': str(w_id), '$d_id': str(d_id)})
        district = json.loads(res.json)['district'][0]
        d_next_o_id = district['d_next_o_id']
        d_tax = district['d_tax']
        d_tax = d_tax / 100.0

        ops.append(deal_response.op_district(district, "r"))

        # update d_next_o_id
        txn.mutate(set_obj={
            'uid': district['d_uid'],
            'd_next_o_id': d_next_o_id + 1
        })

        district['d_next_o_id'] = d_next_o_id + 1
        ops.append(deal_response.op_district(district, "w"))
        district['d_next_o_id'] = d_next_o_id

        # ---------------------- 3. insert to Order and NewOrder ----------------------
        o_id = d_next_o_id
        o_all_local = 1  # assume all local first

        # insert to Order
        order_data = {
            'o_w_id': w_id,
            'o_d_id': d_id,
            'o_id': o_id,
            'o_carrier_id': -1,
            'o_ol_cnt': len(items),
            'o_c_id': c_id,
            'o_entry_d': datetime.now().isoformat(),
            'o_all_local': o_all_local,
            'dgraph.type': 'Order'
        }
        order_uid = txn.mutate(set_obj=order_data).uids['order']

        ops.append(deal_response.op_order(order_data, "w"))

        # insert to NewOrder
        new_order_data = {
            'no_o_id': o_id,
            'no_d_id': d_id,
            'no_w_id': w_id,
            'dgraph.type': 'NewOrder'
        }
        txn.mutate(set_obj=new_order_data)

        ops.append(deal_response.op_new_order(new_order_data, "i"))

        # ---------------------- 4. process order line ----------------------
        total_amount = 0
        for idx, item in enumerate(items):
            ol_supply_w_id = item['supply_w_id']
            ol_i_id = item['i_id']
            ol_quantity = item['quantity']

            # check if the item is local
            if ol_supply_w_id != w_id:
                o_all_local = 0

            # query item
            item_query = '''
            query q($i_id: int) {
                item(func: eq(i_id, $i_id)) {
                    i_uid: uid
                    i_price
                    i_name
                    i_data
                }
            }
            '''
            res = txn.query(item_query, variables={'$i_id': str(ol_i_id)})
            item_data = json.loads(res.json)['item'][0]
            i_price = item_data['i_price']

            # query and update stock
            stock_query = '''
            query q($s_w_id: int, $i_id: int) {
                stock(func: eq(s_w_id, $s_w_id)) @filter(eq(s_i_id, $i_id)) {
                    s_uid: uid
                    s_w_id
                    s_i_id
                    s_quantity
                    s_dist_01
                    s_dist_02  # choose s_dist_xx by district
                }
            }
            '''
            res = txn.query(stock_query, variables={'$s_w_id': str(ol_supply_w_id), '$i_id': str(ol_i_id)})
            stock = json.loads(res.json)['stock'][0]
            s_quantity = stock['s_quantity']
            # s_dist = stock[f's_dist_{d_id:02d}']

            ops.append(deal_response.op_stock(stock, "r"))

            # update quantity
            new_quantity = s_quantity - ol_quantity
            if new_quantity > 0:
                new_quantity = s_quantity - ol_quantity
            else:
                new_quantity = s_quantity - ol_quantity + 91
            txn.mutate(set_obj={
                'uid': stock['s_uid'],
                's_quantity': new_quantity
            })

            stock['s_quantity'] = new_quantity
            ops.append(deal_response.op_stock(stock, "w"))
            stock['s_quantity'] = s_quantity

            # calculate amount
            ol_amount = ol_quantity * i_price * (1 - c_discount) * (1 + w_tax + d_tax)
            total_amount += ol_amount

            # insert to OrderLine
            order_line = {
                'ol_w_id': w_id,
                'ol_d_id': d_id,
                'ol_o_id': o_id,
                'ol_number': idx + 1,
                'ol_i_id': ol_i_id,
                'ol_delivery_d': datetime.now().isoformat(),
                'ol_supply_w_id': ol_supply_w_id,
                'ol_quantity': ol_quantity,
                'ol_amount': ol_amount,
                'ol_dist_info': '',
                'dgraph.type': 'OrderLine'
            }
            txn.mutate(set_obj=order_line)

            ops.append(deal_response.op_order_line(order_line, "w"))

        # update o_all_local field
        txn.mutate(set_obj={
            'uid': order_uid,
            'o_all_local': o_all_local
        })

        commit_response = txn.commit()
        txn2check = deal_response.get_txn_from_response(commit_response, ops)
        print(txn2check)
        return total_amount

    except Exception as e:
        txn.discard()
        raise e


def payment_txn(client: DgraphClient, w_id: int, d_id: int, c_id: int, payment_amount: int, customer_lastname: bool = False, c_last: str = None):
    ops = []
    txn = client.txn()
    try:
        # ---------------------- 1. update w_ytd of a warehouse ----------------------
        query_warehouse = '''
        query q($w_id: int) {
            warehouse(func: eq(w_id, $w_id)) {
                w_uid: uid
                w_id
                w_tax
                w_ytd
                w_name
            }
        }
        '''
        res_w_id = txn.query(query_warehouse, variables={'$w_id': str(w_id)})
        warehouse_data = json.loads(res_w_id.json)

        if not warehouse_data['warehouse']:
            raise Exception("Invalid warehouse ID")

        ops.append(deal_response.op_warehouse(warehouse_data['warehouse'][0], "r"))

        warehouse = warehouse_data['warehouse'][0]
        warehouse['w_ytd'] = warehouse['w_ytd'] + payment_amount
        warehouse_update = {
            'uid': warehouse['w_uid'],
            'w_ytd': warehouse['w_ytd']
        }
        txn.mutate(set_obj=warehouse_update)

        ops.append(deal_response.op_warehouse(warehouse, "w"))

        # ---------------------- 2. update d_ytd of a district ----------------------
        query_district = '''
        query q($w_id: int, $d_id: int) {
            district(func: eq(d_id, $d_id)) @filter(eq(d_w_id, $w_id)) {
                d_uid: uid
                d_w_id
                d_id
                d_ytd
                d_next_o_id
                d_name
            }
        }
        '''
        res_district = txn.query(query_district, variables={'$w_id': str(w_id), '$d_id': str(d_id)})
        district_data = json.loads(res_district.json)

        if not district_data['district']:
            raise Exception("Invalid district ID")

        ops.append(deal_response.op_district(district_data['district'][0], "r"))

        district = district_data['district'][0]
        district_update = {
            'uid': district['d_uid'],
            'd_ytd': district['d_ytd'] + payment_amount
        }
        txn.mutate(set_obj=district_update)

        district['d_ytd'] = district['d_ytd'] + payment_amount
        ops.append(deal_response.op_district(district, "w"))
        district['d_ytd'] = district['d_ytd'] - payment_amount

        # ---------------------- 3. query customer ----------------------
        if customer_lastname:
            query_customer_lastname = '''
            query q($w_id: int, $d_id: int, $c_last: string) {
                customers(func: eq(c_last, $c_last)) @filter(eq(c_w_id, $w_id) AND eq(c_d_id, $d_id)) {
                    c_uid: uid
                    c_w_id
                    c_d_id
                    c_id
                    c_balance
                    c_delivery_cnt
                    c_first
                    c_middle
                    c_last
                    c_ytd_payment
                    c_payment_cnt
                    c_credit
                    c_data
                }
            }
            '''
            res_customers = txn.query(query_customer_lastname, variables={'$w_id': str(w_id), '$d_id': str(d_id), '$c_last': str(c_last)})
            customers_data = json.loads(res_customers.json)

            if not customers_data['customers']:
                raise Exception("Invalid customer last name")

            customers = sorted(customers_data['customers'], key=lambda x: x['c_first'])
            customer = customers[len(customers) // 2]
        else:
            query_customer = '''
            query q($w_id: int, $d_id: int, $c_id: int) {
                customer(func: eq(c_id, $c_id)) @filter(eq(c_w_id, $w_id) AND eq(c_d_id, $d_id)) {
                    c_uid: uid
                    c_w_id
                    c_d_id
                    c_id
                    c_balance
                    c_delivery_cnt
                    c_first
                    c_middle
                    c_last
                    c_ytd_payment
                    c_payment_cnt
                    c_credit
                    c_data
                }
            }
            '''
            res_customer = txn.query(query_customer, variables={'$w_id': str(w_id), '$d_id': str(d_id), '$c_id': str(c_id)})
            customer_data = json.loads(res_customer.json)

            if not customer_data['customer']:
                raise Exception("Invalid customer ID")

            customer = customer_data['customer'][0]

        ops.append(deal_response.op_customer(customer, "r"))

        # ---------------------- 4. update customer ----------------------
        customer_update = {
            'uid': customer['c_uid'],
            'c_balance': customer['c_balance'] - payment_amount,
            'c_ytd_payment': customer['c_ytd_payment'] + payment_amount,
            'c_payment_cnt': customer['c_payment_cnt'] + 1
        }

        if customer['c_credit'] == "BC":
            c_new_data = f"{customer['c_id']} {d_id} {w_id} {customer['c_id']} {w_id} {payment_amount}"
            if 'c_data' in customer:
                c_new_data = c_new_data + " | " + customer['c_data']
            c_new_data = c_new_data[:500]
            customer_update['c_data'] = c_new_data

        txn.mutate(set_obj=customer_update)

        customer['c_balance'] = customer['c_balance'] - payment_amount
        ops.append(deal_response.op_customer(customer, "w"))
        customer['c_balance'] = customer['c_balance'] + payment_amount

        # ---------------------- 5. insert to History ----------------------
        history_data = {
            'h_c_id': customer['c_id'],
            'h_c_d_id': district['d_id'],
            'h_c_w_id': warehouse['w_id'],
            'h_d_id': district['d_id'],
            'h_w_id': warehouse['w_id'],
            'h_date': datetime.now().isoformat(),
            'h_amount': payment_amount,
            'h_data': f"{warehouse['w_name']}    {district['d_name']}",
            'dgraph.type': 'History'
        }
        txn.mutate(set_obj=history_data)

        commit_response = txn.commit()
        txn2check = deal_response.get_txn_from_response(commit_response, ops)
        print(txn2check)
        return customer_update['c_balance']

    except Exception as e:
        txn.discard()
        raise e


def delivery_txn(client: DgraphClient, w_id: int, carrier_id: int):
    ops = []
    txn = client.txn()
    try:
        # ---------------------- 1. get a new order ----------------------
        query = '''
        query q($w_id: int) {
            new_orders(func: type(NewOrder), first: 1) @filter(eq(no_w_id, $w_id)) {
                no_uid: uid
                no_w_id
                no_o_id
                no_d_id
            }
        }
        '''
        res = txn.query(query, variables={'$w_id': str(w_id)})
        data = json.loads(res.json)
        if not data['new_orders']:
            txn.discard()
            return 0

        ops.append(deal_response.op_new_order(data['new_orders'][0], "r"))

        new_order = data['new_orders'][0]

        # ---------------------- 2. update o_carrier_id of Order ----------------------
        query_order = '''
                query q($w_id: int, $d_id: int, $o_id: int) {
                    order(func: eq(o_id, $o_id)) @filter(eq(o_d_id, $d_id) AND eq(o_w_id, $w_id)) {
                        o_uid: uid
                        o_w_id
                        o_d_id
                        o_id
                        o_carrier_id
                        o_ol_cnt
                        o_c_id
                    }
                }
                '''
        res_order = txn.query(query_order, variables={'$w_id': str(new_order['no_w_id']),
                                                      '$d_id': str(new_order['no_d_id']),
                                                      '$o_id': str(new_order['no_o_id'])})
        orders_data = json.loads(res_order.json)
        if not orders_data['order']:
            raise Exception(f"Order not found.")

        order = orders_data['order'][0]

        ops.append(deal_response.op_order(order, "r"))

        order_update = {
            'uid': order['o_uid'],
            'o_carrier_id': carrier_id
        }
        txn.mutate(set_obj=order_update)

        order['o_carrier_id'] = carrier_id
        ops.append(deal_response.op_order(order, "w"))

        ol_amount = 0
        for i in range(1, order['o_ol_cnt'] + 1):
            # ---------------------- 3. query order line ----------------------
            query_order_lines = '''
            query q($w_id: int, $d_id: int, $o_id: int, $ol_num: int) {
                order_lines(func: eq(ol_o_id, $o_id)) @filter(eq(ol_d_id, $d_id) AND eq(ol_w_id, $w_id) AND eq(ol_number, $ol_num)) {
                    ol_uid: uid
                    ol_w_id
                    ol_d_id
                    ol_o_id
                    ol_number
                    ol_i_id
                    ol_delivery_d
                    ol_amount
                }
            }
            '''
            res_order_lines = txn.query(query_order_lines, variables={'$w_id': str(order['o_w_id']),
                                                                      '$d_id': str(order['o_d_id']),
                                                                      '$o_id': str(order["o_id"]), '$ol_num': str(i)})
            order_lines_data = json.loads(res_order_lines.json)
            order_line = order_lines_data['order_lines'][0]

            ops.append(deal_response.op_order_line(order_line, "r"))

            ol_amount += order_line['ol_amount']

            # ---------------------- 4. update ol_delivery_d of OrderLine ----------------------
            order_line['ol_delivery_d'] = datetime.now().isoformat()
            line_update = {
                'uid': order_line['ol_uid'],
                'ol_delivery_d': order_line['ol_delivery_d']
            }
            txn.mutate(set_obj=line_update)

            ops.append(deal_response.op_order_line(order_line, "w"))

        # ---------------------- 5. delete new order ----------------------
        txn.mutate(del_obj={'uid': new_order['no_uid']})

        ops.append(deal_response.op_new_order(new_order, "d"))

        # ---------------------- 6. update customer ----------------------
        customer_query = '''
                    query q($w_id: int, $d_id: int, $c_id: int) {
                        customer(func: eq(c_id, $c_id)) @filter(eq(c_d_id, $d_id) AND eq(c_w_id, $w_id)) {
                            c_uid: uid
                            c_w_id
                            c_d_id
                            c_id
                            c_balance
                            c_delivery_cnt
                        }
                    }
                    '''
        customer_res = txn.query(customer_query, variables={'$w_id': str(order['o_w_id']),
                                                            '$d_id': str(order['o_d_id']),
                                                            '$c_id': str(order['o_c_id'])})
        customer_data = json.loads(customer_res.json)
        customer = customer_data['customer'][0]

        ops.append(deal_response.op_customer(customer, "r"))

        # update c_delivery_cnt and c_balance
        customer_update = {
            'uid': customer['c_uid'],
            'c_delivery_cnt': customer['c_delivery_cnt'] + 1,
            'c_balance': customer['c_balance'] + int(ol_amount)
        }
        txn.mutate(set_obj=customer_update)

        customer['c_delivery_cnt'] = customer['c_delivery_cnt'] + 1
        customer['c_balance'] = customer['c_balance'] + int(ol_amount)
        ops.append(deal_response.op_customer(customer, "w"))

        commit_response = txn.commit()
        txn2check = deal_response.get_txn_from_response(commit_response, ops)
        print(txn2check)
        return 1

    except Exception as e:
        txn.discard()
        raise e


def order_status_txn(client: DgraphClient, w_id: int, d_id: int, c_id: int = None, by_last_name: bool = False, c_last: str = None):
    ops = []
    txn = client.txn()
    try:
        # ---------------------- 1. query customer ----------------------
        if by_last_name:
            query_customers = '''
            query q($w_id: int, $d_id: int, $c_last: string) {
                customers(func: eq(c_last, $c_last)) @filter(eq(c_d_id, $d_id) AND eq(c_w_id, $w_id)) {
                    c_uid: uid
                    c_w_id
                    c_d_id
                    c_id
                    c_balance
                    c_delivery_cnt
                    c_first
                    c_middle
                    c_last
                }
            }
            '''
            res_customers = txn.query(query_customers, variables={'$w_id': str(w_id), '$d_id': str(d_id), '$c_last': str(c_last)})
            customers_data = json.loads(res_customers.json)

            if not customers_data['customers']:
                raise Exception("Customer not found")

            customers = sorted(customers_data['customers'], key=lambda x: x['c_first'])
            customer = customers[len(customers) // 2]
        else:
            query_customer = '''
            query q($w_id: int, $d_id: int, $c_id: int) {
                customer(func: eq(c_id, $c_id)) @filter(eq(c_d_id, $d_id) AND eq(c_w_id, $w_id)) {
                    c_uid: uid
                    c_w_id
                    c_d_id
                    c_id
                    c_balance
                    c_delivery_cnt
                    c_first
                    c_middle
                    c_last
                }
            }
            '''
            res_customer = txn.query(query_customer, variables={'$w_id': str(w_id), '$d_id': str(d_id), '$c_id': str(c_id)})
            customer_data = json.loads(res_customer.json)

            if not customer_data['customer']:
                raise Exception("Customer not found")

            customer = customer_data['customer'][0]

        ops.append(deal_response.op_customer(customer, "r"))

        customer_info = {
            "c_id": customer['c_id'],
            "c_first": customer['c_first'],
            "c_middle": customer['c_middle'],
            "c_last": customer['c_last'],
            "c_balance": customer['c_balance']
        }

        # ---------------------- 2. query an order ----------------------
        query_order = '''
        query q($w_id: int, $d_id: int, $c_id: int) {
            orders(func: eq(o_c_id, $c_id)) @filter(eq(o_d_id, $d_id) AND eq(o_w_id, $w_id)) {
                o_w_id
                o_d_id
                o_id
                o_carrier_id
                o_ol_cnt
                o_c_id
                o_entry_d
                o_all_local
            }
        }
        '''
        res_order = txn.query(query_order, variables={'$w_id': str(w_id), '$d_id': str(d_id), '$c_id': str(c_id)})
        orders_data = json.loads(res_order.json)

        if not orders_data['orders']:
            txn.commit()
            txn2check = deal_response.get_read_only_txn(txn._ctx.start_ts, ops)
            print(txn2check)
            return 1

        order = orders_data['orders'][0]

        ops.append(deal_response.op_order(order, "r"))

        order_info = {
            "o_id": order['o_id'],
            "o_entry_d": order['o_entry_d'],
            "o_carrier_id": order.get('o_carrier_id', None),
            "o_ol_cnt": order.get('o_ol_cnt')
        }

        for i in range(1, order_info['o_ol_cnt'] + 1):
            # ---------------------- 3. query order line ----------------------
            query_order_lines = '''
            query q($w_id: int, $d_id: int, $o_id: int, $ol_num: int) {
                order_lines(func: eq(ol_o_id, $o_id)) @filter(eq(ol_d_id, $d_id) AND eq(ol_w_id, $w_id) AND eq(ol_number, $ol_num)) {
                    ol_w_id
                    ol_d_id
                    ol_o_id
                    ol_number
                    ol_i_id
                    ol_delivery_d
                    ol_supply_w_id
                    ol_quantity
                    ol_amount
                    ol_dist_info
                }
            }
            '''
            res_order_lines = txn.query(query_order_lines, variables={'$w_id': str(w_id), '$d_id': str(d_id), '$o_id': str(order_info["o_id"]), '$ol_num': str(i)})
            order_lines_data = json.loads(res_order_lines.json)

            ops.append(deal_response.op_order_line(order_lines_data['order_lines'][0], "r"))

        txn.commit()
        txn2check = deal_response.get_read_only_txn(txn._ctx.start_ts, ops)
        print(txn2check)
        return 1

    except Exception as e:
        txn.discard()
        raise e


def stock_level_txn(client: DgraphClient, w_id: int, d_id: int, threshold: int, last_orders: int = 20):
    ops = []
    txn = client.txn()
    try:
        # ---------------------- 1. query district ----------------------
        district_query = '''
        query q($w_id: int, $d_id: int) {
            district(func: eq(d_id, $d_id)) @filter(eq(d_w_id, $w_id)) {
                d_w_id
                d_id
                d_ytd
                d_next_o_id
            }
        }
        '''
        res = txn.query(district_query, variables={'$w_id': str(w_id), '$d_id': str(d_id)})
        data = json.loads(res.json)
        if not data['district']:
            raise Exception("District not found")

        ops.append(deal_response.op_district(data['district'][0], "r"))

        next_o_id = data['district'][0]['d_next_o_id']
        min_o_id = next_o_id - last_orders  # Define the minimum order ID for the range

        # ---------------------- 2. query stock ----------------------
        low_stock_count = 0

        # 遍历最近的订单 ID
        for o_id in range(min_o_id, next_o_id):
            # ---------------------- 3. query order line ----------------------
            order_query = '''
            query q($w_id: int, $d_id: int, $o_id: int) {
                order(func: eq(o_id, $o_id)) @filter(eq(o_w_id, $w_id) AND eq(o_d_id, $d_id)) {
                    o_w_id
                    o_d_id
                    o_id
                    o_carrier_id
                    o_ol_cnt
                    o_c_id
                }
            }
            '''
            res = txn.query(order_query, variables={'$w_id': str(w_id), '$d_id': str(d_id), '$o_id': str(o_id)})
            order_data = json.loads(res.json)
            if not order_data['order']:
                continue

            ops.append(deal_response.op_order(order_data['order'][0], "r"))

            ol_count = order_data['order'][0].get('o_ol_cnt', 0)

            for ol_num in range(1, ol_count + 1):
                order_line_query = '''
                query q($w_id: int, $d_id: int, $o_id: int, $ol_num: int) {
                    order_lines(func: eq(ol_o_id, $o_id)) @filter(eq(ol_d_id, $d_id) AND eq(ol_w_id, $w_id) AND eq(ol_number, $ol_num)) {
                        ol_w_id
                        ol_d_id
                        ol_o_id
                        ol_number
                        ol_i_id
                        ol_delivery_d
                    }
                }
                '''
                res = txn.query(order_line_query, variables={'$w_id': str(w_id), '$d_id': str(d_id), '$o_id': str(o_id), '$ol_num': str(ol_num)})
                order_line_data = json.loads(res.json)
                order_line = order_line_data['order_lines'][0]

                ops.append(deal_response.op_order_line(order_line, "r"))

                # ---------------------- 4. query stock ----------------------
                stock_query = '''
                query q($w_id: int, $i_id: int) {
                    stock(func: eq(s_i_id, $i_id)) @filter(eq(s_w_id, $w_id)) {
                        s_w_id
                        s_i_id
                        s_quantity
                    }
                }
                '''
                res = txn.query(stock_query, variables={'$w_id': str(w_id), '$i_id': str(order_line['ol_i_id'])})
                stock_data = json.loads(res.json)

                if not stock_data['stock']:
                    continue  # Skip if no stock information is found

                ops.append(deal_response.op_stock(stock_data['stock'][0], "r"))

                if stock_data['stock'][0]['s_quantity'] < threshold:
                    low_stock_count += 1

        txn.commit()
        txn2check = deal_response.get_read_only_txn(txn._ctx.start_ts, ops)
        print(txn2check)
        return 1

    except Exception as e:
        txn.discard()
        raise e


def connect_to_dgraph():
    client_stub = pydgraph.DgraphClientStub("localhost:9080")
    client = pydgraph.DgraphClient(client_stub)
    return client


def main():
    client = connect_to_dgraph()

    w_id = 1
    for i in range(TOTAL_TXN_NUM):
        rand_num = random.random()
        if rand_num < 0.45:
            d_id = random.randint(1, TPCCConstants.DIST_PER_WARE)
            c_id = random.randint(1, TPCCConstants.CUST_PER_DIST)
            items = []
            for j in range(0, random.randint(5, 15)):
                items.append({
                    "supply_w_id": w_id,
                    "i_id": random.randint(1, TPCCConstants.MAXITEMS),
                    "quantity": random.randint(1, 10)
                })
            new_order_txn(client, w_id, d_id, c_id, items)
        elif rand_num < 0.88:
            d_id = random.randint(1, TPCCConstants.DIST_PER_WARE)
            c_id = random.randint(1, TPCCConstants.CUST_PER_DIST)
            payment_amount = random.randint(1, 5000)
            payment_txn(client, w_id, d_id, c_id, payment_amount)
        elif rand_num < 0.92:
            d_id = random.randint(1, TPCCConstants.DIST_PER_WARE)
            c_id = random.randint(1, TPCCConstants.CUST_PER_DIST)
            order_status_txn(client, w_id, d_id, c_id)
        elif rand_num < 0.96:
            carrier_id = random.randint(1, 10)
            delivery_txn(client, w_id, carrier_id)
        else:
            d_id = random.randint(1, TPCCConstants.DIST_PER_WARE)
            threshold = random.randint(10, 20)
            stock_level_txn(client, w_id, d_id, threshold)


if __name__ == "__main__":
    main()
