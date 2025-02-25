import pydgraph
import deal_response


def create_client_stub():
    return pydgraph.DgraphClientStub("localhost:9080")


def create_client(client_stub):
    return pydgraph.DgraphClient(client_stub)


client_stub = create_client_stub()
client = create_client(client_stub)


def drop_all():
    client.alter(pydgraph.Operation(drop_all=True))


def insert_warehouse(w_id, w_name, w_street_1, w_street_2, w_city, w_state, w_zip, w_tax, w_ytd):
    txn = client.txn()
    mutation = {
        #"set": [{
            "w_id": w_id,
            "w_name": w_name,
            "w_street_1": w_street_1,
            "w_street_2": w_street_2,
            "w_city": w_city,
            "w_state": w_state,
            "w_zip": w_zip,
            "w_tax": w_tax,
            "w_ytd": w_ytd,
            "dgraph.type": 'Warehouse'
        #}]
    }
    txn.mutate(set_obj=mutation)
    response = txn.commit()
    ops = [deal_response.op_warehouse(mutation, "w")]
    print(deal_response.get_txn_from_response(response, ops))


def insert_district(d_id, d_w_id, d_name, d_street_1, d_street_2, d_city, d_state, d_zip, d_tax, d_ytd, d_next_o_id):
    txn = client.txn()
    mutation = {
        #"set": [{
            "d_id": d_id,
            "d_name": d_name,
            "d_street_1": d_street_1,
            "d_street_2": d_street_2,
            "d_city": d_city,
            "d_state": d_state,
            "d_zip": d_zip,
            "d_tax": d_tax,
            "d_ytd": d_ytd,
            "d_next_o_id": d_next_o_id,
            "d_w_id": d_w_id,
            "dgraph.type": 'District'
        #}]
    }
    txn.mutate(set_obj=mutation)
    response = txn.commit()
    ops = [deal_response.op_district(mutation, "w")]
    print(deal_response.get_txn_from_response(response, ops))


def insert_customer(c_id, c_d_id, c_w_id, c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_since, c_credit, c_credit_lim, c_discount, c_balance, c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_data):
    txn = client.txn()
    mutation = {
        #"set": [{
            "c_id": c_id,
            "c_first": c_first,
            "c_middle": c_middle,
            "c_last": c_last,
            "c_street_1": c_street_1,
            "c_street_2": c_street_2,
            "c_city": c_city,
            "c_state": c_state,
            "c_zip": c_zip,
            "c_phone": c_phone,
            "c_since": c_since,
            "c_credit": c_credit,
            "c_credit_lim": c_credit_lim,
            "c_discount": c_discount,
            "c_balance": c_balance,
            "c_ytd_payment": c_ytd_payment,
            "c_payment_cnt": c_payment_cnt,
            "c_delivery_cnt": c_delivery_cnt,
            "c_data": c_data,
            "c_d_id": c_d_id,
            "c_w_id": c_w_id,
            "dgraph.type": 'Customer'
        #}]
    }
    txn.mutate(set_obj=mutation)
    response = txn.commit()
    ops = [deal_response.op_customer(mutation, "w")]
    print(deal_response.get_txn_from_response(response, ops))


def insert_order(o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local):
    txn = client.txn()
    mutation = {
        #"set": [{
            "o_id": o_id,
            "o_entry_d": o_entry_d,
            "o_carrier_id": -1,
            "o_ol_cnt": o_ol_cnt,
            "o_all_local": o_all_local,
            "o_c_id": o_c_id,
            "o_d_id": o_d_id,
            "o_w_id": o_w_id,
            "dgraph.type": 'Order'
        #}]
    }
    txn.mutate(set_obj=mutation)
    response = txn.commit()
    ops = [deal_response.op_order(mutation, "w")]
    print(deal_response.get_txn_from_response(response, ops))
    

def insert_orderline(ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info, ol_delivery_d):
    txn = client.txn()
    mutation = {
        #"set": [{
            "ol_number": ol_number,
            "ol_i_id": ol_i_id,
            "ol_supply_w_id": ol_supply_w_id,
            "ol_delivery_d": ol_delivery_d,
            "ol_quantity": ol_quantity,
            "ol_amount": ol_amount,
            "ol_dist_info": ol_dist_info,
            "ol_o_id": ol_o_id,
            "ol_d_id": ol_d_id,
            "ol_w_id": ol_w_id,
            "dgraph.type": 'OrderLine'
        #}]
    }
    txn.mutate(set_obj=mutation)
    response = txn.commit()
    ops = [deal_response.op_order_line(mutation, "w")]
    print(deal_response.get_txn_from_response(response, ops))


def insert_neworder(no_o_id, no_d_id, no_w_id):
    txn = client.txn()
    mutation = {
        #"set": [{
            "no_o_id": no_o_id,
            "no_d_id": no_d_id,
            "no_w_id": no_w_id,
            "dgraph.type": 'NewOrder'
        #}]
    }
    txn.mutate(set_obj=mutation)
    response = txn.commit()
    ops = [deal_response.op_new_order(mutation, "i")]
    print(deal_response.get_txn_from_response(response, ops))


def insert_item(i_id, i_im_id, i_name, i_price, i_data):
    txn = client.txn()
    mutation = {
        #"set": [{
            "i_id": i_id,
            "i_name": i_name,
            "i_price": i_price,
            "i_im_id": i_im_id,
            "i_data": i_data,
            "dgraph.type": 'Item'
        #}]
    }
    txn.mutate(set_obj=mutation)
    txn.commit()


def insert_stock(s_i_id, s_w_id, s_quantity, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10, s_ytd, s_order_cnt, s_remote_cnt, s_data):
    txn = client.txn()
    mutation = {
        #"set": [{
            "s_quantity": s_quantity,
            "s_ytd": s_ytd,
            "s_order_cnt": s_order_cnt,
            "s_remote_cnt": s_remote_cnt,
            "s_data": s_data,
            "s_dist_01": s_dist_01,
            "s_dist_02": s_dist_02,
            "s_dist_03": s_dist_03,
            "s_dist_04": s_dist_04,
            "s_dist_05": s_dist_05,
            "s_dist_06": s_dist_06,
            "s_dist_07": s_dist_07,
            "s_dist_08": s_dist_08,
            "s_dist_09": s_dist_09,
            "s_dist_10": s_dist_10,
            "s_i_id": s_i_id,
            "s_w_id": s_w_id,
            "dgraph.type": 'Stock'
        #}]
    }
    txn.mutate(set_obj=mutation)
    response = txn.commit()
    ops = [deal_response.op_stock(mutation, "w")]
    print(deal_response.get_txn_from_response(response, ops))


def insert_history(h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_date, h_amount, h_data):
    txn = client.txn()
    mutation = {
        #"set": [{
            "h_date": h_date,
            "h_amount": h_amount,
            "h_data": h_data,
            "h_c_id": h_c_id,
            "h_c_d_id": h_c_d_id,
            "h_c_w_id": h_c_w_id,
            "h_d_id": h_d_id,
            "h_w_id": h_w_id,
            "dgraph.type": 'History'
        #}]
    }
    txn.mutate(set_obj=mutation)
    txn.commit()


def set_schema():
    schema = '''
    w_id: int @index(int) .
    w_name: string .
    w_street_1: string .
    w_street_2: string .
    w_city: string .
    w_state: string .
    w_zip: string .
    w_tax: int .
    w_ytd: int .
    type Warehouse {
        w_id
        w_name
        w_street_1
        w_street_2
        w_city
        w_state
        w_zip
        w_tax
        w_ytd
    }

    d_id: int @index(int) .
    d_name: string .
    d_street_1: string .
    d_street_2: string .
    d_city: string .
    d_state: string .
    d_zip: string .
    d_tax: int .
    d_ytd: int .
    d_next_o_id: int @index(int) .
    d_w_id: int @index(int) .
    type District {
        d_id
        d_name
        d_street_1
        d_street_2
        d_city
        d_state
        d_zip
        d_tax
        d_ytd
        d_next_o_id
        d_w_id
    }

    c_id: int @index(int) .
    c_first: string .
    c_middle: string .
    c_last: string @index(hash) .
    c_street_1: string .
    c_street_2: string .
    c_city: string .
    c_state: string .
    c_zip: string .
    c_phone: string .
    c_since: datetime .
    c_credit: string .
    c_credit_lim: float .
    c_discount: float .
    c_balance: int .
    c_ytd_payment: float .
    c_payment_cnt: int @index(int) .
    c_delivery_cnt: int @index(int) .
    c_data: string .
    c_d_id: int @index(int) .
    c_w_id: int @index(int) .
    type Customer {
        c_id
        c_first
        c_middle
        c_last
        c_street_1
        c_street_2
        c_city
        c_state
        c_zip
        c_phone
        c_since
        c_credit
        c_credit_lim
        c_discount
        c_balance
        c_ytd_payment
        c_payment_cnt
        c_delivery_cnt
        c_data
        c_d_id
        c_w_id
    }

    o_id: int @index(int) .
    o_entry_d: datetime .
    o_carrier_id: int @index(int) .
    o_ol_cnt: int @index(int) .
    o_all_local: bool .
    o_c_id: int @index(int) .
    o_d_id: int @index(int) .
    o_w_id: int @index(int) .
    type Order {
        o_id
        o_entry_d
        o_carrier_id
        o_ol_cnt
        o_all_local
        o_c_id
        o_d_id
        o_w_id
    }

    ol_number: int @index(int) .
    ol_i_id: int @index(int) .
    ol_supply_w_id: int @index(int) .
    ol_delivery_d: datetime .
    ol_quantity: int @index(int) .
    ol_amount: float .
    ol_dist_info: string .
    ol_o_id: int @index(int) .
    ol_d_id: int @index(int) .
    ol_w_id: int @index(int) .
    type OrderLine {
        ol_number
        ol_i_id
        ol_supply_w_id
        ol_delivery_d
        ol_quantity
        ol_amount
        ol_dist_info
        ol_o_id
        ol_d_id
        ol_w_id
    }

    no_o_id: int @index(int) .
    no_d_id: int @index(int) .
    no_w_id: int @index(int) .
    type NewOrder {
        no_o_id
        no_d_id
        no_w_id
    }

    i_id: int @index(int) .
    i_name: string .
    i_price: float .
    i_im_id: int @index(int) .
    i_data: string .
    type Item {
        i_id
        i_name
        i_price
        i_im_id
        i_data
    }

    s_quantity: int @index(int) .
    s_ytd: int @index(int) .
    s_order_cnt: int @index(int) .
    s_remote_cnt: int @index(int) .
    s_data: string .
    s_dist_01: string .
    s_dist_02: string .
    s_dist_03: string .
    s_dist_04: string .
    s_dist_05: string .
    s_dist_06: string .
    s_dist_07: string .
    s_dist_08: string .
    s_dist_09: string .
    s_dist_10: string .
    s_i_id: int @index(int) .
    s_w_id: int @index(int) .
    type Stock {
        s_quantity
        s_ytd
        s_order_cnt
        s_remote_cnt
        s_data
        s_dist_01
        s_dist_02
        s_dist_03
        s_dist_04
        s_dist_05
        s_dist_06
        s_dist_07
        s_dist_08
        s_dist_09
        s_dist_10
        s_i_id
        s_w_id
    }

    h_date: datetime .
    h_amount: float .
    h_data: string .
    h_c_id: int @index(int) .
    h_c_d_id: int @index(int) .
    h_c_w_id: int @index(int) .
    h_d_id: int @index(int) .
    h_w_id: int @index(int) .
    type History {
        h_date
        h_amount
        h_data
        h_c_id
        h_c_d_id
        h_c_w_id
        h_d_id
        h_w_id
    }
    '''
    op = pydgraph.Operation(schema=schema)
    return client.alter(op)
