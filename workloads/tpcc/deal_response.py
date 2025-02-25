def get_timestamp_from_response(response):
    response = str(response).strip()
    timestamps = response.split('\n')
    start_ts = int(timestamps[0].split(': ')[1])
    commit_ts = int(timestamps[1].split(': ')[1])
    return start_ts, commit_ts


def op_warehouse(w, r_or_w):
    return {"t": r_or_w, "k": "warehouse:" + str(w["w_id"]), "v": str(w["w_tax"]) + "#" + str(w["w_ytd"])}


def op_district(d, r_or_w):
    return {"t": r_or_w, "k": "district:" + str(d["d_w_id"]) + "#" + str(d["d_id"]),
            "v": str(d["d_ytd"]) + "#" + str(d["d_next_o_id"])}


def op_customer(c, r_or_w):
    return {"t": r_or_w, "k": "customer:" + str(c["c_w_id"]) + "#" + str(c["c_d_id"]) + "#" + str(c["c_id"]),
            "v": str(c["c_balance"]) + "#" + str(c["c_delivery_cnt"])}


def op_order(o, r_or_w):
    return {"t": r_or_w, "k": "order:" + str(o["o_w_id"]) + "#" + str(o["o_d_id"]) + "#" + str(o["o_id"]),
            "v": str(o["o_carrier_id"]) + "#" + str(o["o_ol_cnt"]) + "#" + str(o["o_c_id"])}


def op_new_order(no, r_or_i_or_d):
    if r_or_i_or_d == "r":
        return {"t": "r", "k": "new_order:" + str(no["no_w_id"]) + "#" + str(no["no_d_id"]) + "#" + str(no["no_o_id"]),
                "v": 1}
    elif r_or_i_or_d == "i":
        return {"t": "w", "k": "new_order:" + str(no["no_w_id"]) + "#" + str(no["no_d_id"]) + "#" + str(no["no_o_id"]),
                "v": 1}
    else:
        return {"t": "w", "k": "new_order:" + str(no["no_w_id"]) + "#" + str(no["no_d_id"]) + "#" + str(no["no_o_id"]),
                "v": 0}


def op_stock(s, r_or_w):
    return {"t": r_or_w, "k": "stock:" + str(s["s_w_id"]) + "#" + str(s["s_i_id"]), "v": s["s_quantity"]}


def op_order_line(ol, r_or_w):
    ol_delivery_d = str(ol["ol_delivery_d"])
    ol_delivery_d = ol_delivery_d.split('.')[0]
    if ol_delivery_d.endswith('Z'):
        ol_delivery_d = ol_delivery_d[:-1]
    return {"t": r_or_w, "k": "order_line:" + str(ol["ol_w_id"]) + "#" + str(ol["ol_d_id"]) + "#" + str(ol["ol_o_id"]) + "#" + str(ol["ol_number"]),
            "v": str(ol["ol_i_id"]) + "#" + ol_delivery_d}


def get_txn_from_response(response, ops):
    start_ts, commit_ts = get_timestamp_from_response(response)
    return {
        "sid": 0,
        "sts": {"p": start_ts, "l": 0},
        "cts": {"p": commit_ts, "l": 0},
        "ops": ops,
    }


def get_read_only_txn(start_ts, ops):
    return {
        "sid": 0,
        "sts": {"p": start_ts, "l": 0},
        "cts": {"p": start_ts, "l": 0},
        "ops": ops,
    }
