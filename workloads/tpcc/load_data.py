import random
import string
from datetime import datetime

import dgraph_impl


class TPCCConstants:
    # Transaction types
    TXN_TYPE = {
        'TXN_NEWORDER': 1,
        'TXN_PAYMENT': 2,
        'TXN_ORDERSTATUS': 3,
        'TXN_DELIVERY': 4,
        'TXN_STOCKLEVEL': 5
    }

    # TPCC constants
    MAXITEMS = 10000
    CUST_PER_DIST = 300
    DIST_PER_WARE = 10
    ORD_PER_DIST = 300

    # Table prefixes
    TBL_Warehouse = "WAREHOUSE"
    TBL_District = "DISTRICT"
    TBL_Customer = "CUSTOMER"
    TBL_History = "HISTORY"
    TBL_NewOrder = "NEWORDER"
    TBL_Order = "ORDER"
    TBL_OrderLine = "ORDERLINE"
    TBL_Item = "ITEM"
    TBL_Stock = "STOCK"


class Utils:
    r = random.Random()
    alphanum = string.digits + string.ascii_uppercase + string.ascii_lowercase
    numeric = string.digits
    random_C_C_LAST = r.randint(0, 255)
    random_C_C_ID = r.randint(0, 1023)
    random_C_OL_I_ID = r.randint(0, 8191)

    @staticmethod
    def RandomNumber(x, y):
        assert x <= y
        return random.randint(x, y)

    @staticmethod
    def NURand(A, x, y):
        C = 0
        if A == 255:
            C = Utils.random_C_C_LAST
        elif A == 1023:
            C = Utils.random_C_C_ID
        elif A == 8191:
            C = Utils.random_C_OL_I_ID
        else:
            print(f"wrong A[{A}] in NURand")
            assert False
        return (((Utils.RandomNumber(0, A) | Utils.RandomNumber(x, y)) + C) % (y - x + 1)) + x

    @staticmethod
    def Lastname(num):
        names = ["BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING"]
        ret = names[(num % 1000) // 100]
        ret += names[(num // 10) % 10]
        ret += names[num % 10]
        return ret

    @staticmethod
    def MakeAlphaString(x, y):
        len_str = Utils.RandomNumber(x, y)
        return ''.join(random.choice(Utils.alphanum) for _ in range(len_str))

    @staticmethod
    def MakeNumberString(x, y):
        len_str = Utils.RandomNumber(x, y)
        return ''.join(random.choice(Utils.numeric) for _ in range(len_str))

    @staticmethod
    def MakeAddress():
        a = [
            Utils.MakeAlphaString(10, 20),  # Street 1
            Utils.MakeAlphaString(10, 20),  # Street 2
            Utils.MakeAlphaString(10, 20),  # City
            Utils.MakeAlphaString(2, 2),    # State
            Utils.MakeNumberString(9, 9)    # Zip
        ]
        return a

    @staticmethod
    def MakeTimeStamp():
        return datetime.now().isoformat()

    @staticmethod
    def printProgress(x, total):
        l = 20
        num = x * l // total
        if (x * l) % total != 0:
            return
        a = ['*' if i <= num else ' ' for i in range(l)]
        print(f"|{''.join(a)}|\r", end="")

    @staticmethod
    def shuffleArray(a):
        random.shuffle(a)

    @staticmethod
    def test():
        for i in range(20):
            print(f"{i}: ")
            print(Utils.RandomNumber(10, 100))
            print(Utils.MakeNumberString(10, 30))
            print(Utils.MakeAlphaString(10, 20))
            print(Utils.Lastname(Utils.RandomNumber(0, 1000)))


class LoadData:
    def __init__(self, w_id):
        self.orig = [False] * TPCCConstants.MAXITEMS
        self.w_id = w_id
        self.lastname2customer = {}

    def loadItems(self):
        if self.w_id != 1:
            # all warehouses share a same item table
            return
        print("Loading item...")

        for i in range(TPCCConstants.MAXITEMS):
            self.orig[i] = False

        # 10% items are original
        for i in range(TPCCConstants.MAXITEMS // 10):
            pos = -1
            while pos == -1 or self.orig[pos]:
                pos = random.randint(0, TPCCConstants.MAXITEMS - 1)
            self.orig[pos] = True

        for i_id in range(1, TPCCConstants.MAXITEMS + 1):
            # Generate Item Data
            i_name = Utils.MakeAlphaString(14, 24)
            i_price = random.randint(100, 10000) / 100.0
            i_data = Utils.MakeAlphaString(26, 50)
            idatasiz = len(i_data)
            if self.orig[i_id - 1]:
                pos = random.randint(0, idatasiz - 8)
                i_data = i_data[:pos] + "ORIGINAL" + i_data[pos + 8:]
            #self.beginTxn()
            dgraph_impl.insert_item(i_id, 0, i_name, i_price, i_data)
            #self.commitTxn()

        print("Item done")

    def loadWare(self, w_id):
        w_name = Utils.MakeAlphaString(6, 10)
        address = Utils.MakeAddress()
        w_tax = random.randint(10, 20)
        w_ytd = 3000000
        #self.beginTxn()
        dgraph_impl.insert_warehouse(w_id, w_name, address[0], address[1], address[2], address[3], address[4], w_tax, w_ytd)
        #self.commitTxn()

        self.Stock(w_id)
        self.District(w_id)

    def loadCust(self, w_id):
        print(f"Loading customer for  wid: {w_id} ...")
        for d_id in range(1, TPCCConstants.DIST_PER_WARE + 1):
            self.Customer(d_id, w_id)

    def Customer(self, d_id, w_id):
        self.lastname2customer = {Utils.Lastname(i): [] for i in range(1, 1001)}
        for c_id in range(1, TPCCConstants.CUST_PER_DIST + 1):
            c_first = Utils.MakeAlphaString(8, 16)
            c_middle = "OE"
            c_last = Utils.Lastname(c_id) if c_id <= 1000 else Utils.Lastname(Utils.NURand(255, 0, 999))
            address = Utils.MakeAddress()
            c_phone = Utils.MakeNumberString(16, 16)
            c_credit = "G" if random.randint(0, 1) == 1 else "B"
            c_credit += "C"
            c_cred_lim = 50000
            c_discount = random.randint(0, 50) / 100.0
            c_balance = -10
            c_data = Utils.MakeAlphaString(300, 500)
            c_since = Utils.MakeTimeStamp()

            #self.beginTxn()
            dgraph_impl.insert_customer(c_id, d_id, w_id, c_first, c_middle, c_last, *address, c_phone, c_since, c_credit,
                                c_cred_lim, c_discount, c_balance, 10.0, 1, 0, c_data)
            #self.commitTxn()
            # update Customer secondary Index
            self.lastname2customer[c_last].append(str(c_id))

    def loadOrd(self, w_id):
        print(f"Loading Orders for  W={w_id}")
        for d_id in range(1, TPCCConstants.DIST_PER_WARE + 1):
            self.Orders(d_id, w_id)

    def Orders(self, d_id, w_id):
        cids = [i + 1 for i in range(TPCCConstants.ORD_PER_DIST)]
        random.shuffle(cids)
        for o_id in range(1, TPCCConstants.ORD_PER_DIST + 1):
            o_c_id = cids[o_id - 1]
            o_carrier_id = random.randint(1, 10)
            o_ol_cnt = random.randint(5, 15)

            #self.beginTxn()
            dgraph_impl.insert_order(o_id, d_id, w_id, o_c_id, Utils.MakeTimeStamp(), o_ol_cnt, True)
            if o_id >= TPCCConstants.ORD_PER_DIST * 0.7:
                dgraph_impl.insert_neworder(o_id, d_id, w_id)
            # generate order line data
            for ol in range(1, o_ol_cnt + 1):
                ol_i_id = random.randint(1, TPCCConstants.MAXITEMS)
                ol_supply_id = w_id
                ol_quantity = 5
                ol_amount = 0.0
                ol_dist_info = Utils.MakeAlphaString(24, 24)
                if o_id >= TPCCConstants.ORD_PER_DIST * 0.7:
                    dgraph_impl.insert_orderline(o_id, d_id, w_id, ol, ol_i_id, ol_supply_id, ol_quantity, ol_amount, ol_dist_info, Utils.MakeTimeStamp())
                else:
                    ol_amount = random.randint(10, 10000) / 100.0
                    dgraph_impl.insert_orderline(o_id, d_id, w_id, ol, ol_i_id, ol_supply_id, ol_quantity, ol_amount, ol_dist_info, Utils.MakeTimeStamp())
            #self.commitTxn()

    def Stock(self, w_id):
        print(f"Loading stock for w_id: {w_id}")
        s_w_id = w_id
        for i in range(TPCCConstants.MAXITEMS):
            self.orig[i] = False
        for i in range(TPCCConstants.MAXITEMS // 10):
            pos = -1
            while pos == -1 or self.orig[pos]:
                pos = random.randint(0, TPCCConstants.MAXITEMS - 1)
            self.orig[pos] = True
        for s_i_id in range(1, TPCCConstants.MAXITEMS + 1):
            s_quantity = random.randint(10, 100)
            s_dist_xx = [Utils.MakeAlphaString(24, 24) for _ in range(10)]
            s_data = Utils.MakeAlphaString(26, 50)
            sdatasiz = len(s_data)
            if self.orig[s_i_id - 1]:
                pos = random.randint(0, sdatasiz - 8)
                s_data = s_data[:pos] + "ORIGINAL" + s_data[pos + 8:]

            #self.beginTxn()
            dgraph_impl.insert_stock(s_i_id, s_w_id, s_quantity, *s_dist_xx, 0, 0, 0, s_data)
            #self.commitTxn()

    def District(self, w_id):
        print(f"Loading District for w_id: {w_id}")
        #self.beginTxn()
        d_w_id = w_id
        d_ytd = 30000
        d_next_o_id = TPCCConstants.ORD_PER_DIST + 1
        for d_id in range(1, TPCCConstants.DIST_PER_WARE + 1):
            d_name = Utils.MakeAlphaString(6, 10)
            address = Utils.MakeAddress()
            d_tax = random.randint(10, 20)
            dgraph_impl.insert_district(d_id, d_w_id, d_name, *address, d_tax, d_ytd, d_next_o_id)
        #self.commitTxn()

    def loadAll(self):
        self.loadItems()
        self.loadWare(self.w_id)
        self.loadCust(self.w_id)
        self.loadOrd(self.w_id)


if __name__ == '__main__':
    dgraph_impl.drop_all()
    dgraph_impl.set_schema()
    for i in range(0, 1):
        load_data = LoadData(i + 1)
        load_data.loadAll()
