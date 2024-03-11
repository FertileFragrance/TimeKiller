package history.transaction;

import java.io.Serializable;

public class TidVal<ValueType> implements Serializable {
    private String tid;
    private ValueType val;

    public TidVal(String tid, ValueType val) {
        this.tid = tid;
        this.val = val;
    }

    public TidVal() {}

    public String getTid() {
        return tid;
    }

    public ValueType getVal() {
        return val;
    }

    public String getLeft() {
        return tid;
    }

    public ValueType getRight() {
        return val;
    }
}
