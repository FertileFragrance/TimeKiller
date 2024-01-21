package history.transaction;

import com.alibaba.fastjson.annotation.JSONField;

import java.io.Serializable;

public class HybridLogicalClock implements Comparable<HybridLogicalClock>, Serializable {
    @JSONField(name = "p")
    private final Long physical;
    @JSONField(name = "l")
    private final Long logical;

    public HybridLogicalClock(Long physical, Long logical) {
        this.physical = physical;
        this.logical = logical;
    }

    public Long getPhysical() {
        return physical;
    }

    public Long getLogical() {
        return logical;
    }

    @Override
    public String toString() {
        return String.format("HLC(%s, %s)", physical, logical);
    }

    @Override
    public int compareTo(HybridLogicalClock o) {
        if (this.physical.equals(o.physical)) {
            return this.logical.compareTo(o.logical);
        }
        return this.physical.compareTo(o.physical);
    }
}
