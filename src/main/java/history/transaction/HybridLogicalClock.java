package history.transaction;

public class HybridLogicalClock implements Comparable<HybridLogicalClock> {
    private final Long physical;
    private final Long logical;

    public HybridLogicalClock(Long physical, Long logical) {
        this.physical = physical;
        this.logical = logical;
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
