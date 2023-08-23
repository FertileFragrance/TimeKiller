package violation;

public abstract class Violation {
    protected ViolationType type;

    public abstract void fix();
}
