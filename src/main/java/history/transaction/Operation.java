package history.transaction;

public class Operation<KeyType, ValueType> {
    private final OpType type;
    private final KeyType key;
    private final ValueType value;

    public Operation(OpType type, KeyType key, ValueType value) {
        this.type = type;
        this.key = key;
        this.value = value;
    }

    public OpType getType() {
        return type;
    }

    public KeyType getKey() {
        return key;
    }

    public ValueType getValue() {
        return value;
    }

    @Override
    public String toString() {
        return String.format("%s(%s, %s)", type.toString().charAt(0), key, value);
    }
}
