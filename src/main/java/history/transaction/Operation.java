package history.transaction;

import com.alibaba.fastjson.annotation.JSONField;

import java.util.Objects;

public class Operation<KeyType, ValueType> {
    @JSONField(name = "t")
    private final OpType type;
    @JSONField(name = "k")
    private final KeyType key;
    @JSONField(name = "v")
    private ValueType value;

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

    public void setValue(ValueType value) {
        this.value = value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Operation<?, ?> operation = (Operation<?, ?>) o;
        return type == operation.type && key.equals(operation.key) && Objects.equals(value, operation.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, key, value);
    }

    @Override
    public String toString() {
        return String.format("%s(%s, %s)", type.toString().charAt(0), key, value);
    }
}
