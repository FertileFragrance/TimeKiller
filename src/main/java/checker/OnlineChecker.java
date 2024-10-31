package checker;

import history.History;

public interface OnlineChecker extends Checker {
    <KeyType, ValueType> int gc(History<KeyType, ValueType> history);

    <KeyType, ValueType> void preGc(History<KeyType, ValueType> history);
    void performGc();
    <KeyType, ValueType> int postGc(History<KeyType, ValueType> history);
}
