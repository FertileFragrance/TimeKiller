package checker;

import history.History;

public interface OnlineChecker extends Checker {
    <KeyType, ValueType> int gc(History<KeyType, ValueType> history);
}
