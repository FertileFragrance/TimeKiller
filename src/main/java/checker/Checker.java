package checker;

import history.History;
import violation.Violation;

import java.util.ArrayList;

public interface Checker {
    <KeyType, ValueType> ArrayList<Violation> check(History<KeyType, ValueType> history);

    <KeyType, ValueType> void saveToFile(History<KeyType, ValueType> history);
}
