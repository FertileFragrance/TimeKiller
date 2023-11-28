package reader;

import history.History;
import violation.Violation;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;

public interface Reader<KeyType, ValueType> {
    Pair<History<KeyType, ValueType>, ArrayList<Violation>> read(String filepathOrJsonString);
}
