package edu.vanderbilt.cs.live6;

import java.util.Collection;
import java.util.stream.Stream;

public interface PrecisionTree<T extends Collection<?>> {
    Stream<T> itemsWithinRange(String locationCodePrefix, int precision);
    T itemsAtLocation(String locationCode);
}
