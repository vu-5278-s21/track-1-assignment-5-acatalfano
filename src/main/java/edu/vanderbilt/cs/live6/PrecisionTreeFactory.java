package edu.vanderbilt.cs.live6;

import java.util.Collection;

public interface PrecisionTreeFactory<T extends Collection<?>> {
    PrecisionTree<T> with(int resolution);
}
