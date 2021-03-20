package edu.vanderbilt.cs.live6;

import java.util.ArrayList;
import java.util.Collection;

public class ArrayListPrecisionTreeFactory<T> implements
    PrecisionTreeFactory<Collection<T>> {
    @Override
    public PrecisionTree<Collection<T>> with(int resolution) {
        return new ArrayPrecisionTree<>(resolution, ArrayList::new);
    }
}
