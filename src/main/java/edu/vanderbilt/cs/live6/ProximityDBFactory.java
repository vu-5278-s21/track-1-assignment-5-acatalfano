package edu.vanderbilt.cs.live6;

import java.util.Collection;

public class ProximityDBFactory {
    /**
     * @param <T>
     * 
     * @return
     */
    public <T> ProximityDB<T> create(GeoHashFactory hashFactory, int bits) {
        final PrecisionTreeFactory<Collection<GeohashEntry<T>>> precisionTreeFactory =
            new ArrayListPrecisionTreeFactory<>();
        return new ProximityDbTree<>(precisionTreeFactory, hashFactory, bits);
    }

}
