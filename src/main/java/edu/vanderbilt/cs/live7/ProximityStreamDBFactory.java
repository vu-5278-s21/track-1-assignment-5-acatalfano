package edu.vanderbilt.cs.live7;

import edu.vanderbilt.cs.live6.GeoHashFactory;
import edu.vanderbilt.cs.live6.ProximityDB;
import edu.vanderbilt.cs.live6.ProximityDBFactory;

public class ProximityStreamDBFactory {
    public <T> ProximityStreamDB<T> create(
        AttributesStrategy<T> strat,
        GeoHashFactory hashFactory,
        int bits
    ) {
        ProximityDB<T> proximityDB = (new ProximityDBFactory()).create(hashFactory, bits);
        return new NaiveProximityStreamDB<>(proximityDB, strat);
    }

}
