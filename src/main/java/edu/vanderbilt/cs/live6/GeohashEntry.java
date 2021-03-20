package edu.vanderbilt.cs.live6;

import java.util.Objects;

public class GeohashEntry<T> {
    private final DataAndPosition<T> dataAndPosition;
    private final GeoHash geohash;

    public GeohashEntry(
        DataAndPosition<T> dataAndPosition,
        GeoHash geohash
    ) {
        this.dataAndPosition = dataAndPosition;
        this.geohash = geohash;
    }

    public DataAndPosition<T> getDataAndPosition() {
        return dataAndPosition;
    }

    @Override
    public int hashCode() {
        return Objects.hash(geohash, dataAndPosition.getData());
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof GeohashEntry<?>)) {
            return false;
        }
        GeohashEntry<?> geohashEntry = (GeohashEntry<?>)o;
        return geohash.equals(geohashEntry.geohash)
            && dataAndPosition
                .getData()
                .equals(geohashEntry.dataAndPosition.getData());
    }
}
