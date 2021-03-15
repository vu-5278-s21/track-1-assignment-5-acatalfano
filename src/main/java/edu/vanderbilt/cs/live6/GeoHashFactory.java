package edu.vanderbilt.cs.live6;

public interface GeoHashFactory {

    /**
     * @ToDo
     *
     * You will need to create an implementation of this interface that
     * knows how to create your new GeoHash objects.
     *
     * @param lat
     * @param lon
     * @param bitsOfPrecision
     * @return
     */
    public GeoHash with(double lat, double lon, int bitsOfPrecision);

}
