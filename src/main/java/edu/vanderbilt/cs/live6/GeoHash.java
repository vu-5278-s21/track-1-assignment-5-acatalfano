package edu.vanderbilt.cs.live6;

public interface GeoHash extends Iterable<Boolean> {

    public int bitsOfPrecision();

    /**
     * Similar to "substring" on Strings. This method should
     * return the first n bits of the GeoHash as a new
     * GeoHash.
     *
     * @param n
     * @return
     */
    public GeoHash prefix(int n);

    // @Bonus, this is not required, but is a nice challenge
    // for bonus points
    public GeoHash northNeighbor();

    // @Bonus, this is not required, but is a nice challenge
    // for bonus points
    public GeoHash southNeighbor();

    // @Bonus, this is not required, but is a nice challenge
    // for bonus points
    public GeoHash westNeighbor();

    // @Bonus, this is not required, but is a nice challenge
    // for bonus points
    public GeoHash eastNeighbor();



}
