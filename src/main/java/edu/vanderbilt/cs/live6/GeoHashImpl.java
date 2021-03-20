package edu.vanderbilt.cs.live6;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The original version of GeoHash failed to properly encapsulate the representation of
 * the GeoHash from consumers of the hashes. This class shields users from the underlying
 * representation by properly encapsulating the underlying representation. Please use your
 * original GeoHash work to complete this class.
 *
 * After you complete this class, go and complete the GeoDBFactory, GeoHashFactory, and
 * DefaultGeoDBConfiguration.
 *
 * You will need to update your implementation of GeoDB to use a GeoHashFactory to produce
 * geohash objects when the insert, delete, nearby, contains, etc. methods are called. You
 * cannot directly call your old GeoHash class or assume that your implementation of the
 * GeoHash will be provided to your GeoDB.
 *
 * There are some items marked with @Bonus that are not required. However, if you want an
 * additional challenge, knock these items out AFTER completing everything else.
 *
 */
public class GeoHashImpl implements GeoHash {
    private final List<Boolean> geoHash;
    private static final int BITS_IN_INTEGER = 32;

    public GeoHashImpl(
        double lat,
        double lon,
        int bitsOfPrecision
    ) {
        geoHash = GeoHashInternal.geohash(lat, lon, bitsOfPrecision);
    }

    private GeoHashImpl(
        List<Boolean> latitudeGeohash,
        List<Boolean> longitudeGeohash
    ) {
        geoHash = GeoHashInternal.geohash(latitudeGeohash, longitudeGeohash);
    }

    private GeoHashImpl(List<Boolean> geohash) {
        geoHash = geohash;
    }

    public int bitsOfPrecision() {
        return -1;
    }

    /**
     * Similar to "substring" on Strings. This method should return the first n bits of
     * the GeoHash as a new GeoHash.
     *
     * @param n
     * 
     * @return
     */
    public GeoHashImpl prefix(int n) {
        return new GeoHashImpl(geoHash.subList(0, n));
    }

    public GeoHashImpl northNeighbor() {
        List<Boolean> latitudeGeohash = CircularArithmetic.increment(latitudeHash());
        List<Boolean> longitudeGeohash = longitudeHash();
        return new GeoHashImpl(latitudeGeohash, longitudeGeohash);
    }

    public GeoHashImpl southNeighbor() {
        List<Boolean> latitudeGeohash = CircularArithmetic.decrement(latitudeHash());
        List<Boolean> longitudeGeohash = longitudeHash();
        return new GeoHashImpl(latitudeGeohash, longitudeGeohash);
    }

    public GeoHashImpl westNeighbor() {
        List<Boolean> latitudeGeohash = latitudeHash();
        List<Boolean> longitudeGeohash = CircularArithmetic.decrement(longitudeHash());
        return new GeoHashImpl(latitudeGeohash, longitudeGeohash);
    }

    public GeoHashImpl eastNeighbor() {
        List<Boolean> latitudeGeohash = latitudeHash();
        List<Boolean> longitudeGeohash = CircularArithmetic.increment(longitudeHash());
        return new GeoHashImpl(latitudeGeohash, longitudeGeohash);
    }

    @Override
    public Iterator<Boolean> iterator() {
        return geoHash.iterator();
    }

    @Override
    public boolean equals(Object o) {
        if (o == this)
            return true;
        if (!(o instanceof GeoHashImpl)) {
            return false;
        }
        GeoHashImpl geoHashImpl = (GeoHashImpl)o;
        return Objects.equals(geoHash, geoHashImpl.geoHash);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(geoHash);
    }

    public String toString() {
        return GeoHashImpl.hashAsString(geoHash);
    }

    private static String hashAsString(List<Boolean> hash) {
        return hash
            .stream()
            .reduce(
                "",
                (accum, bit) -> accum + (bit ? '1' : '0'),
                (lhs, rhs) -> lhs + rhs
            );
    }

    private List<Boolean> latitudeHash() {
        List<Boolean> latHash = new ArrayList<>();
        for(int i = 0; i < geoHash.size(); i += 2) {
            latHash.add(geoHash.get(i));
        }
        return latHash;
    }

    private List<Boolean> longitudeHash() {
        List<Boolean> lngHash = new ArrayList<>();
        for(int i = 1; i < geoHash.size(); i += 2) {
            lngHash.add(geoHash.get(i));
        }
        return lngHash;
    }

    private static class CircularArithmetic {
        public static List<Boolean> increment(List<Boolean> list) {
            return wrapAdd(list, 1);
        }

        public static List<Boolean> decrement(List<Boolean> list) {
            return wrapAdd(list, -1);
        }



        /**
         * convenience function for handling the data conversions
         * 
         * @param lhsList
         * @param rhs
         */
        private static List<Boolean> wrapAdd(List<Boolean> lhsList, int rhs) {
            return wrapAdd(hashAsString(lhsList), rhs)
                .chars()
                .mapToObj(intChar -> ((char)intChar) == '1')
                .collect(Collectors.toList());
        }

        /**
         * 
         * emulate truncated 2's-complement addition between a String (parsed as a binary
         * int) and an int the result is inverse-truncated string (leading bits are
         * dropped) so that it is the same length as the input string
         * 
         * @param lhsString
         * @param rhs
         */
        private static String wrapAdd(String lhsString, int rhs) {
            int lhs = Integer.parseInt(lhsString, 2);
            int result = lhs + rhs;
            String bitString = zeroPaddedBitString(result);
            return bitString.substring(GeoHashImpl.BITS_IN_INTEGER - lhsString.length());
        }

        /**
         * Converts value to bit-string and pads with leading 0's so that negative and
         * positive numbers yeild equal-length strings
         * 
         * @param value
         */
        private static String zeroPaddedBitString(int value) {
            String bitString = Integer.toBinaryString(value);
            return String.format("%32s", bitString).replace(" ", "0");
        }
    }

    private static class GeoHashInternal {
        private static final Range LATITUDE_RANGE = new Range(-90, 90);
        private static final Range LONGITUDE_RANGE = new Range(-180, 180);

        private static class Range {
            private final double min;
            private final double max;

            private Range(
                double minValue,
                double maxValue
            ) {
                if (minValue > maxValue) {
                    throw new IllegalArgumentException(
                        "minValue > maxValue. minValue must be supplied first"
                    );
                }
                min = minValue;
                max = maxValue;
            }
        }

        public static List<Boolean> geohash(
            double lat,
            double lon,
            int bitsOfPrecision
        ) {
            return geohash2D(lat, LATITUDE_RANGE, lon, LONGITUDE_RANGE, bitsOfPrecision);
        }

        public static List<Boolean> geohash(
            List<Boolean> latitudeGeohash,
            List<Boolean> longitudeGeohash
        ) {
            return weaveLatAndLngGeohashes(
                latitudeGeohash.iterator(),
                longitudeGeohash.iterator()
            );
        }

        private static Stream<Boolean> geohash1D(
            double valueToHash,
            Range valueRange,
            int bitsOfPrecision
        ) {
            geohash1DValidateArgs(valueRange, bitsOfPrecision);
            valueToHash = snapValueToRange(valueToHash, valueRange);

            double normalizedUpperRange = Math.abs(valueRange.max - valueRange.min);
            double normalizedValue = valueToHash - valueRange.min;

            return recursiveGeohash1D(
                normalizedValue, normalizedUpperRange, bitsOfPrecision
            );
        }

        private static List<Boolean> geohash2D(
            double v1,
            Range v1range,
            double v2,
            Range v2range,
            int bitsOfPrecision
        ) {
            int longitudePrecision = bitsOfPrecision / 2;
            Iterator<Boolean> longitudeGeohash =
                GeoHashInternal.geohash1D(v2, v2range, longitudePrecision).iterator();

            int latitudePrecision = (int)Math.ceil(bitsOfPrecision / 2.0);
            Iterator<Boolean> latitudeGeohash =
                GeoHashInternal.geohash1D(v1, v1range, latitudePrecision).iterator();

            return weaveLatAndLngGeohashes(latitudeGeohash, longitudeGeohash);
        }

        private static List<Boolean> weaveLatAndLngGeohashes(
            Iterator<Boolean> latitudeGeohash,
            Iterator<Boolean> longitudeGeohash
        ) {
            List<Boolean> geohashResult = new ArrayList<>();

            while(latitudeGeohash.hasNext()) {
                geohashResult.add(latitudeGeohash.next());

                if (longitudeGeohash.hasNext()) {
                    geohashResult.add(longitudeGeohash.next());
                }
            }
            return geohashResult;
        }

        private static void geohash1DValidateArgs(
            Range valueRange,
            int bitsOfPrecision
        ) {

            if (valueRange == null) {
                throw new IllegalArgumentException(
                    "value range must be an array of 2 doubles"
                );
            }

            if (bitsOfPrecision < 0) {
                throw new IllegalArgumentException(
                    "precision must be a positive integer"
                );
            }
        }

        private static double snapValueToRange(double valueToHash, Range valueRange) {
            if (valueToHash < valueRange.min) {
                valueToHash = valueRange.min;
            } else if (valueToHash > valueRange.max) {
                valueToHash = valueRange.max;
            }
            return valueToHash;
        }


        private static Stream<Boolean> recursiveGeohash1D(
            double normalizedValue,
            double normalizedUpperRange,
            int precision
        ) {

            if (precision != 0) {
                Boolean nextValue;
                double nextNormalizedValue;
                double nextUpperRange = normalizedUpperRange / 2;

                if (normalizedValue >= nextUpperRange) {
                    nextValue = true;
                    nextNormalizedValue = normalizedValue - nextUpperRange;
                } else {
                    nextValue = false;
                    nextNormalizedValue = normalizedValue;
                }
                return Stream
                    .concat(
                        Stream.of(nextValue),
                        GeoHashInternal
                            .recursiveGeohash1D(
                                nextNormalizedValue, nextUpperRange, precision - 1
                            )
                    );
            } else {
                // BASE CASE
                return Stream.empty();
            }
        }
    }

}
