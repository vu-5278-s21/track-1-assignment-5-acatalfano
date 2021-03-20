package edu.vanderbilt.cs.live6;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class ArrayPrecisionTree<T extends Collection<?>> implements PrecisionTree<T> {
    private final int resolution;
    private final List<T> precisionTree;

    public ArrayPrecisionTree(
        int resolutionValue,
        Supplier<T> supplier
    ) {
        resolution = resolutionValue;
        final int capacity = (int)Math.pow(2, resolution);
        precisionTree = new ArrayList<>();
        for(int i = 0; i < capacity; i++) {
            precisionTree.add(supplier.get());
        }
    }

    @Override
    public Stream<T> itemsWithinRange(String locationCodePrefix, int precision) {
        int startIndex = rangeStartIndex(locationCodePrefix, precision);
        int endIndex = rangeEndIndex(startIndex, precision);
        return precisionTree.subList(startIndex, endIndex).stream();
    }

    @Override
    public T itemsAtLocation(String locationCode) {
        return precisionTree.get(index(locationCode));
    }

    /**
     * @Return index in the physical list where the provided position should be stored (as
     *             per underlying tree semantics)
     */
    private int index(String locationCode) {
        return rangeStartIndex(locationCode, resolution);
    }

    /**
     * @Return list index where the range starts for all stored positions that match the
     *             provided position up to the provided precision
     * 
     * @Assume precision <= resolution
     */
    private int rangeStartIndex(
        String locationCode,
        int precision
    ) {
        String geohashString = zeroPaddedBitString(locationCode, precision);
        return Integer.parseInt(geohashString, 2);
    }

    /**
     * @Return list index where the range ends for all stored positions that match the
     *             provided start index up to the provided precision
     * 
     * @Assume precision <= resolution
     */
    private int rangeEndIndex(
        int startIndex,
        int precision
    ) {
        return (int)Math.pow(2, (resolution - precision)) + startIndex;
    }

    /**
     * @Return item at specified location-code prefix and precision, as a string
     *             representing a binary number. The string is padded with 0's (at the end
     *             of the string) until it matches the DB's resolution
     * 
     * @Assume precision <= resolution
     */
    private String zeroPaddedBitString(String locationCodePrefix, int precision) {
        StringBuilder paddedBitsBuilder = new StringBuilder(locationCodePrefix);
        char[] padding = new char[resolution - precision];
        Arrays.fill(padding, '0');
        paddedBitsBuilder.append(padding);
        return paddedBitsBuilder.toString();
    }

}
