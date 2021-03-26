package edu.vanderbilt.cs.live6;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.Stream.Builder;

public class ArrayPrecisionTree<T extends Collection<?>> implements PrecisionTree<T> {
    private final int resolution;
    private final NavigableMap<BigInteger, T> precisionTree;
    private final Supplier<T> treeNodeFactory;

    public ArrayPrecisionTree(
        int resolutionValue,
        Supplier<T> supplier
    ) {
        resolution = resolutionValue;
        precisionTree = new TreeMap<>();
        treeNodeFactory = supplier;
    }

    @Override
    public Stream<T> itemsWithinRange(String locationCodePrefix, int precision) {
        Builder<T> streamBuilder = Stream.builder();
        BigInteger startIndex = rangeStartIndex(locationCodePrefix, precision)
            .subtract(BigInteger.ONE);
        BigInteger endIndex = rangeEndIndex(startIndex, precision);

        Map.Entry<BigInteger, T> nextEntry = precisionTree.higherEntry(startIndex);

        while(nextEntry != null && nextEntry.getKey().compareTo(endIndex) <= 0) {
            if (!nextEntry.getValue().isEmpty()) {
                streamBuilder.accept(nextEntry.getValue());
            }
            nextEntry = precisionTree.higherEntry(nextEntry.getKey());
        }
        return streamBuilder.build();
    }

    @Override
    public T itemsAtLocation(String locationCode) {
        T items;
        BigInteger index = index(locationCode);
        if (precisionTree.get(index) == null) {
            items = treeNodeFactory.get();
            precisionTree.put(index, items);
        } else {
            items = precisionTree.get(index);
        }
        return items;
    }

    /**
     * @Return index in the physical list where the provided position should be stored (as
     *             per underlying tree semantics)
     */
    private BigInteger index(String locationCode) {
        return rangeStartIndex(locationCode, resolution);
    }

    /**
     * @Return list index where the range starts for all stored positions that match the
     *             provided position up to the provided precision
     * 
     * @Assume precision <= resolution
     */
    private BigInteger rangeStartIndex(
        String locationCode,
        int precision
    ) {
        String geohashString = zeroPaddedBitString(locationCode, precision);
        return new BigInteger(geohashString, 2);
    }

    /**
     * @Return list index where the range ends for all stored positions that match the
     *             provided start index up to the provided precision
     * 
     * @Assume precision <= resolution
     */
    private BigInteger rangeEndIndex(
        BigInteger startIndex,
        int precision
    ) {
        return BigInteger.TWO.pow(resolution - precision).add(startIndex);
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
