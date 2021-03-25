package edu.vanderbilt.cs.live6;

import java.util.List;
import java.util.stream.Collectors;

public class CircularArithmetic {

    private static final int BITS_IN_INTEGER = 32;

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
     * emulate truncated 2's-complement addition between a String (parsed as a binary int)
     * and an int the result is inverse-truncated string (leading bits are dropped) so
     * that it is the same length as the input string
     * 
     * @param lhsString
     * @param rhs
     */
    private static String wrapAdd(String lhsString, int rhs) {
        int lhs = Integer.parseInt(lhsString, 2);
        int result = lhs + rhs;
        String bitString = zeroPaddedBitString(result);
        return bitString.substring(BITS_IN_INTEGER - lhsString.length());
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

    private static String hashAsString(List<Boolean> hash) {
        return hash
            .stream()
            .reduce(
                "",
                (accum, bit) -> accum + (bit ? '1' : '0'),
                (lhs, rhs) -> lhs + rhs
            );
    }
}
