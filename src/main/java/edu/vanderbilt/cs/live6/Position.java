package edu.vanderbilt.cs.live6;

import java.util.Objects;

public interface Position {

    public static Position with(double lat, double lon){
        return new Position() {

            @Override
            public double getLatitude() {
                return lat;
            }

            @Override
            public double getLongitude() {
                return lon;
            }

            @Override
            public int hashCode() {
                return Objects.hash(lat, lon);
            }

            @Override
            public boolean equals(Object obj) {
                return obj instanceof Position &&
                        obj != null &&
                        ((Position)obj).getLongitude() == lon &&
                        ((Position)obj).getLatitude() == lat;
            }
        };
    }

    public double getLatitude();

    public double getLongitude();

}
