package edu.vanderbilt.cs.live6;

/**
 * DataAndPosition binds a latitude / longitude location to a
 * specific piece of data, such as a Building object, String,
 * RealEstateListing object, Map, etc.
 *
 * DataAndPosition<Map<String,String>> positionedMap =
 *                 DataAndPosition.with(0, 0, new HashMap<String,String>());
 *
 * public class Foo {...}
 *
 * DataAndPosition<Foo> positionedFoo = DataAndPosition.with(1,1, new Foo());
 *
 *
 * @param <T>
 */
public interface DataAndPosition<T> extends Position {

    public static <T> DataAndPosition<T> with(double lat, double lon, T data){
        return new DataAndPosition() {

            @Override
            public double getLatitude() {
                return lat;
            }

            @Override
            public double getLongitude() {
                return lon;
            }

            @Override
            public Object getData() { return data; }
        };
    }

    public T getData();

}
