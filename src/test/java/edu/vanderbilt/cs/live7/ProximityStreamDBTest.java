package edu.vanderbilt.cs.live7;


import edu.vanderbilt.cs.live6.*;
import edu.vanderbilt.cs.live7.example.Building;
import edu.vanderbilt.cs.live7.example.BuildingAttributesStrategy;
import edu.vanderbilt.cs.live7.example.MapAttributesStrategy;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

public class ProximityStreamDBTest {

    private class FakeGeoHash implements GeoHash {

        private List<Boolean> bits = new ArrayList<>();

        public FakeGeoHash(boolean[] bits) {
            for(Boolean b : bits) {
                this.bits.add(b);
            }
        }

        public FakeGeoHash(List<Boolean> bits) {
            this.bits = bits;
        }

        @Override
        public int bitsOfPrecision() {
            return bits.size();
        }

        @Override
        public GeoHash prefix(int n) {
            return new FakeGeoHash(this.bits.subList(0, n));
        }

        @Override
        public GeoHash northNeighbor() {
            return null;
        }

        @Override
        public GeoHash southNeighbor() {
            return null;
        }

        @Override
        public GeoHash westNeighbor() {
            return null;
        }

        @Override
        public GeoHash eastNeighbor() {
            return null;
        }

        @Override
        public Iterator<Boolean> iterator() {
            return bits.iterator();
        }
    }

    private <T> ProximityStreamDB<T> newDB(
        AttributesStrategy<T> strat,
        Map<Position, boolean[]> hashLookup,
        int bits
    ) {

        return new ProximityStreamDBFactory()
            .create(
                strat,
                (lat, lon, bs) -> new FakeGeoHash(hashLookup.get(Position.with(lat, lon)))
                    .prefix(bs),
                bits
            );
    }

    private static boolean[] randomGeoHash(int bits) {
        boolean[] hash = new boolean[bits];
        for(int i = 0; i < hash.length; i++) {
            hash[i] = Math.random() > 0.5;
        }
        return hash;
    }

    public Position randomPosition() {
        double lat = -90.0 + (Math.random() * 180); // generate a random lat between -90/90
        double lon = -180 + (Math.random() * 360); // generate a random lon between -180/180
        return Position.with(lat, lon);
    }

    /**
     * This method randomly generates a set of unique Positions and maps them to a
     * specified number of geohashes. The geohashes are completely random and the mapping
     * is random.
     *
     * For example, randomCoordinateHashMappings(16, 100, 12) would generate 100 unique
     * Positions and map them to 12 random geohashes of 16 bits each.
     *
     * @param bits
     * @param total
     * @param groups
     * 
     * @return
     */
    private Map<Position, boolean[]> randomCoordinateHashMappings(
        int bits,
        int total,
        int groups,
        int sharedPrefixLength
    ) {
        int avg = total / groups; // If it doesn't divide evenly, there is a remainder discarded

        Map<Position, boolean[]> mappings = new HashMap<>();
        Set<Position> positions = new HashSet<>();
        Set<String> hashes = new HashSet<>();

        for(int i = 0; i < groups; i++) {

            // We generate random unique geohash prefixes of length
            // `sharedPrefixLength` so that we can synthesize groups
            // of positions that will match up to a certain number of
            // bits
            boolean[] hash = randomGeoHash(sharedPrefixLength);
            while(hashes.contains(toString(hash))) {
                hash = randomGeoHash(sharedPrefixLength);
            }
            hashes.add(toString(hash));

            // Create `avg` Position objects that are unique
            // and map each one to the random hash
            for(int j = 0; j < avg; j++) {

                // We randomize every bit after the shared prefix
                boolean[] fullHash = Arrays.copyOf(hash, bits);
                for(int k = hash.length; k < fullHash.length; k++) {
                    fullHash[k] = (Math.random() > 0.5);
                }

                Position pos = randomPosition();
                while(positions.contains(pos)) {
                    pos = randomPosition();
                }
                positions.add(pos);
                mappings.put(pos, fullHash);
            }

        }

        return mappings;
    }

    public String toString(boolean[] data) {
        String hashString = "";
        for(boolean b : data) {
            hashString += (b ? "1" : "0");
        }
        return hashString;
    }


    // This is the most comprehensive, but also the most difficult to
    // understand test. I would save this test for last if you are
    // trying to incrementally pass the test methdods.
    @Test
    public void testStreamAttributesRandom() {

        // This test randomly generates a set of positions that are
        // artificially assigned to geohashes. The geohashes are constructed
        // such that the positions are guaranteed to fall into N groups that
        // match K bits of their geohashes. The test checks that all data items
        // in a given group are correctly streamed when any position that is mapped
        // to that group is provided as the nearby search position. The nearby searches
        // are done with K bits to guarantee that all items in the group will have matching
        // geohashes.
        //
        // A synthetic example:
        //
        // groups = 3;
        // sharedPrefixLength = 2;
        // bits = 4;
        // buildings = 6;
        //
        // randomMappings = {
        //      [(-88.01, 0) 1111]
        //      [(-48.01, 90) 1101]
        //      [(-88.01, 20) 1000]
        //      [(20.01, 0) 1001]
        //      [(118.01, -10) 0110]
        //      [(88.01, 10) 0101]
        // }
        //
        // There are three unique prefixes of length 2.
        // [11, 10, 01]
        //
        // Every position has been mapped to a random geohash that
        // starts with one of these prefixes.
        //
        // For any given prefix, we know in advance what positions will
        // map to it.
        //
        // For each position, we can check that all other locations with
        // a matching prefix are returned when we do a nearby search on
        // that position.
        //
        // Note: the hashes are completely random and unrelated to the
        // acutal positions on the earth -- it shouldn't matter to your
        // implementation how the position to geohash translation is done,
        // as long as it is consistent

        int maxGroups = 128;
        int maxBits = 256;

        int groups = 1 + (int)Math.rint(Math.random() * (maxGroups - 1));
        int sharedPrefixLength =
            Math
                .max(
                    (int)Math.log(groups) + 16, // We have to ensure that we have
                    // enough bits in the shared prefix
                    // to differentiate all the groups and
                    // not take forever to randomly generate
                    // the unique shared prefixes
                    (int)Math.rint(Math.random() * maxBits)
                );
        int bits = sharedPrefixLength +
            (int)Math.rint(Math.random() * (maxBits - sharedPrefixLength));
        int buildings = (int)Math.rint(Math.random() * 28 * groups);
        ;


        System.out
            .println(
                "Testing " + buildings + " items with "
                    + bits + " bit hashes and " + sharedPrefixLength + " shared bits in "
                    + groups + " groups"
            );

        Map<Position, boolean[]> randomMappings = randomCoordinateHashMappings(
            bits, buildings, groups, sharedPrefixLength
        );
        ProximityStreamDB<Building> db = newDB(
            new BuildingAttributesStrategy(), randomMappings, bits
        );

        Map<String, Set<String>> hashToBuildingName = new HashMap<>();
        Map<String, List<Double>> hashToSqft = new HashMap<>();
        Map<String, List<Double>> hashToClassrooms = new HashMap<>();

        for(Map.Entry<Position, boolean[]> entry : randomMappings.entrySet()) {
            Position pos = entry.getKey();
            String hashstr = toString(entry.getValue()).substring(0, sharedPrefixLength);
            Building b = new Building(
                UUID.randomUUID().toString(),
                Math.random() * 100000,
                Math.rint(Math.random() * 25)
            );
            db.insert(DataAndPosition.with(pos.getLatitude(), pos.getLongitude(), b));

            Set<String> existing = hashToBuildingName
                .getOrDefault(hashstr, new HashSet<>());
            existing.add(b.getName());
            hashToBuildingName.put(hashstr, existing);

            List<Double> curr = hashToSqft.getOrDefault(hashstr, new ArrayList<>());
            curr.add(b.getSizeInSquareFeet());
            hashToSqft.put(hashstr, curr);

            List<Double> rooms = hashToClassrooms
                .getOrDefault(hashstr, new ArrayList<>());
            rooms.add(b.getClassRooms());
            hashToClassrooms.put(hashstr, rooms);
        }

        for(Map.Entry<Position, boolean[]> entry : randomMappings.entrySet()) {
            Position pos = entry.getKey();
            String hashstr = toString(entry.getValue()).substring(0, sharedPrefixLength);
            Set<String> expected = hashToBuildingName
                .getOrDefault(hashstr, new HashSet<>());
            Set<String> actual = db
                .streamNearby(
                    a -> a.getName().equals(BuildingAttributesStrategy.NAME), pos,
                    sharedPrefixLength
                )
                .map(v -> "" + v)
                .collect(Collectors.toSet());

            assertEquals(expected, actual);
        }

        for(Map.Entry<Position, boolean[]> entry : randomMappings.entrySet()) {
            Position pos = entry.getKey();
            String hashstr = toString(entry.getValue()).substring(0, sharedPrefixLength);

            List<Double> expectedSqft = hashToSqft.get(hashstr);

            double expectedAvg = expectedSqft
                .stream()
                .mapToDouble(v -> v)
                .average()
                .getAsDouble();
            double actualAvg = db
                .averageNearby(
                    a -> a
                        .getName()
                        .equals(BuildingAttributesStrategy.SIZE_IN_SQUARE_FEET), pos,
                    sharedPrefixLength
                )
                .getAsDouble();

            assertEquals(expectedAvg, actualAvg, 0.1);

            double expectedMax = expectedSqft
                .stream()
                .mapToDouble(v -> v)
                .max()
                .getAsDouble();
            double actualMax = db
                .maxNearby(
                    a -> a
                        .getName()
                        .equals(BuildingAttributesStrategy.SIZE_IN_SQUARE_FEET), pos,
                    sharedPrefixLength
                )
                .getAsDouble();

            assertEquals(expectedMax, actualMax, 0.1);

            double expectedMin = expectedSqft
                .stream()
                .mapToDouble(v -> v)
                .min()
                .getAsDouble();
            double actualMin = db
                .minNearby(
                    a -> a
                        .getName()
                        .equals(BuildingAttributesStrategy.SIZE_IN_SQUARE_FEET), pos,
                    sharedPrefixLength
                )
                .getAsDouble();

            assertEquals(expectedMin, actualMin, 0.1);
        }

        for(Map.Entry<Position, boolean[]> entry : randomMappings.entrySet()) {
            Position pos = entry.getKey();
            String hashstr = toString(entry.getValue()).substring(0, sharedPrefixLength);

            List<Double> expectedClassrooms = hashToClassrooms.get(hashstr);

            double expectedAvg = expectedClassrooms
                .stream()
                .mapToDouble(v -> v)
                .average()
                .getAsDouble();
            double actualAvg = db
                .averageNearby(
                    a -> a.getName().equals(BuildingAttributesStrategy.CLASSROOMS), pos,
                    sharedPrefixLength
                )
                .getAsDouble();

            assertEquals(expectedAvg, actualAvg, 0.1);

            double expectedMax = expectedClassrooms
                .stream()
                .mapToDouble(v -> v)
                .max()
                .getAsDouble();
            double actualMax = db
                .maxNearby(
                    a -> a.getName().equals(BuildingAttributesStrategy.CLASSROOMS), pos,
                    sharedPrefixLength
                )
                .getAsDouble();

            assertEquals(expectedMax, actualMax, 0.1);

            double expectedMin = expectedClassrooms
                .stream()
                .mapToDouble(v -> v)
                .min()
                .getAsDouble();
            double actualMin = db
                .minNearby(
                    a -> a.getName().equals(BuildingAttributesStrategy.CLASSROOMS), pos,
                    sharedPrefixLength
                )
                .getAsDouble();

            assertEquals(expectedMin, actualMin, 0.1);

            Map<Double, Long> hist = db
                .histogramNearby(
                    a -> a.getName().equals(BuildingAttributesStrategy.CLASSROOMS), pos,
                    sharedPrefixLength
                );
            for(Map.Entry<Double, Long> bucket : hist.entrySet()) {
                assertEquals(
                    expectedClassrooms
                        .stream()
                        .filter(v -> v.equals(bucket.getKey()))
                        .count(),
                    bucket.getValue()
                );
            }
        }

    }

    @Test
    public void testStreamNearby() {

        Map<Position, boolean[]> mapping = new HashMap<>();
        mapping
            .put(Position.with(36.145050, 86.803365), new boolean[] { true, true, true });
        mapping
            .put(
                Position.with(36.148345, 86.802909), new boolean[] { true, true, false }
            );
        mapping
            .put(
                Position.with(36.143171, 86.805772), new boolean[] { true, false, false }
            );

        ProximityStreamDB<Building> strmdb = newDB(
            new BuildingAttributesStrategy(),
            mapping,
            3
        );

        Building kirklandHall = new Building("Kirkland Hall", 150000, 5);
        Building fgh = new Building("Featheringill Hall", 95023.4, 38);
        Building esb = new Building("Engineering Sciences Building", 218793.34, 10);

        strmdb.insert(DataAndPosition.with(36.145050, 86.803365, fgh));
        strmdb.insert(DataAndPosition.with(36.148345, 86.802909, kirklandHall));
        strmdb.insert(DataAndPosition.with(36.143171, 86.805772, esb));

        Set<Building> buildingsNearFgh =
            strmdb
                .nearby(Position.with(36.145050, 86.803365), 2)
                .stream()
                .map(dpos -> dpos.getData())
                .collect(Collectors.toSet());

        assertEquals(2, buildingsNearFgh.size());
        assertTrue(buildingsNearFgh.contains(fgh));
        assertTrue(buildingsNearFgh.contains(kirklandHall));
        assertFalse(buildingsNearFgh.contains(esb));
    }

    @Test
    public void testAverageBuildingsNearby() {
        Map<Position, boolean[]> mapping = new HashMap<>();
        mapping
            .put(Position.with(36.145050, 86.803365), new boolean[] { true, true, true });
        mapping
            .put(
                Position.with(36.148345, 86.802909), new boolean[] { true, true, false }
            );
        mapping
            .put(
                Position.with(36.143171, 86.805772), new boolean[] { true, false, false }
            );

        ProximityStreamDB<Building> strmdb = newDB(
            new BuildingAttributesStrategy(),
            mapping,
            3
        );

        Building kirklandHall = new Building("Kirkland Hall", 150000, 5);
        Building fgh = new Building("Featheringill Hall", 95023.4, 38);
        Building esb = new Building("Engineering Sciences Building", 218793.34, 10);

        strmdb.insert(DataAndPosition.with(36.145050, 86.803365, fgh));
        strmdb.insert(DataAndPosition.with(36.148345, 86.802909, kirklandHall));
        strmdb.insert(DataAndPosition.with(36.143171, 86.805772, esb));

        double averageBuildingSqft = strmdb
            .averageNearby(
                a -> BuildingAttributesStrategy.SIZE_IN_SQUARE_FEET.equals(a.getName()),
                Position.with(36.145050, 86.803365), 2
            )
            .getAsDouble();

        assertEquals(122511.7, averageBuildingSqft, 0.1);

    }

    @Test
    public void testHistogramBuildingsNearby() {
        Map<Position, boolean[]> mapping = new HashMap<>();
        mapping
            .put(Position.with(36.145050, 86.803365), new boolean[] { true, true, true });
        mapping
            .put(
                Position.with(36.148345, 86.802909), new boolean[] { true, true, false }
            );
        mapping
            .put(
                Position.with(36.143171, 86.805772), new boolean[] { true, false, false }
            );

        ProximityStreamDB<Building> strmdb = newDB(
            new BuildingAttributesStrategy(),
            mapping,
            3
        );

        Building kirklandHall = new Building("Kirkland Hall", 150000, 5);
        Building fgh = new Building("Featheringill Hall", 95023.4, 38);
        Building esb = new Building("Engineering Sciences Building", 218793.34, 10);

        strmdb.insert(DataAndPosition.with(36.145050, 86.803365, fgh));
        strmdb.insert(DataAndPosition.with(36.148345, 86.802909, kirklandHall));
        strmdb.insert(DataAndPosition.with(36.143171, 86.805772, esb));

        Map<Object, Long> buildingSizeHistogram = strmdb
            .histogramNearby(
                a -> BuildingAttributesStrategy.SIZE_IN_SQUARE_FEET.equals(a.getName()),
                Position.with(36.145050, 86.803365),
                1
            );

        assertEquals(1, buildingSizeHistogram.get(kirklandHall.getSizeInSquareFeet()));
        assertEquals(1, buildingSizeHistogram.get(esb.getSizeInSquareFeet()));
        assertEquals(1, buildingSizeHistogram.get(fgh.getSizeInSquareFeet()));
    }

    @Test
    public void testHistorySmall() {
        Map<Position, boolean[]> map = new HashMap<>();
        Position p1 = Position.with(1, 100);
        Position p2 = Position.with(-20, 25);
        boolean[] v1 = { false, true, true, true, false };
        boolean[] v2 = { true, true, false, false, true };
        map.put(p1, v1);
        map.put(p2, v2);

        ProximityStreamDB<Map<String, ?>> db = new ProximityStreamDBFactory()
            .create(
                new MapAttributesStrategy(),
                (lat, lon, bs) -> new FakeGeoHash(map.get(Position.with(lat, lon)))
                    .prefix(bs),
                3
            );

        db.insert(DataAndPosition.with(1, 100, new HashMap<>()));
        db.insert(DataAndPosition.with(-20, 25, new HashMap<>()));

        ProximityStreamDB<Map<String, ?>> hist = db.databaseStateAtTime(1);
        assertTrue(hist.contains(p1, 3));
        assertFalse(hist.contains(p2, 3));
        assertTrue(db.contains(p1, 3));
        assertTrue(db.contains(p2, 3));
    }

    @Test
    public void testHistory() {

        int groups = 32;
        int sharedPrefixLength = 8;
        int bits = 16;
        int buildings = 32;

        Map<Position, boolean[]> randomMappings = randomCoordinateHashMappings(
            bits, buildings, groups, sharedPrefixLength
        );

        ProximityStreamDB<Map<String, ?>> strmdb = newDB(
            new MapAttributesStrategy(),
            randomMappings,
            bits
        );

        List<Map<String, Double>> data = new ArrayList<>();
        List<Position> positions = new ArrayList<>();

        for(Position p : randomMappings.keySet()) {
            Map<String, Double> randomMap = new HashMap<>();
            strmdb
                .insert(
                    DataAndPosition.with(p.getLatitude(), p.getLongitude(), randomMap)
                );
            data.add(randomMap);
            positions.add(p);
        }

        for(Position p : positions) {
            strmdb.delete(p, bits);
        }

        for(int i = 0; i < positions.size(); i++) {
            ProximityStreamDB<Map<String, ?>> snapshot = strmdb
                .databaseStateAtTime(i + 1);
            assertTrue(snapshot.contains(positions.get(i), bits));
            for(int j = i + 1; j < positions.size(); j++) {
                assertFalse(snapshot.contains(positions.get(j), bits));
            }
            for(int j = i - 1; j > 0; j--) {
                assertTrue(snapshot.contains(positions.get(j), bits));
            }
        }

        for(int i = 0; i < positions.size(); i++) {
            ProximityStreamDB<Map<String, ?>> snapshot = strmdb
                .databaseStateAtTime(i + positions.size() + 1);
            assertFalse(snapshot.contains(positions.get(i), bits));
            for(int j = i + 1; j < positions.size(); j++) {
                assertTrue(snapshot.contains(positions.get(j), bits));
            }
            for(int j = i - 1; j > 0; j--) {
                assertFalse(snapshot.contains(positions.get(j), bits));
            }
        }

    }

}
