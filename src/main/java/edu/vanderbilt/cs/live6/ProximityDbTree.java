package edu.vanderbilt.cs.live6;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ProximityDbTree<T> implements ProximityDB<T> {
    private final int resolution;
    private final GeoHashFactory geoHashFactory;
    private final PrecisionTree<Collection<GeohashEntry<T>>> geoTree;
    private final PrecisionTreeFactory<Collection<GeohashEntry<T>>> precisionTreeFactory;

    public ProximityDbTree(
        PrecisionTreeFactory<Collection<GeohashEntry<T>>> treeFactory,
        GeoHashFactory hashFactory,
        int precision
    ) {
        resolution = precision;
        geoHashFactory = hashFactory;
        precisionTreeFactory = treeFactory;
        geoTree = treeFactory.with(precision);
    }

    @Override
    public void insert(DataAndPosition<T> data) {
        geoTree
            .itemsAtLocation(treeLocationCode(data, resolution))
            .add(
                new GeohashEntry<>(
                    data,
                    geoHashFactory
                        .with(data.getLatitude(), data.getLongitude(), resolution)
                )
            );
    }

    @Override
    public Collection<DataAndPosition<T>> delete(Position pos) {
        Collection<GeohashEntry<T>> targetSet =
            geoTree.itemsAtLocation(treeLocationCode(pos, resolution));
        List<DataAndPosition<T>> deletions = targetSet
            .stream()
            .map(GeohashEntry::getDataAndPosition)
            .collect(Collectors.toList());

        targetSet.clear();
        return deletions;
    }

    @Override
    public Collection<DataAndPosition<T>> delete(Position pos, int bitsOfPrecision) {
        List<Collection<GeohashEntry<T>>> itemsToClear = geoTree
            .itemsWithinRange(treeLocationCode(pos, bitsOfPrecision), bitsOfPrecision)
            .collect(Collectors.toList());
        List<DataAndPosition<T>> deletedEntries =
            itemsToClear
                .stream()
                .flatMap(Collection::stream)
                .map(GeohashEntry::getDataAndPosition)
                .collect(Collectors.toList());

        itemsToClear.forEach(Collection::clear);
        return deletedEntries;
    }

    @Override
    public boolean contains(Position pos, int bitsOfPrecision) {
        return geoTree
            .itemsWithinRange(treeLocationCode(pos, bitsOfPrecision), bitsOfPrecision)
            .anyMatch(set -> !set.isEmpty());
    }

    @Override
    public Collection<DataAndPosition<T>> nearby(Position pos, int bitsOfPrecision) {
        return geoTree
            .itemsWithinRange(treeLocationCode(pos, bitsOfPrecision), bitsOfPrecision)
            .filter(Objects::nonNull)
            .flatMap(Collection::stream)
            .map(GeohashEntry::getDataAndPosition)
            .collect(Collectors.toList());
    }

    @Override
    public ProximityDB<T> emptyClone() {
        return new ProximityDbTree<>(precisionTreeFactory, geoHashFactory, resolution);
    }

    private String treeLocationCode(Position pos, int precision) {
        Iterator<Boolean> geohashIterator = geoHashFactory
            .with(pos.getLatitude(), pos.getLongitude(), precision)
            .iterator();
        StringBuilder geohashString = new StringBuilder();
        while(geohashIterator.hasNext()) {
            geohashString.append(geohashIterator.next().booleanValue() ? '1' : '0');
        }
        return geohashString.toString();
    }
}
