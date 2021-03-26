package edu.vanderbilt.cs.live7;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import edu.vanderbilt.cs.live6.DataAndPosition;
import edu.vanderbilt.cs.live6.Position;
import edu.vanderbilt.cs.live6.ProximityDB;

public class NaiveProximityStreamDB<T> implements ProximityStreamDB<T> {
	private final ProximityDB<T> currentProximityDb;
	private final AttributesStrategy<T> attributesStrategy;
	private final List<UpdateCommand<T>> operations;

	public NaiveProximityStreamDB(
		ProximityDB<T> initialProximityDB,
		AttributesStrategy<T> strategy
	) {
		currentProximityDb = initialProximityDB;
		attributesStrategy = strategy;
		operations = new ArrayList<>();
	}

	@Override
	public void insert(DataAndPosition<T> data) {
		operations.add(new InsertCommand<>(data));
		currentProximityDb.insert(data);
	}

	@Override
	public Collection<DataAndPosition<T>> delete(Position pos) {
		operations.add(new DeleteCommand<>(pos));
		return currentProximityDb.delete(pos);
	}

	@Override
	public Collection<DataAndPosition<T>> delete(Position pos, int bitsOfPrecision) {
		operations.add(new DeleteRangeCommand<>(pos, bitsOfPrecision));
		return currentProximityDb.delete(pos, bitsOfPrecision);
	}

	@Override
	public boolean contains(Position pos, int bitsOfPrecision) {
		return currentProximityDb.contains(pos, bitsOfPrecision);
	}

	@Override
	public Collection<DataAndPosition<T>> nearby(Position pos, int bitsOfPrecision) {
		return currentProximityDb.nearby(pos, bitsOfPrecision);
	}

	@Override
	public ProximityDB<T> emptyClone() {
		return new NaiveProximityStreamDB<>(
			currentProximityDb.emptyClone(), attributesStrategy
		);
	}

	@Override
	public ProximityStreamDB<T> databaseStateAtTime(int n) {
		if (n > operations.size()) {
			n = operations.size();
		}

		ProximityDB<T> proximityDB = operations
			.subList(0, n)
			.stream()
			.reduce(
				currentProximityDb.emptyClone(), (accumDb, command) -> command
					.execute(accumDb), (oldDb, newDb) -> newDb
			);
		return new NaiveProximityStreamDB<>(proximityDB, attributesStrategy);
	}

	@Override
	public <V> Stream<V> streamNearby(
		AttributeMatcher<V> matcher,
		Position pos,
		int bitsOfPrecision
	) {
		return currentProximityDb
			.nearby(pos, bitsOfPrecision)
			.stream()
			.flatMap(
				dataPos -> attributesStrategy
					.getAttributes(dataPos.getData())
					.stream()
					.filter(attribute -> matcher.matches((attribute)))
					.map(attribute -> (V)attribute.getValue())
			);
	}

	@Override
	public <V extends Double> OptionalDouble averageNearby(
		AttributeMatcher<V> matcher,
		Position pos,
		int bitsOfPrecision
	) {
		return streamNearby(matcher, pos, bitsOfPrecision).mapToDouble(x -> x).average();
	}

	@Override
	public <V extends Double> OptionalDouble minNearby(
		AttributeMatcher<V> matcher,
		Position pos,
		int bitsOfPrecision
	) {
		return streamNearby(matcher, pos, bitsOfPrecision)
			.mapToDouble(x -> x)
			.min();
	}

	@Override
	public <V extends Double> OptionalDouble maxNearby(
		AttributeMatcher<V> matcher,
		Position pos,
		int bitsOfPrecision
	) {
		return streamNearby(matcher, pos, bitsOfPrecision)
			.mapToDouble(x -> x)
			.max();
	}

	@Override
	public <V> Map<V, Long> histogramNearby(
		AttributeMatcher<V> matcher,
		Position pos,
		int bitsOfPrecision
	) {
		return streamNearby(matcher, pos, bitsOfPrecision)
			.collect(
				Collectors
					.groupingByConcurrent(
						attributeValue -> attributeValue,
						Collectors.counting()
					)
			);
	}
}
