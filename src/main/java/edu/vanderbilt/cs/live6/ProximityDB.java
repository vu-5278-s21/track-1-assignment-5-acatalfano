package edu.vanderbilt.cs.live6;


import java.util.Collection;

/**
 * Create an implementation of the ProximityDB.
 *
 * The ProximityDB maps data items to coordinates and allows searching
 * for nearby data items using geohashes.
 *
 * You are free to adapt your GeoDB implementation. However, please make
 * sure that your implementation of ProximityDB is in this package so that
 * it is easy for your peers and the instructor to find.
 *
 * There will be ambiguous requirements. Use your best judgement in determining
 * an appropriate interpretation. Keep these ambiguities in mind when you code
 * review others' solutions later in class.
 *
 *
 * The type parameter T is the type of data stored in the database. For example,
 * to map Strings to coordinates, new ProximityDBImpl<String>(...)
 *
 * See the example package for a sample application that stores Building
 * objects in the database
 */
public interface ProximityDB<T> {

    /**
     * Inserts a data item into the database at the specified location.
     * You SHOULD preserve duplicate data items.
     *
     */
    public void insert(DataAndPosition<T> data);

    /**
     * Deletes the all data items at the specified
     * location from the database.
     *
     * Returns the list of data items that were deleted.
     *
     */
    public Collection<DataAndPosition<T>> delete(Position pos);

    /**
     * Deletes all data items from the database that
     * match the provided latitude and longitude
     * up to the specified number of bits of precision
     * in their geohashes.
     *
     * Returns the list of deleted data items.
     *
     */
    public Collection<DataAndPosition<T>> delete(Position pos, int bitsOfPrecision);

    /**
     * Returns true if the database contains at least one data item that
     * matches the provided latitude and longitude
     * up to the specified number of bits of precision
     * in its geohash.
     *
     */
    public boolean contains(Position pos, int bitsOfPrecision);

    /**
     * Returns all data items in the database that
     * match the provided latitude and longitude
     * up to the specified number of bits of precision
     * in their geohashes.
     *
     */
    public Collection<DataAndPosition<T>> nearby(Position pos, int bitsOfPrecision);

    /**
     * Returns an empty instance of the DB with the same settings (e.g., bits, hash factory,
     * etc.) as this instance. However, the clone will contain no data.
     *
     * @return
     */
    public ProximityDB<T> emptyClone();
}
