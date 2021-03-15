package edu.vanderbilt.cs.live7;

import java.util.Collection;

/**
 * Given a data item of type T, returns the attributes of this
 * piece of data.
 *
 * An attribute can be thought of as a key/value pair that is
 * associated with the data. Each attribute has a name, type,
 * and value.
 *
 * Another way of thinking of attribute is that they represent
 * "columns" and the data items are "rows" in a database.
 * Since the type of the data items in our database can vary,
 * we need a helper class (e.g., strategy) to extract the
 * columns present in a data item.
 *
 *
 * @param <T>
 */
public interface AttributesStrategy<T> {

    public Collection<Attribute> getAttributes(T data);
}
