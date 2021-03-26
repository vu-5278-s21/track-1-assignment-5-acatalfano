package edu.vanderbilt.cs.live7;

import edu.vanderbilt.cs.live6.DataAndPosition;
import edu.vanderbilt.cs.live6.ProximityDB;

public class InsertCommand<T> implements UpdateCommand<T> {
    private final DataAndPosition<T> dataAndPosition;

    public InsertCommand(DataAndPosition<T> dataAndPos) {
        dataAndPosition = dataAndPos;
    }

    @Override
    public ProximityDB<T> execute(ProximityDB<T> proximityDB) {
        proximityDB.insert(dataAndPosition);
        return proximityDB;
    }

}
