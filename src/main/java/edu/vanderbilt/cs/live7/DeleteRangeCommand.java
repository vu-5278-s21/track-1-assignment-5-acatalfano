package edu.vanderbilt.cs.live7;

import edu.vanderbilt.cs.live6.Position;
import edu.vanderbilt.cs.live6.ProximityDB;

public class DeleteRangeCommand<T> implements UpdateCommand<T> {
    private final Position position;
    private final int bitsOfPrecision;

    public DeleteRangeCommand(
        Position pos,
        int precision
    ) {
        position = pos;
        bitsOfPrecision = precision;
    }

    @Override
    public ProximityDB<T> execute(ProximityDB<T> proximityDB) {
        proximityDB.delete(position, bitsOfPrecision);
        return proximityDB;
    }

}
