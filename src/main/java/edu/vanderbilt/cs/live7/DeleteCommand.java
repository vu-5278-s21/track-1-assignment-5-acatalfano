package edu.vanderbilt.cs.live7;

import edu.vanderbilt.cs.live6.Position;
import edu.vanderbilt.cs.live6.ProximityDB;

public class DeleteCommand<T> implements UpdateCommand<T> {
    private final Position position;

    public DeleteCommand(Position pos) {
        position = pos;
    }

    @Override
    public ProximityDB<T> execute(ProximityDB<T> proximityDB) {
        proximityDB.delete(position);
        return proximityDB;
    }

}
