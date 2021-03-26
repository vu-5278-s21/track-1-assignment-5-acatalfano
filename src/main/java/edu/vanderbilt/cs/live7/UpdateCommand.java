package edu.vanderbilt.cs.live7;

import edu.vanderbilt.cs.live6.ProximityDB;

public interface UpdateCommand<T> {
	public ProximityDB<T> execute(ProximityDB<T> proximityDB);
}
