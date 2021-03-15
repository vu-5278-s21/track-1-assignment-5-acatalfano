package edu.vanderbilt.cs.live7.example;

import edu.vanderbilt.cs.live7.Attribute;
import edu.vanderbilt.cs.live7.AttributesStrategy;

import java.util.Arrays;
import java.util.Collection;

public class BuildingAttributesStrategy implements AttributesStrategy<Building> {

    public static final String SIZE_IN_SQUARE_FEET = "sizeInSquareFeet";
    public static final String CLASSROOMS = "classrooms";
    public static final String NAME = "name";

    @Override
    public Collection<Attribute> getAttributes(Building data) {
        Attribute sqft = new Attribute(SIZE_IN_SQUARE_FEET, Double.class, data.getSizeInSquareFeet());
        Attribute classrooms = new Attribute(CLASSROOMS, Double.class, data.getClassRooms());
        Attribute name = new Attribute(NAME, String.class, data.getName());

        return Arrays.asList(sqft, classrooms, name);
    }
}
