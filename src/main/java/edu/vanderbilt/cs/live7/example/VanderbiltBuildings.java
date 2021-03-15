package edu.vanderbilt.cs.live7.example;

import edu.vanderbilt.cs.live6.DataAndPosition;
import edu.vanderbilt.cs.live6.Position;
import edu.vanderbilt.cs.live6.ProximityDB;
import edu.vanderbilt.cs.live6.ProximityDBFactory;

import edu.vanderbilt.cs.live7.Attribute;
import edu.vanderbilt.cs.live7.ProximityStreamDB;

import java.util.Collection;
import java.util.Map;

public class VanderbiltBuildings {

    public static void main(String[] args){

        Building kirklandHall = new Building("Kirkland Hall", 150000, 5);
        Building fgh = new Building("Featheringill Hall", 95023.4, 38);
        Building esb = new Building("Engineering Sciences Building", 218793.34, 10);

        ProximityStreamDB strmdb = null; // Create an instance of your ProximityStreamDB impl
        strmdb.insert( DataAndPosition.with(36.145050, 86.803365, fgh) );
        strmdb.insert( DataAndPosition.with(36.148345, 86.802909, kirklandHall));
        strmdb.insert( DataAndPosition.with(36.143171, 86.805772, esb));

        Collection<DataAndPosition<Building>> buildingsNearFgh =
                strmdb.nearby(Position.with(36.145050, 86.803365), 28);

        for(DataAndPosition<Building> buildingAndPos : buildingsNearFgh){
            System.out.println(buildingAndPos.getData().getName() + " is located at " +
                    buildingAndPos.getLatitude() + "," + buildingAndPos.getLongitude()
            );
        }

        double averageBuildingSqft = strmdb.averageNearby(
                a -> BuildingAttributesStrategy.SIZE_IN_SQUARE_FEET.equals(a.getName()),
                Position.with(36.145050, 86.803365),28 ).getAsDouble();

        System.out.println("The average building size near FGH Hall is: " + averageBuildingSqft + "sqft");

        Map<Object,Long> buildingSizeHistogram = strmdb.histogramNearby(
                a -> BuildingAttributesStrategy.SIZE_IN_SQUARE_FEET.equals(a.getName()),
                Position.with(36.145050, 86.803365),
                16 );

        for(Map.Entry<Object, Long> countForValue : buildingSizeHistogram.entrySet()){
            System.out.println("There are " + countForValue.getValue() + " buildings with " +
                      countForValue.getKey() + "sqft nearby." );
        }

    }


}
