package edu.vanderbilt.cs.live7.example;

import java.util.Objects;

public class Building {

    private String name;

    private double sizeInSquareFeet;

    private double classRooms;

    public Building(String name, double sqft, double classRooms) {
        this.sizeInSquareFeet = sqft;
        this.classRooms = classRooms;
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public double getSizeInSquareFeet() {
        return sizeInSquareFeet;
    }

    public void setSizeInSquareFeet(double sizeInSquareFeet) {
        this.sizeInSquareFeet = sizeInSquareFeet;
    }

    public double getClassRooms() {
        return classRooms;
    }

    public void setClassRooms(double classRooms) {
        this.classRooms = classRooms;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Building building = (Building) o;
        return Objects.equals(name, building.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }
}
