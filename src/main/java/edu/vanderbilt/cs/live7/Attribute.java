package edu.vanderbilt.cs.live7;

public class Attribute<T> {

    private final String name;
    private final Class<T> type;
    private final T value;

    public Attribute(String name, Class<T> type, T value) {
        this.name = name;
        this.type = type;
        this.value = value;
    }

    public T getValue() {return this.value;}

    public String getName() {
        return this.name;
    }

    public Class<T> getType() {
        return this.type;
    }
}
