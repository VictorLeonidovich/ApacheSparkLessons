package _05_Tuples._01_RDDs_of_Objects;

public class IntegerWithSquareRoot {

    private final Integer originalInteger;
    private final double squareRoot;

    public IntegerWithSquareRoot(final Integer value) {
        this.originalInteger = value;
        this.squareRoot = Math.sqrt(originalInteger);
    }

    public double getSquareRoot() {
        return squareRoot;
    }

    public double getOriginalInteger() {
        return originalInteger;
    }
}
