package cn.ac.istic.ufo.freebase;

public class Resource {
    private String value;

    public Resource(String value) {
        this.setValue(value);
    }

    public boolean isLiteral() {
        return this.getValue().startsWith("\"");
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
