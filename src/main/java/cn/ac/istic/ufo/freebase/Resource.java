package cn.ac.istic.ufo.freebase;

import java.util.Arrays;

/**
 * A resource could be a subject, predicate, or an object.
 * An object could be an entity or a literal value
 *
 * @author ajabal
 */
public class Resource {
    private String value;

    public Resource(String value) {
        this.setValue(value);
    }

    /**
     * check whether a resource is literal
     *
     * @return boolean
     */
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
