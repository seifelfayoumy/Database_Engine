import java.io.Serializable;

public class NullObject extends Object implements Serializable {
    public NullObject() {

    }

    @Override
    public String toString() {
        return "null";
    }
}
