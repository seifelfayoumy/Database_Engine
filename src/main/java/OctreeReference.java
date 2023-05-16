import java.io.Serializable;

public class OctreeReference implements Serializable {
    String[] columns;
    String address;
    String name;

    public OctreeReference(String[] columns, String address, String name) {
        this.columns = columns;
        this.address = address;
        this.name = name;
    }


}
