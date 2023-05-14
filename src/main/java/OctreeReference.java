import java.io.Serializable;

public class OctreeReference implements Serializable {
    String[] columns;
    String address;

    public  OctreeReference(String[] columns, String address){
        this.columns = columns;
        this.address = address;
    }


}
