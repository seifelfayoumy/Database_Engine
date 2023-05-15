import java.io.Serializable;

public class IndexReference implements Serializable {
    Object x;
    Object y;
    Object z;
    String pageAddress;

    public IndexReference(Object x, Object y, Object z, String pageAddress){
        this.x = x;
        this.y = y;
        this.z = z;
        this.pageAddress = pageAddress;
    }

    public String toString(){
        return "(x: "+ this.x +", y: " +this.y +", z: "+ this.z+") at address: "+this.pageAddress;
    }
}
