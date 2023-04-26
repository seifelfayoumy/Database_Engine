import java.io.Serializable;

public class PageInfo implements Serializable {
    public int noOfTuples;
    public int maxTuples;
    public Object minValue;
    public Object maxValue;
    public String address;
    public boolean isFull;

    public PageInfo(int maxTuples, String address) {
        this.noOfTuples = 0;
        this.maxTuples = maxTuples;
        this.minValue = null;
        this.maxValue = null;
        this.address = address;
        this.isFull = false;
    }

}
