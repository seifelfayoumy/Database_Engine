public class PageInfo {
    public int noOfTuples;
    public int maxTuples;
    public String minValue;
    public String maxValue;
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
