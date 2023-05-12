import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Hashtable;

public class OctreeNode{

    Object min;
    Object max;
    ArrayList<OctreeNode> children;
    Boolean isLeaf;
    Boolean isFull;
    Hashtable<Object,String> content;
    int maxData;


    public OctreeNode(Object min, Object max, int maxData){
        this.min = min;
        this.max = max;
        this.isLeaf = true;
        this.isLeaf = false;
        this.content = new Hashtable<Object, String>();
        this.maxData = maxData;
    }

    public void insert( Object key, String pageAdress){
        if(this.content.size() < this.maxData){
            this.content.put(key, pageAdress);
        }
        if(this.content.size() == this.maxData){
            this.isFull = true;
        }
    }

    public void createChildren(){
        for(int i =0;i<8;i++){

        }
    }

}