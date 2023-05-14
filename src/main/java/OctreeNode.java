import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Hashtable;

public class OctreeNode implements Serializable {

    Object minX;
    Object maxX;
    String typeX;
    Object minY;
    Object maxY;
    String typeY;
    Object minZ;
    Object maxZ;
    String typeZ;
    ArrayList<OctreeNode> children;
    Boolean isLeaf;
    Boolean isFull;
    ArrayList<IndexReference> content;
    int maxData;


    public OctreeNode(Object minX, Object maxX, String typeX, Object minY, Object maxY, String typeY,Object minZ, Object maxZ, String typeZ, int maxData){
        this.minX = minX;
        this.maxX = maxX;
        this.typeX = typeX;
        this.minY = minY;
        this.maxY = maxY;
        this.typeY = typeY;
        this.minZ = minZ;
        this.maxZ = maxZ;
        this.typeZ = typeZ;
        this.isLeaf = true;
        this.isFull = false;
        this.content = new ArrayList<IndexReference>();
        this.maxData = maxData;
    }

    public void insert( IndexReference value){
        if(this.content.size() < this.maxData){
            this.content.add(value);
        }
        if(this.content.size() == this.maxData){
            this.isFull = true;
        }
    }

    public boolean correctPosition(Object x, Object y, Object z){
        Boolean correctX = Octree.compareKey(x,this.minX,this.typeX) >= 0 && Octree.compareKey(x,this.maxX,this.typeX) < 0;
        Boolean correctY = Octree.compareKey(y,this.minY,this.typeY) >= 0 && Octree.compareKey(y,this.maxY,this.typeY) < 0;
        Boolean correctZ = Octree.compareKey(z,this.minZ,this.typeZ) >= 0 && Octree.compareKey(z,this.maxZ,this.typeZ) < 0;

        return correctX && correctY && correctZ;
    }

    public void createChildren(){
        ArrayList<Object> divisionsX = OctreeNode.getDivisions(minX, maxX, typeX);
        ArrayList<Object> divisionsY = OctreeNode.getDivisions(minY, maxY, typeY);
        ArrayList<Object> divisionsZ = OctreeNode.getDivisions(minZ, maxZ, typeZ);

        for(int i =0;i<8;i++){
            OctreeNode n = new OctreeNode(divisionsX.get(i), divisionsX.get(i+1),typeX,divisionsY.get(i), divisionsY.get(i+1),typeY,divisionsZ.get(i), divisionsZ.get(i+1),typeZ,maxData);
            this.children.add(n);
        }
        this.isLeaf = false;
    }

    static ArrayList<Object> getDivisions(Object min, Object max, String type){
        ArrayList<Object> result = new ArrayList<Object>();
        Object middle4 = Octree.getMiddle(min, max,type);
        Object middle2 = Octree.getMiddle(min, middle4,type);
        Object middle6 = Octree.getMiddle(middle4, max,type);
        Object middle1 = Octree.getMiddle(min, middle2,type);
        Object middle3 = Octree.getMiddle(middle2, middle4,type);
        Object middle5 = Octree.getMiddle(middle4, middle6,type);
        Object middle7 = Octree.getMiddle(middle6, max,type);

        result.add(min);
        result.add(middle1);
        result.add(middle2);
        result.add(middle3);
        result.add(middle4);
        result.add(middle5);
        result.add(middle6);
        result.add(middle7);
        result.add(max);

        return  result;

    //    min - m1 -m2 -m3 -m4 -m5 -m6 -m7 -max
    }



}