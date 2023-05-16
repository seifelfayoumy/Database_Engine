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
    Boolean lastNode;


    public OctreeNode(Object minX, Object maxX, String typeX, Object minY, Object maxY, String typeY, Object minZ, Object maxZ, String typeZ, int maxData) {
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
        lastNode = false;
    }

    public void insert(IndexReference value) {
        if (this.content.size() < this.maxData) {
            this.content.add(value);
        }
        if (this.content.size() == this.maxData) {
            this.isFull = true;
        }
    }

    public boolean correctPosition(Object x, Object y, Object z) {
        if (this.lastNode) {
            Boolean correctX = Octree.compareKey(x, this.minX, this.typeX) >= 0 && Octree.compareKey(x, this.maxX, this.typeX) <= 0;
            Boolean correctY = Octree.compareKey(y, this.minY, this.typeY) >= 0 && Octree.compareKey(y, this.maxY, this.typeY) <= 0;
            Boolean correctZ = Octree.compareKey(z, this.minZ, this.typeZ) >= 0 && Octree.compareKey(z, this.maxZ, this.typeZ) <= 0;
            return correctX && correctY && correctZ;
        } else {
            Boolean correctX = Octree.compareKey(x, this.minX, this.typeX) >= 0 && Octree.compareKey(x, this.maxX, this.typeX) < 0;
            Boolean correctY = Octree.compareKey(y, this.minY, this.typeY) >= 0 && Octree.compareKey(y, this.maxY, this.typeY) < 0;
            Boolean correctZ = Octree.compareKey(z, this.minZ, this.typeZ) >= 0 && Octree.compareKey(z, this.maxZ, this.typeZ) < 0;
            return correctX && correctY && correctZ;
        }
    }


    public void createChildren() {
        ArrayList<Object> divisionsX = OctreeNode.getDivisions(minX, maxX, typeX);
        ArrayList<Object> divisionsY = OctreeNode.getDivisions(minY, maxY, typeY);
        ArrayList<Object> divisionsZ = OctreeNode.getDivisions(minZ, maxZ, typeZ);

        this.children = new ArrayList<OctreeNode>();
        for (int dx = 0; dx < 2; dx++) {
            for (int dy = 0; dy < 2; dy++) {
                for (int dz = 0; dz < 2; dz++) {
                    OctreeNode n = new OctreeNode(
                            divisionsX.get(dx), divisionsX.get(dx + 1), typeX,
                            divisionsY.get(dy), divisionsY.get(dy + 1), typeY,
                            divisionsZ.get(dz), divisionsZ.get(dz + 1), typeZ,
                            maxData
                    );
                    if (dx == 1 && dy == 1 && dz == 1) {
                        n.lastNode = true;
                    }
                    this.children.add(n);
                }
            }
        }
        this.isLeaf = false;
    }


    static ArrayList<Object> getDivisions(Object min, Object max, String type) {
        ArrayList<Object> result = new ArrayList<Object>();

        Object middle = Octree.getMiddle(min, max, type);

        result.add(min);
        result.add(middle);
        result.add(max);

        return result;
    }


}