import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Date;

public class Octree{
    String tableName;
    String columnName;
    ArrayList<OctreeNode> nodes;
    Object min;
    Object max;
    int maxPerNode;
    String dataType;
    OctreeNode root;
    public Octree(Object min, Object max, String tableName, String columnName, int maxPerNode, String dataType){
        this.nodes = new ArrayList<OctreeNode>();
        this.min = min;
        this.max = max;
        this.tableName = tableName;
        this.columnName = columnName;
        this.maxPerNode = maxPerNode;
        this.root = new OctreeNode(min,max,maxPerNode);
        this.dataType = dataType;
    }

    public void save() throws Exception {
        FileOutputStream fileOut = new FileOutputStream("src/resources/index_" + this.tableName + "_"+ this.columnName+".ser");
        ObjectOutputStream out = new ObjectOutputStream(fileOut);
        out.writeObject(this);
        out.close();
        fileOut.close();
    }

    public void insert(Object key, String pageAddress){
        this.insertHelper(key, pageAddress, this.root);
    }

    private void insertHelper(Object key, String pageAdrress, OctreeNode node){
        if(node.isLeaf){
            if(!node.isFull){
                node.insert(key,pageAdrress);
                return;
            }else{

            }
        }else{
            for(int i=0;i<8;i++){
                OctreeNode currNode = node.children.get(i);
                if(this.compareKey(key, currNode.min) >= 0 && this.compareKey(key, currNode.min) < 0){
                    this.insertHelper(key, pageAdrress, currNode);
                }
            }
        }
    }

    public int compareKey(Object o1, Object o2){
        switch (this.dataType) {
            case "java.lang.String":
                return ((String) o1).compareTo((String) o2);
            case "java.lang.Integer":
                return ((Integer) o1).compareTo((Integer) o2);
            case "java.lang.Double":
                return ((Double) o1).compareTo((Double) o2);
            case "java.util.Date":
                return ((Date) o1).compareTo((Date) o2);
            default:
                return 0;
        }
    }
}