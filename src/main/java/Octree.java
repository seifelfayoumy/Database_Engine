import java.io.*;
import java.util.*;

public class Octree implements Serializable {
    String tableName;
    String[] columns;
    ArrayList<OctreeNode> nodes;
    int maxPerNode;

    OctreeNode root;

    public Octree(Object minX, Object maxX, String typeX, Object minY, Object maxY, String typeY,Object minZ, Object maxZ, String typeZ, String tableName, String[] columns, int maxPerNode){
        this.nodes = new ArrayList<OctreeNode>();
        this.tableName = tableName;
        this.columns = columns;
        this.maxPerNode = maxPerNode;
        this.root = new OctreeNode(minX, maxX, typeX,minY,maxY,typeY,minZ,maxZ,typeZ,maxPerNode);
    }

    public void save(String address) throws Exception {
        FileOutputStream fileOut = new FileOutputStream(address);
        ObjectOutputStream out = new ObjectOutputStream(fileOut);
        out.writeObject(this);
        out.close();
        fileOut.close();
    }
    public static Octree read(String address) throws Exception {
        FileInputStream fileIn = new FileInputStream(address);
        ObjectInputStream in = new ObjectInputStream(fileIn);
        Octree tree = (Octree) in.readObject();
        in.close();
        fileIn.close();

        return tree;
    }

    public IndexReference search(Object x, Object y, Object z){
        return this.searchHelper(x,y,z,this.root);
    }
    private IndexReference searchHelper(Object x, Object y, Object z, OctreeNode node) {
        if (node.isLeaf) {
            for (IndexReference ref : node.content) {
                if (Octree.compareKey(x, ref.x, node.typeX) == 0 &&
                        Octree.compareKey(y, ref.y, node.typeY) == 0 &&
                        Octree.compareKey(z, ref.z, node.typeZ) == 0) {
                    return ref;
                }
            }
        } else {
            for (OctreeNode child : node.children) {
                if (child.correctPosition(x, y, z)) {
                    return this.searchHelper(x, y, z, child);
                }
            }
        }
        return null;
    }

    public void delete(Object x, Object y, Object z) {
        deleteHelper(x, y, z, root);
    }

    private void deleteHelper(Object x, Object y, Object z, OctreeNode node) {
        if (node.isLeaf) {
            Iterator<IndexReference> iterator = node.content.iterator();
            while (iterator.hasNext()) {
                IndexReference ref = iterator.next();
                if (Octree.compareKey(x, ref.x, node.typeX) == 0 &&
                        Octree.compareKey(y, ref.y, node.typeY) == 0 &&
                        Octree.compareKey(z, ref.z, node.typeZ) == 0) {
                    iterator.remove();
                    node.isFull = false;
                    break;
                }
            }
        } else {
            for (OctreeNode child : node.children) {
                if (child.correctPosition(x, y, z)) {
                    deleteHelper(x, y, z, child);
                    break;
                }
            }
            // After deletion, check if total content of all children is less than the maximum per node.
            // If so, remove children and make this node a leaf with all the children's content.
            int totalContent = 0;
            for (OctreeNode child : node.children) {
                totalContent += child.content.size();
            }
            if (totalContent <= node.maxData) {
                for (OctreeNode child : node.children) {
                    node.content.addAll(child.content);
                }
                node.children.clear();
                node.isLeaf = true;
            }
        }
    }



    public void insert(IndexReference value){
        this.insertHelper(value, this.root);
    }

    private void insertHelper(IndexReference value, OctreeNode node){
        if(node.isLeaf){
            if(!node.isFull){
                node.insert(value);
                return;
            }else{
                node.createChildren();
                this.insertHelper(value, node);
            }
        }else{
            for(int i=0;i<8;i++){
                OctreeNode currNode = node.children.get(i);
                if(currNode.correctPosition(value.x, value.y, value.z)){
                    this.insertHelper(value, currNode);
                }
            }
        }
    }

    public static int compareKey(Object o1, Object o2, String dataType){
        switch (dataType) {
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

    static String getMiddleString(String S, String T)
    {
        int N = Math.min(S.length(), T.length());
        // Stores the base 26 digits after addition
        int[] a1 = new int[N + 1];

        for (int i = 0; i < N; i++) {
            a1[i + 1] = (int)S.charAt(i) - 97
                    + (int)T.charAt(i) - 97;
        }

        // Iterate from right to left
        // and add carry to next position
        for (int i = N; i >= 1; i--) {
            a1[i - 1] += (int)a1[i] / 26;
            a1[i] %= 26;
        }

        // Reduce the number to find the middle
        // string by dividing each position by 2
        for (int i = 0; i <= N; i++) {

            // If current value is odd,
            // carry 26 to the next index value
            if ((a1[i] & 1) != 0) {

                if (i + 1 <= N) {
                    a1[i + 1] += 26;
                }
            }

            a1[i] = (int)a1[i] / 2;
        }
        String result = "";
        for (int i = 1; i <= N; i++) {
            result+=((char)(a1[i] + 97));
        }
        return result;
    }

    static Object getMiddle(Object o1, Object o2, String type){
        switch (type) {
            case "java.lang.String":
                return Octree.getMiddleString((String)o1,(String) o2);
            case "java.lang.Integer":
                return (((Integer) o2)+ ((Integer) o1)) /2;
            case "java.lang.Double":
                return (((Double) o2)+ ((Double) o1)) /2;
            case "java.util.Date":
                Date d1 = (Date) o1;
                Date d2 = (Date) o2;
                int monthDiff = (d2.getMonth() - d1.getMonth())/2;
                int hourDiff = (d2.getHours() - d1.getHours())/2;
                int yearDiff = (d2.getYear() - d1.getYear())/2;
                Calendar c = Calendar.getInstance();
                c.setTime(d1);
                c.add(Calendar.MONTH,monthDiff);
                c.add(Calendar.HOUR,hourDiff);
                c.add(Calendar.YEAR,yearDiff);
                return c.getTime();
            default:
                return 0;
        }
    }

    public static void printOctree(Octree tree){
        Octree.printOctreeHelper(tree.root);
    }
    private static void printOctreeHelper(OctreeNode node){
        if(node.isLeaf){
            System.out.println("[("+node.minX+ ","+ node.minY+","+ node.minZ+") ("+node.maxX+","+node.maxY+","+node.maxZ+")]");
            for (int i=0;i<node.content.size();i++){
                System.out.print(node.content.get(i));
            }
        }else{
            System.out.println("[("+node.minX+ ","+ node.minY+","+ node.minZ+") ("+node.maxX+","+node.maxY+","+node.maxZ+")]");
        }
    }

}