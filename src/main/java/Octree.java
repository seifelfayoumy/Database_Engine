import java.io.*;
import java.util.*;

public class Octree implements Serializable {
    String tableName;
    String[] columns;
    ArrayList<OctreeNode> nodes;
    int maxPerNode;

    OctreeNode root;

    public Octree(Object minX, Object maxX, String typeX, Object minY, Object maxY, String typeY, Object minZ, Object maxZ, String typeZ, String tableName, String[] columns, int maxPerNode) {
        this.nodes = new ArrayList<OctreeNode>();
        this.tableName = tableName;
        this.columns = columns;
        this.maxPerNode = maxPerNode;
        this.root = new OctreeNode(minX, maxX, typeX, minY, maxY, typeY, minZ, maxZ, typeZ, maxPerNode);
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

    public IndexReference search(Object x, Object y, Object z) {
        return this.searchHelper(x, y, z, this.root);
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
            }else{
                node.createChildren();
                ArrayList<IndexReference> oldContent = new ArrayList<IndexReference>(node.content);
                node.content.clear();
                node.isFull = false;

                for(int i=0;i<oldContent.size();i++){
                    for(int j =0;j<8;j++){
                        if(node.children.get(j).correctPosition(oldContent.get(i).x,oldContent.get(i).y,oldContent.get(i).z)){
                            node.children.get(j).insert(oldContent.get(i));
                            break;
                        }
                    }
                }
                this.insertHelper(value, node);
            }
        }else{
            for(int i=0;i<8;i++){
                OctreeNode currNode = node.children.get(i);
                if(currNode.correctPosition(value.x, value.y, value.z)){
                    this.insertHelper(value, currNode);
                    break;
                }
            }
        }
    }





    public static int compareKey(Object o1, Object o2, String dataType) {
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

    static String getMiddleString(String a, String b) {
        // ensure a is lexicographically smaller than b
        if (a.compareTo(b) > 0) {
            String temp = a;
            a = b;
            b = temp;
        }
        char[] aArr = a.toCharArray();
        char[] bArr = b.toCharArray();
        char[] midArr = new char[Math.max(aArr.length, bArr.length)];
        Arrays.fill(midArr, 'a');

        for (int i = 0; i < Math.min(aArr.length, bArr.length); i++) {
            int avg = (aArr[i] + bArr[i]) / 2;
            midArr[i] = (char) avg;
            if(aArr[i] < avg && avg < bArr[i]) break;
        }

        String middle = new String(midArr);
        if (a.compareTo(middle) < 0 && middle.compareTo(b) < 0) {
            return middle;
        } else {
            return null;
        }
    }

    static Object getMiddle(Object o1, Object o2, String type) {
        switch (type) {
            case "java.lang.String":
                return Octree.getMiddleString((String) o1, (String) o2);
            case "java.lang.Integer":
                return (((Integer) o2) + ((Integer) o1)) / 2;
            case "java.lang.Double":
                return (((Double) o2) + ((Double) o1)) / 2;
            case "java.util.Date":
                Date d1 = (Date) o1;
                Date d2 = (Date) o2;
                long t1 = d1.getTime();
                long t2 = d2.getTime();
                return new Date((t1 + t2) / 2);
            default:
                return 0;
        }
    }

    public void printOctree() {
        printNode(root, "");
    }

    private void printNode(OctreeNode node, String indent) {
        if (node == null) {
            System.out.println(indent + "null");
            return;
        }

        System.out.println(indent + "Node");
        System.out.println(indent + "Min X: " + node.minX + " Type X: " + node.typeX);
        System.out.println(indent + "Max X: " + node.maxX + " Type X: " + node.typeX);
        System.out.println(indent + "Min Y: " + node.minY + " Type Y: " + node.typeY);
        System.out.println(indent + "Max Y: " + node.maxY + " Type Y: " + node.typeY);
        System.out.println(indent + "Min Z: " + node.minZ + " Type Z: " + node.typeZ);
        System.out.println(indent + "Max Z: " + node.maxZ + " Type Z: " + node.typeZ);

        if (node.isLeaf) {
            System.out.println(indent + "Content: ");
            for (int i = 0; i < node.content.size(); i++) {
                System.out.println(node.content.get(i));
            }
        } else {
            for (OctreeNode child : node.children) {
                printNode(child, indent + "  ");
            }
        }
    }

    public ArrayList<OctreeNode> getAllNodes() {
        ArrayList<OctreeNode> nodes = new ArrayList<>();
        collectNodes(this.root, nodes);
        return nodes;
    }

    private void collectNodes(OctreeNode node, ArrayList<OctreeNode> nodes) {
        if (node == null) {
            return;
        }

        nodes.add(node);

        if (!node.isLeaf) {
            for (OctreeNode child : node.children) {
                collectNodes(child, nodes);
            }
        }
    }


}