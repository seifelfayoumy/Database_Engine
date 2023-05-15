import java.io.*;
import java.util.*;

public class Octree implements Serializable {
    String tableName;
    String[] columns;
    ArrayList<OctreeNode> nodes;
    int maxPerNode;
    String name;

    OctreeNode root;

    public Octree(Object minX, Object maxX, String typeX, Object minY, Object maxY, String typeY, Object minZ, Object maxZ, String typeZ, String tableName, String[] columns, int maxPerNode, String name) {
        this.nodes = new ArrayList<OctreeNode>();
        this.tableName = tableName;
        this.columns = columns;
        this.maxPerNode = maxPerNode;
        this.name = name;
        this.root = new OctreeNode(minX, maxX, typeX, minY, maxY, typeY, minZ, maxZ, typeZ, maxPerNode);
        this.root.lastNode = true;
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
                }
            }
        } else {
            for (OctreeNode child : node.children) {
                if (child.correctPosition(x, y, z)) {
                    deleteHelper(x, y, z, child);
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


    public void insert(IndexReference value) {
        this.insertHelper(value, this.root);
    }

    private void insertHelper(IndexReference value, OctreeNode node) {
        if (node.isLeaf) {
            if (node.isFull) {
                node.createChildren(); // Create child nodes
                ArrayList<IndexReference> oldContent = new ArrayList<>(node.content);
                oldContent.add(value);
                node.content.clear();
                node.isFull = false;
                node.isLeaf = false;

                for (IndexReference oldValue : oldContent) {
                    insertHelper(oldValue, node);
                }
            } else {
                node.insert(value);
            }
        } else {
            for (int i = 0; i < 8; i++) {
                OctreeNode currNode = node.children.get(i);
                if (currNode.correctPosition(value.x, value.y, value.z)) {
                    insertHelper(value, currNode);
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
        int shorterLength = Math.min(a.length(), b.length());
        char[] middleArr = new char[shorterLength];

        for (int i = 0; i < shorterLength; i++) {
            int total = a.charAt(i) + b.charAt(i);
            if (total > 255) {
                total -= 256;
            }
            middleArr[i] = (char) total;
        }

        return new String(middleArr);
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
                Date a = (Date) o1;
                Date b = (Date) o2;
                if (a.after(b)) {
                    Date temp = a;
                    a = b;
                    b = temp;
                }

                // Get the difference between the two dates.
                long difference = b.getTime() - a.getTime();

                // Calculate the midpoint.
                long midpoint = difference / 2;

                // Create a new Date object from the midpoint.
                return new Date(a.getTime() + midpoint);
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
        System.out.println(indent + "last? "+node.lastNode);

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