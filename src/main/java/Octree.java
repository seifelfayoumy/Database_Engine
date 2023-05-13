import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;

public class Octree{
    String tableName;
    String[] columns;
    ArrayList<OctreeNode> nodes;
    int maxPerNode;
    String dataType;
    OctreeNode root;

    public Octree(Object minX, Object maxX, String typeX, Object minY, Object maxY, String typeY,Object minZ, Object maxZ, String typeZ, String tableName, String[] columns, int maxPerNode, String dataType){
        this.nodes = new ArrayList<OctreeNode>();
        this.tableName = tableName;
        this.columns = columns;
        this.maxPerNode = maxPerNode;
        this.root = new OctreeNode(minX, maxX, typeX,minY,maxY,typeY,minZ,maxZ,typeZ,maxPerNode);
        this.dataType = dataType;
    }

    public void save() throws Exception {
        FileOutputStream fileOut = new FileOutputStream("src/resources/index_" + this.tableName + "_"+ this.columns[0] + "_"+this.columns[1]+"_"+this.columns[2]+".ser");
        ObjectOutputStream out = new ObjectOutputStream(fileOut);
        out.writeObject(this);
        out.close();
        fileOut.close();
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
}