import java.io.*;
import java.util.Hashtable;
import java.util.Properties;
import java.util.Vector;

public abstract class Page {
//    public int noOfTuples;
//    public int maxTuples;
//
//
//
//    public Page(int max){
//        this.noOfTuples = 0;
//        this.maxTuples = max;
//    }

    public static void writePage(String address, Vector<Hashtable<String, Object>> page) throws IOException {
        FileOutputStream fileOut = new FileOutputStream(address);
        ObjectOutputStream out = new ObjectOutputStream(fileOut);
        out.writeObject(page);
        out.close();
        fileOut.close();
    }

    public static Vector<Hashtable<String, Object>> readPage(String address) throws IOException, ClassNotFoundException {
        FileInputStream fileIn = new FileInputStream(address);
        ObjectInputStream in = new ObjectInputStream(fileIn);
        Vector<Hashtable<String, Object>> pageObject = (Vector<Hashtable<String, Object>>) in.readObject();
        in.close();
        fileIn.close();

        return pageObject;
    }




}
