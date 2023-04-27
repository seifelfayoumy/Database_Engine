import java.io.*;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Vector;

public abstract class Page implements Serializable {

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

    public static PageInfo insertToPage(PageInfo pageInfo, Hashtable<String, Object> tuple, String clusteringKey, String tableName, String csvAddress) throws IOException, MaxRowsException, ClassNotFoundException, DuplicateRowException {

        Vector<Hashtable<String, Object>> tuples = Page.readPage(pageInfo.address);
        for (int i = 0; i < tuples.size(); i++) {
            if (tuples.get(i).get(clusteringKey).equals(tuple.get(clusteringKey))) {
                throw new DuplicateRowException("duplicate clustering key");
            }
        }
        if (pageInfo.noOfTuples < pageInfo.maxTuples) {
            tuples.add(tuple);
            Collections.sort(tuples, new PageComparator(tableName, csvAddress, clusteringKey));
            pageInfo.minValue = tuples.firstElement().get(clusteringKey);
            pageInfo.maxValue = tuples.lastElement().get(clusteringKey);
            pageInfo.noOfTuples += 1;
            if (pageInfo.noOfTuples == pageInfo.maxTuples) {
                pageInfo.isFull = true;
            }
            Page.writePage(pageInfo.address, tuples);

            return pageInfo;
        } else {
            throw new MaxRowsException("MAX ROWS");
        }

    }

    public static PageInfo insertToFirstPage(PageInfo pageInfo, Hashtable<String, Object> tuple, String clusteringKey) throws IOException, MaxRowsException, ClassNotFoundException, DuplicateRowException {

        Vector<Hashtable<String, Object>> tuples = new Vector<Hashtable<String, Object>>();
        tuples.add(tuple);
        pageInfo.noOfTuples += 1;
        pageInfo.minValue = tuple.get(clusteringKey);
        pageInfo.maxValue = tuple.get(clusteringKey);
        Page.writePage(pageInfo.address, tuples);
        return pageInfo;

    }

    public static Hashtable<String, Object> getLastTuple(PageInfo pageInfo) throws IOException, ClassNotFoundException {
        Vector<Hashtable<String, Object>> tuples = Page.readPage(pageInfo.address);
        return tuples.lastElement();
    }

    public static PageInfo removeLastTuple(PageInfo pageInfo, String clusteringKey) throws IOException, ClassNotFoundException {
        Vector<Hashtable<String, Object>> tuples = Page.readPage(pageInfo.address);
        tuples.remove(tuples.size() - 1);
        pageInfo.isFull = false;
        pageInfo.noOfTuples -= 1;
        pageInfo.minValue = tuples.firstElement().get(clusteringKey);
        pageInfo.maxValue = tuples.lastElement().get(clusteringKey);
        Page.writePage(pageInfo.address, tuples);
        return pageInfo;
    }

    public static PageInfo deleteFromPage(PageInfo pageInfo, Hashtable<String, Object> deleteValues, String clusteringKey) throws IOException, MaxRowsException, ClassNotFoundException {

        Vector<Hashtable<String, Object>> tuples = Page.readPage(pageInfo.address);

        for (int i = 0; i < pageInfo.noOfTuples; i++) {
            Enumeration<String> keys = deleteValues.keys();
            Boolean deleteTuple = false;
            while (keys.hasMoreElements()) {
                String currColumnName = keys.nextElement();
                if (tuples.get(i).get(currColumnName).equals(deleteValues.get(currColumnName))) {
                    deleteTuple = true;
                } else {
                    deleteTuple = false;
                }
            }
            if (deleteTuple) {
                tuples.remove(i);
                pageInfo.noOfTuples -= 1;
                pageInfo.isFull = false;
            }
        }
        if (pageInfo.noOfTuples != 0) {
            pageInfo.minValue = tuples.firstElement().get(clusteringKey);
            pageInfo.maxValue = tuples.lastElement().get(clusteringKey);
        }

        Page.writePage(pageInfo.address, tuples);
        return pageInfo;

    }

    public static void updateFromPage(PageInfo pageInfo, Hashtable<String, Object> updateValues, String clusteringKey, String clusteringKeyValue) throws IOException, MaxRowsException, ClassNotFoundException {

        Vector<Hashtable<String, Object>> tuples = Page.readPage(pageInfo.address);
        for (int i = 0; i < pageInfo.noOfTuples; i++) {
            if (tuples.get(i).get(clusteringKey).equals(clusteringKeyValue)) {
                Enumeration<String> keys = updateValues.keys();

                while (keys.hasMoreElements()) {
                    String currColumnName = keys.nextElement();
                    tuples.get(i).replace(currColumnName, updateValues.get(currColumnName));
                }
            }
        }
        Page.writePage(pageInfo.address, tuples);
    }


    public static void deletePage(String address) {
        File pageFile = new File(address);
        pageFile.delete();
    }

    public static void createPage(String address) {
        File pageFile = new File(address);
        try {
            pageFile.createNewFile();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static PageInfo renameFile(PageInfo pageInfo, String newAddress) {
        File oldFile = new File(pageInfo.address);
        File newFile = new File(newAddress);
        oldFile.renameTo(newFile);
        pageInfo.address = newAddress;
        return pageInfo;
    }


}
