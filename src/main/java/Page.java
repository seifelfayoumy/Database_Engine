import com.opencsv.exceptions.CsvValidationException;

import java.io.*;
import java.util.*;

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

            tuples = null;
            System.gc();

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
        tuples = null;
        System.gc();
        return pageInfo;
    }

    public static Hashtable<String, Object> getLastTuple(PageInfo pageInfo) throws IOException, ClassNotFoundException {
        Vector<Hashtable<String, Object>> tuples = Page.readPage(pageInfo.address);
        Hashtable<String, Object> lastElement = tuples.lastElement();
        tuples = null;
        System.gc();
        return lastElement;
    }

    public static PageInfo removeLastTuple(PageInfo pageInfo, String clusteringKey) throws IOException, ClassNotFoundException {
        Vector<Hashtable<String, Object>> tuples = Page.readPage(pageInfo.address);
        tuples.remove(tuples.size() - 1);
        pageInfo.isFull = false;
        pageInfo.noOfTuples -= 1;
        pageInfo.minValue = tuples.firstElement().get(clusteringKey);
        pageInfo.maxValue = tuples.lastElement().get(clusteringKey);
        Page.writePage(pageInfo.address, tuples);
        tuples = null;
        System.gc();
        return pageInfo;
    }

    public static PageInfo deleteFromPage(String csvAddress, String tableName, PageInfo pageInfo, Hashtable<String, Object> deleteValues, String clusteringKey) throws IOException, MaxRowsException, ClassNotFoundException, CsvValidationException {

        Vector<Hashtable<String, Object>> tuples = Page.readPage(pageInfo.address);
        ArrayList<Hashtable<String, Object>> deletedTuples = new ArrayList<Hashtable<String, Object>>();
        if (deleteValues.containsKey(clusteringKey)) {
            //BINARY SEARCH
            int l = 0, r = tuples.size() - 1;
            while (l <= r) {
                int m = l + (r - l) / 2;
                // Check if x is present at mid
                if (tuples.get(m).get(clusteringKey).equals(deleteValues.get(clusteringKey))) {
                    Enumeration<String> keys = deleteValues.keys();
                    Boolean deleteTuple = false;
                    while (keys.hasMoreElements()) {
                        String currColumnName = keys.nextElement();
                        if (tuples.get(m).get(currColumnName).equals(deleteValues.get(currColumnName))) {
                            deleteTuple = true;
                        } else {
                            deleteTuple = false;
                            break;
                        }
                    }
                    if (deleteTuple) {
                        deletedTuples.add(tuples.get(m));
                    }
                }
                // If x greater, ignore left half
                if (Table.compareClusterKey(csvAddress, tableName, tuples.get(m).get(clusteringKey), deleteValues.get(clusteringKey)) < 0)
                    l = m + 1;
                    // If x is smaller, ignore right half
                else
                    r = m - 1;
            }
        } else {
            for (int i = 0; i < pageInfo.noOfTuples; i++) {
                Enumeration<String> keys = deleteValues.keys();
                Boolean deleteTuple = false;
                while (keys.hasMoreElements()) {
                    String currColumnName = keys.nextElement();
                    if (tuples.get(i).get(currColumnName).equals(deleteValues.get(currColumnName))) {
                        deleteTuple = true;
                    } else {
                        deleteTuple = false;
                        break;
                    }
                }
                if (deleteTuple) {
                    deletedTuples.add(tuples.get(i));
                }
            }
        }

        for (int i = 0; i < deletedTuples.size(); i++) {
            tuples.remove(deletedTuples.get(i));
            pageInfo.noOfTuples -= 1;
            pageInfo.isFull = false;
        }
        if (pageInfo.noOfTuples != 0) {
            pageInfo.minValue = tuples.firstElement().get(clusteringKey);
            pageInfo.maxValue = tuples.lastElement().get(clusteringKey);
        }

        Page.writePage(pageInfo.address, tuples);
        tuples = null;
        System.gc();
        return pageInfo;

    }

    public static void updateFromPage(String csvAddress, String tableName, PageInfo pageInfo, Hashtable<String, Object> updateValues, String clusteringKey, Object clusteringKeyValue) throws IOException, MaxRowsException, ClassNotFoundException, CsvValidationException {

        Vector<Hashtable<String, Object>> tuples = Page.readPage(pageInfo.address);
        //BINARY SEARCH
        int l = 0, r = tuples.size() - 1;
        while (l <= r) {
            int m = l + (r - l) / 2;
            // Check if x is present at mid
            if (tuples.get(m).get(clusteringKey).equals(clusteringKeyValue)) {
                Enumeration<String> keys = updateValues.keys();
                while (keys.hasMoreElements()) {
                    String currColumnName = keys.nextElement();
                    tuples.get(m).replace(currColumnName, updateValues.get(currColumnName));
                }
            }
            // If x greater, ignore left half
            if (Table.compareClusterKey(csvAddress, tableName, tuples.get(m).get(clusteringKey), clusteringKeyValue) < 0)
                l = m + 1;
                // If x is smaller, ignore right half
            else
                r = m - 1;
        }

        Page.writePage(pageInfo.address, tuples);
        tuples = null;
        System.gc();
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
    public static void renameFile(String  address, String newAddress) {
        File oldFile = new File(address);
        File newFile = new File(newAddress);
        oldFile.renameTo(newFile);
    }


}
