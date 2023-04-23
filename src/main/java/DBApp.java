import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;

import com.opencsv.CSVWriter;

import java.io.FileWriter;
import java.net.URL;

public class DBApp {
    public static ArrayList<Table> tables;

    public void DBApp(){
        this.init();
    }


    public void init() {
        //update my tables static variable by getting data from resources
    }

    public void createTable(String strTableName, String strClusteringKeyColumn,
                            Hashtable<String, String> htblColNameType, Hashtable<String, String> htblColNameMin,
                            Hashtable<String, String> htblColNameMax) throws DBAppException {

        //check if that tablename doesn't exist in metadata, if it does erorr
        //check type for errors

        boolean equalHashtables = htblColNameType.keySet().equals(htblColNameMin.keySet()) && htblColNameType.keySet().equals(htblColNameMax.keySet());
        if (!equalHashtables) {
            throw new DBAppException("hashtables doesn't match");
        }

        Enumeration<String> keys = htblColNameType.keys();

        while (keys.hasMoreElements()) {
            String currColumnName = keys.nextElement();

            CSVWriter writer = null;
            try {
                writer = new CSVWriter(new FileWriter("src/resources/metadata.csv", true));
            } catch (IOException e) {
                throw new DBAppException(e.getMessage());
            }


            String clusteringKey = "False";
            if (strClusteringKeyColumn.equals(currColumnName)) {
                clusteringKey = "True";
            }
            //Create record
            String[] record = new String[]{strTableName, currColumnName, htblColNameType.get(currColumnName), clusteringKey, "null", "null", htblColNameMin.get(currColumnName), htblColNameMax.get(currColumnName)};

            //Write the record to file
            writer.writeNext(record, false);

            //close the writer
            try {
                writer.close();
            } catch (IOException e) {
                throw new DBAppException(e.getMessage());
            }
        }


    }

    public void insertIntoTable(String strTableName,
                                Hashtable<String, Object> htblColNameValue) throws DBAppException {
    }

    public void updateTable(String strTableName,
                            String strClusteringKeyValue, Hashtable<String, Object> htblColNameValue)
            throws DBAppException {
    }

    public void deleteFromTable(String strTableName,
                                Hashtable<String, Object> htblColNameValue) throws DBAppException {
    }
}
