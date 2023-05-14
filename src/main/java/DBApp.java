import com.opencsv.CSVWriter;

import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;

public class DBApp {
    public ArrayList<Table> tables;

    public DBApp() throws DBAppException {
        this.tables = new ArrayList<Table>();
        this.init();
    }


    public void init() throws DBAppException {
        try {
            this.tables = Table.getTablesFromDisk("src/resources");
        } catch (Exception e) {
            throw new DBAppException(e.getMessage());
        }
    }

    public void createTable(String strTableName, String strClusteringKeyColumn,
                            Hashtable<String, String> htblColNameType, Hashtable<String, String> htblColNameMin,
                            Hashtable<String, String> htblColNameMax) throws DBAppException {

        try {
            boolean tableExists = Table.tableExistsOnDisk("src/resources/metadata.csv", strTableName);
            if (tableExists) {
                throw new DBAppException("table name already exists");
            }
            boolean correctTypes = Table.validateTypes(htblColNameType);
            if (!correctTypes) {
                throw new DBAppException("invalid column types");
            }
        } catch (Exception e) {
            throw new DBAppException(e.getMessage());
        }


        boolean equalHashtables = htblColNameType.keySet().equals(htblColNameMin.keySet()) && htblColNameType.keySet().equals(htblColNameMax.keySet());
        if (!equalHashtables) {
            throw new DBAppException("hashtables doesn't match");
        }

        if (!htblColNameType.containsKey(strClusteringKeyColumn)) {
            throw new DBAppException("invalid clustering key");
        }

        try {
            Table.writeTableToCSV("src/resources/metadata.csv", strTableName, strClusteringKeyColumn, htblColNameType, htblColNameMin, htblColNameMax);
            Table table = new Table(strTableName, strClusteringKeyColumn);
            this.tables.add(table);
        } catch (Exception e) {
            throw new DBAppException(e.getMessage());
        }


    }

    public void insertIntoTable(String strTableName,
                                Hashtable<String, Object> htblColNameValue) throws DBAppException {

        Hashtable<String, Object> updatedTuple = Table.validateTupleOnInsert("src/resources/metadata.csv", strTableName, htblColNameValue);
        Enumeration<String> keys = updatedTuple.keys();
        while (keys.hasMoreElements()) {
            String currColumnName = keys.nextElement();
            if(updatedTuple.get(currColumnName) instanceof String){
                String newValue = updatedTuple.get(currColumnName).toString().toLowerCase();
                updatedTuple.replace(currColumnName, newValue);
            }
        }
        for (int i = 0; i < this.tables.size(); i++) {
            if (this.tables.get(i).name.equals(strTableName)) {
                try {
                    this.tables.get(i).insert(updatedTuple);
                } catch (Exception e) {
                    throw new DBAppException(e.getMessage());
                }
            }
        }
    }

    public void updateTable(String strTableName,
                            String strClusteringKeyValue, Hashtable<String, Object> htblColNameValue)
            throws DBAppException {

        Table.validateTupleOnUpdate("src/resources/metadata.csv", strTableName, htblColNameValue);
        for (int i = 0; i < this.tables.size(); i++) {
            if (this.tables.get(i).name.equals(strTableName)) {
                try {
                    this.tables.get(i).update(strTableName, strClusteringKeyValue, htblColNameValue);
                } catch (Exception e) {
                    throw new DBAppException(e.getMessage());
                }
            }
        }
    }

    public void deleteFromTable(String strTableName,
                                Hashtable<String, Object> htblColNameValue) throws DBAppException {
        Table.validateTupleOnDelete("src/resources/metadata.csv", strTableName, htblColNameValue);
        for (int i = 0; i < this.tables.size(); i++) {
            if (this.tables.get(i).name.equals(strTableName)) {
                try {
                    this.tables.get(i).delete(htblColNameValue);
                } catch (Exception e) {
                    throw new DBAppException(e.getMessage());
                }

            }
        }
    }

    public void createIndex(String strTableName,
                            String[] strarrColName) throws DBAppException{
        if(strarrColName.length != 3){
            throw new DBAppException("columns number should be 3 to make an index");
        }
        try {
            boolean tableExists = Table.tableExistsOnDisk("src/resources/metadata.csv", strTableName);
            if (!tableExists) {
                throw new DBAppException("table does not exist");
            }
        } catch (Exception e) {
            throw new DBAppException(e.getMessage());
        }

        for (int i = 0; i < this.tables.size(); i++) {
            if (this.tables.get(i).name.equals(strTableName)) {
                try {
                    this.tables.get(i).addIndex(strarrColName);
                } catch (Exception e) {
                    throw new DBAppException(e.getMessage());
                }
            }
        }



    }
}
