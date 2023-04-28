import java.util.ArrayList;
import java.util.Hashtable;

public class DBApp {
    private ArrayList<Table> tables;

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

        Table.validateTuple("src/resources/metadata.csv", strTableName, htblColNameValue);
        for (int i = 0; i < this.tables.size(); i++) {
            if (this.tables.get(i).name.equals(strTableName)) {
                try {
                    this.tables.get(i).insert(htblColNameValue);
                } catch (Exception e) {
                    throw new DBAppException(e.getMessage());
                }
            }
        }
    }

    public void updateTable(String strTableName,
                            String strClusteringKeyValue, Hashtable<String, Object> htblColNameValue)
            throws DBAppException {

        Table.validateTuple("src/resources/metadata.csv", strTableName, htblColNameValue);
        for (int i = 0; i < this.tables.size(); i++) {
            if (this.tables.get(i).name.equals(strTableName)) {
                try {
                    this.tables.get(i).update(strClusteringKeyValue, htblColNameValue);
                } catch (Exception e) {
                    throw new DBAppException(e.getMessage());
                }
            }
        }
    }

    public void deleteFromTable(String strTableName,
                                Hashtable<String, Object> htblColNameValue) throws DBAppException {
        Table.validateTuple("src/resources/metadata.csv", strTableName, htblColNameValue);
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
}
