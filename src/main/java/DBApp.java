import com.opencsv.CSVWriter;

import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DBApp {
    public ArrayList<Table> tables;

    public DBApp() throws DBAppException {
        this.tables = new ArrayList<Table>();
        this.init();
    }


    public void init() throws DBAppException {
        try {
            List<Path> files;
            try (Stream<Path> walk = Files.walk(Paths.get("src/resources"))) {
                files = walk.filter(Files::isRegularFile)
                        .collect(Collectors.toList());
            }
            for (int i = 0; i < files.size(); i++) {
                if(files.get(i).toString().endsWith("_table.ser")){
                    FileInputStream fileIn = new FileInputStream(files.get(i).toFile());
                    ObjectInputStream in = new ObjectInputStream(fileIn);
                    Table table = (Table) in.readObject();
                    this.tables.add(table);
                    in.close();
                    fileIn.close();
                }
            }
        } catch (Exception e) {
            throw new DBAppException(e.getMessage());
        }

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
            String[] record = new String[]{strTableName, currColumnName, htblColNameType.get(currColumnName), clusteringKey, "null", "null", htblColNameMin.get(currColumnName), htblColNameMax.get(currColumnName)};
            writer.writeNext(record, false);
            try {
                writer.close();
            } catch (IOException e) {
                throw new DBAppException(e.getMessage());
            }
        }

        try {
            Table table = new Table(strTableName,strClusteringKeyColumn);
            this.tables.add(table);
        } catch (Exception e) {
            throw new DBAppException(e.getMessage());
        }



    }

    public void insertIntoTable(String strTableName,
                                Hashtable<String, Object> htblColNameValue) throws DBAppException {

        for(int i=0;i<this.tables.size();i++){
            if(this.tables.get(i).name.equals(strTableName)){
                try {

                    this.tables.get(i).insert(htblColNameValue);
                } catch (MaxRowsException e) {
                    throw new RuntimeException(e);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                } catch (ClassNotFoundException e) {
                    throw new RuntimeException(e);
                } catch (DuplicateRowException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    public void updateTable(String strTableName,
                            String strClusteringKeyValue, Hashtable<String, Object> htblColNameValue)
            throws DBAppException {
    }

    public void deleteFromTable(String strTableName,
                                Hashtable<String, Object> htblColNameValue) throws DBAppException {
    }
}
