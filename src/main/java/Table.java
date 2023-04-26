import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvValidationException;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Table implements Serializable {

    String name;
    ArrayList<PageInfo> pages;
    String clusteringKey;
    int maxRows;

    public Table(String name, String clusteringKey) throws Exception {

        Properties prop = new Properties();
        FileInputStream fis = new FileInputStream("src/resources/DBApp.config");
        prop.load(fis);
        this.maxRows = Integer.parseInt(prop.getProperty("MaximumRowsCountinTablePage"));

        this.name = name;
        this.pages = new ArrayList<PageInfo>();
        this.clusteringKey = clusteringKey;

        this.save();
    }

    public void insert(Hashtable<String, Object> tuple) throws MaxRowsException, IOException, ClassNotFoundException, DuplicateRowException {
        if (this.pages.size() == 0) {
            String pageAddress = "src/resources/" + this.name + "_1.ser";
            PageInfo newPage = new PageInfo(this.maxRows, pageAddress);
            this.pages.add(newPage);
            Page.createPage(pageAddress);
            this.pages.add(0, Page.insertToFirstPage(this.pages.get(0), tuple, this.clusteringKey));
        } else {
            for (int i = 0; i < this.pages.size(); i++) {
                if (i != this.pages.size() - 1) {
                    if (tuple.get(this.clusteringKey).toString().compareTo(this.pages.get(i).minValue.toString()) >= 0
                            && tuple.get(this.clusteringKey).toString().compareTo(this.pages.get(i + 1).minValue.toString()) < 0) {
                        if (this.pages.get(i).isFull) {
                            Hashtable<String, Object> lastTuple = Page.getLastTuple(this.pages.get(i));
                            this.pages.set(i, Page.removeLastTuple(this.pages.get(i), this.clusteringKey));
                            this.pages.set(i, Page.insertToPage(this.pages.get(i), tuple, this.clusteringKey));
                            this.insert(lastTuple);
                        } else {
                            this.pages.set(i, Page.insertToPage(this.pages.get(i), tuple, this.clusteringKey));
                        }
                    }
                } else {
                    if (tuple.get(this.clusteringKey).toString().compareTo(this.pages.get(i).minValue.toString()) >= 0
                            && tuple.get(this.clusteringKey).toString().compareTo(this.pages.get(i + 1).minValue.toString()) < 0) {
                        if (this.pages.get(i).isFull) {
                            Hashtable<String, Object> lastTuple = Page.getLastTuple(this.pages.get(i));
                            this.pages.set(i, Page.removeLastTuple(this.pages.get(i), this.clusteringKey));
                            this.pages.set(i, Page.insertToPage(this.pages.get(i), tuple, this.clusteringKey));
                            String pageAddress = "src/resources/" + this.name + "_" + this.pages.size() + 1 + ".ser";
                            PageInfo newPage = new PageInfo(this.maxRows, pageAddress);
                            this.pages.add(newPage);
                            Page.createPage(pageAddress);
                            Page.insertToPage(newPage, lastTuple, this.clusteringKey);
                        } else {
                            this.pages.set(i, Page.insertToPage(this.pages.get(i), tuple, this.clusteringKey));
                        }
                    }
                }
            }
        }


    }


    public void save() throws Exception {

        FileOutputStream fileOut = new FileOutputStream("src/resources/" + this.name + "_table.ser");
        ObjectOutputStream out = new ObjectOutputStream(fileOut);
        out.writeObject(this);
        out.close();
        fileOut.close();

    }

    public static void writeTableToCSV(String address, String strTableName, String strClusteringKeyColumn,
                                       Hashtable<String, String> tuple, Hashtable<String, String> htblColNameMin,
                                       Hashtable<String, String> htblColNameMax) throws IOException {
        Enumeration<String> keys = tuple.keys();
        while (keys.hasMoreElements()) {
            String currColumnName = keys.nextElement();
            CSVWriter writer = new CSVWriter(new FileWriter(address, true));
            String clusteringKey = "False";
            if (strClusteringKeyColumn.equals(currColumnName)) {
                clusteringKey = "True";
            }
            String[] record = new String[]{strTableName, currColumnName, tuple.get(currColumnName), clusteringKey, "null", "null", htblColNameMin.get(currColumnName), htblColNameMax.get(currColumnName)};
            writer.writeNext(record, false);
            writer.close();
        }
    }

    public static ArrayList<Table> getTablesFromDisk(String directoryAddress) throws IOException, ClassNotFoundException {
        ArrayList<Table> tables = new ArrayList<Table>();
        List<Path> files;
        Stream<Path> walk = Files.walk(Paths.get(directoryAddress));
        files = walk.filter(Files::isRegularFile)
                .collect(Collectors.toList());

        for (int i = 0; i < files.size(); i++) {
            if (files.get(i).toString().endsWith("_table.ser")) {
                FileInputStream fileIn = new FileInputStream(files.get(i).toFile());
                ObjectInputStream in = new ObjectInputStream(fileIn);
                Table table = (Table) in.readObject();
                tables.add(table);
                in.close();
                fileIn.close();
            }
        }

        return tables;
    }

    public static boolean tableExistsOnDisk(String csvAddress, String tableName) throws IOException, CsvValidationException {

        boolean exists = false;
        FileReader filereader = new FileReader(csvAddress);
        CSVReader csvReader = new CSVReader(filereader);
        String[] nextRecord;
        while ((nextRecord = csvReader.readNext()) != null) {
            if (nextRecord[0].equals(tableName)) {
                exists = true;
            }
        }
        return exists;
    }

    public static boolean checkTypeForColumnValue(String csvAddress, String tableName, String columnName, Object columnValue) throws IOException, CsvValidationException {
        boolean correct = false;
        FileReader filereader = new FileReader(csvAddress);
        CSVReader csvReader = new CSVReader(filereader);
        String[] nextRecord;
        while ((nextRecord = csvReader.readNext()) != null) {
            if (nextRecord[0].equals(tableName) && nextRecord[1].equals(columnName)) {
                if (columnValue instanceof String && nextRecord[2].equals("java.lang.String")
                        || columnValue instanceof Double && nextRecord[2].equals("java.lang.Double")
                        || columnValue instanceof Integer && nextRecord[2].equals("java.lang.Integer")
                        || columnValue instanceof Date && nextRecord[2].equals("java.lang.Date")) {
                    correct = true;
                }
            }
        }
        return correct;
    }

    public static boolean checkColumnRange(String csvAddress, String tableName, String columnName, Object columnValue) throws IOException, CsvValidationException {
        boolean correct = false;
        FileReader filereader = new FileReader(csvAddress);
        CSVReader csvReader = new CSVReader(filereader);
        String[] nextRecord;
        while ((nextRecord = csvReader.readNext()) != null) {
            if (nextRecord[0].equals(tableName) && nextRecord[1].equals(columnName)) {
                if (columnValue.toString().compareTo(nextRecord[6].toString()) >= 0
                        && columnValue.toString().compareTo(nextRecord[7].toString()) <= 0) {
                    correct = true;
                }
            }
        }
        return correct;
    }

    public static void validateTableColumn(String csvAddress, String tableName, String columnName, Object columnValue) throws DBAppException {
        try {
            boolean correctColumnType = Table.checkTypeForColumnValue(csvAddress, tableName, columnName, columnValue);
            if (!correctColumnType) {
                throw new DBAppException("one of the column types does not match the correct type");
            }
            boolean correctColumnRange = Table.checkColumnRange(csvAddress, tableName, columnName, columnValue);
            if (!correctColumnRange) {
                throw new DBAppException("one of the column values does not fit in the correct range");
            }
        } catch (Exception e) {
            throw new DBAppException(e.getMessage());
        }
    }

    public static void validateTuple(String csvAddress, String tableName, Hashtable<String, Object> tuple) throws DBAppException {
        try {
            boolean tableExists = Table.tableExistsOnDisk(csvAddress, tableName);
            if (!tableExists) {
                throw new DBAppException("table does not exist");
            }
        } catch (Exception e) {
            throw new DBAppException(e.getMessage());
        }

        Enumeration<String> keys = tuple.keys();
        while (keys.hasMoreElements()) {
            String currColumnName = keys.nextElement();
            Object currColumnValue = tuple.get(currColumnName);
            Table.validateTableColumn(csvAddress, tableName, currColumnName, currColumnValue);
        }
    }


}
