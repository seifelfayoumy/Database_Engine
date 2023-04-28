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
    boolean isEmpty;
    String csvAddress;

    public Table(String name, String clusteringKey) throws Exception {

        Properties prop = new Properties();
        FileInputStream fis = new FileInputStream("src/resources/DBApp.config");
        prop.load(fis);
        this.maxRows = Integer.parseInt(prop.getProperty("MaximumRowsCountinTablePage"));

        this.name = name;
        this.pages = new ArrayList<PageInfo>();
        this.clusteringKey = clusteringKey;
        this.isEmpty = true;
        this.csvAddress = "src/resources/metadata.csv";

        this.save();
    }

    public void insert(Hashtable<String, Object> tuple) throws Exception {
        if (this.pages.size() == 0) {
            String pageAddress = "src/resources/" + this.name + "_1.ser";
            PageInfo newPage = new PageInfo(this.maxRows, pageAddress);
            this.pages.add(newPage);
            Page.createPage(pageAddress);
            this.pages.set(0, Page.insertToFirstPage(this.pages.get(0), tuple, this.clusteringKey));
        } else {
            int pagesSize = this.pages.size();
            for (int i = 0; i < pagesSize; i++) {
                if (i == pagesSize - 1) {
                    if (this.pages.get(i).isFull) {
                        if (Table.compareClusterKey(this.csvAddress, this.name, tuple.get(this.clusteringKey), this.pages.get(i).maxValue) > 0) {
                            String pageAddress = "src/resources/" + this.name + "_" + (this.pages.size() + 1) + ".ser";
                            PageInfo newPage = new PageInfo(this.maxRows, pageAddress);
                            this.pages.add(newPage);
                            Page.createPage(pageAddress);
                            this.pages.set(i + 1, Page.insertToFirstPage(this.pages.get(i + 1), tuple, this.clusteringKey));
                            break;
                        } else {
                            Hashtable<String, Object> lastTuple = Page.getLastTuple(this.pages.get(i));
                            this.pages.set(i, Page.removeLastTuple(this.pages.get(i), this.clusteringKey));
                            this.pages.set(i, Page.insertToPage(this.pages.get(i), tuple, this.clusteringKey, this.name, this.csvAddress));
                            String pageAddress = "src/resources/" + this.name + "_" + (this.pages.size() + 1) + ".ser";
                            PageInfo newPage = new PageInfo(this.maxRows, pageAddress);
                            this.pages.add(newPage);
                            Page.createPage(pageAddress);
                            this.pages.set(i + 1, Page.insertToFirstPage(this.pages.get(i + 1), lastTuple, this.clusteringKey));
                            break;
                        }

                    } else {
                        this.pages.set(i, Page.insertToPage(this.pages.get(i), tuple, this.clusteringKey, this.name, this.csvAddress));
                        break;
                    }

                } else {
                    if (Table.compareClusterKey(this.csvAddress, this.name, tuple.get(this.clusteringKey), this.pages.get(i + 1).minValue) < 0) {
                        if (this.pages.get(i).isFull) {
                            if (Table.compareClusterKey(this.csvAddress, this.name, tuple.get(this.clusteringKey), this.pages.get(i).maxValue) > 0) {
                                if (this.pages.get(i + 1).isFull) {
                                    Table.shiftDown(this, i + 1);
                                }
                                this.pages.set(i + 1, Page.insertToPage(this.pages.get(i + 1), tuple, this.clusteringKey, this.name, this.csvAddress));
                                break;
                            } else if (Table.compareClusterKey(this.csvAddress, this.name, tuple.get(this.clusteringKey), this.pages.get(i).maxValue) < 0) {
                                Table.shiftDown(this, i);
                                this.pages.set(i, Page.insertToPage(this.pages.get(i), tuple, this.clusteringKey, this.name, this.csvAddress));
                                break;
                            } else {
                                throw new DuplicateRowException("Duplicate clustering key");
                            }
                        } else {
                            this.pages.set(i, Page.insertToPage(this.pages.get(i), tuple, this.clusteringKey, this.name, this.csvAddress));
                            break;
                        }
                    }
                }
            }
        }
        this.isEmpty = false;
        this.save();
    }

    public void update(String clusteringKeyValue, Hashtable<String, Object> tuple) throws Exception {
        for (int i = 0; i < this.pages.size(); i++) {
            if (i != this.pages.size() - 1) {
                if (Table.compareClusterKey(this.csvAddress, this.name, tuple.get(this.clusteringKey), this.pages.get(i).minValue) >= 0
                        && Table.compareClusterKey(this.csvAddress, this.name, tuple.get(this.clusteringKey), this.pages.get(i + 1).minValue) < 0) {
                    Page.updateFromPage(this.pages.get(i), tuple, this.clusteringKey, clusteringKeyValue);
                }
            } else {
                if (Table.compareClusterKey(this.csvAddress, this.name, tuple.get(this.clusteringKey), this.pages.get(i).minValue) >= 0) {
                    Page.updateFromPage(this.pages.get(i), tuple, this.clusteringKey, clusteringKeyValue);
                }
            }

        }
        this.save();
    }

    public void delete(Hashtable<String, Object> tuple) throws Exception {
        for (int i = 0; i < this.pages.size(); i++) {
            PageInfo deletedFrom = Page.deleteFromPage(this.pages.get(i), tuple, this.clusteringKey);
            if (deletedFrom.noOfTuples == 0) {
                this.pages.remove(i);
                Page.deletePage(deletedFrom.address);
                this.renamePageFiles();
            } else {
                this.pages.set(i, deletedFrom);
            }

        }
        this.save();
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
                        || columnValue instanceof Date && nextRecord[2].equals("java.util.Date")) {
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
                switch (nextRecord[2]) {
                    case ("java.lang.Integer"):
                        if (((Integer) columnValue).compareTo(Integer.parseInt(nextRecord[6])) >= 0
                                && ((Integer) columnValue).compareTo(Integer.parseInt(nextRecord[7])) <= 0) {
                            correct = true;
                        }
                        break;
                    case ("java.lang.Double"):
                        if (((Double) columnValue).compareTo(Double.parseDouble(nextRecord[6])) >= 0
                                && ((Double) columnValue).compareTo(Double.parseDouble(nextRecord[7])) <= 0) {
                            correct = true;
                        }
                        break;
                    case ("java.util.Date"):
                        if (((Date) columnValue).compareTo(new Date(nextRecord[6])) >= 0
                                && ((Date) columnValue).compareTo(new Date(nextRecord[7])) <= 0) {
                            correct = true;
                        }
                        break;
                    case ("java.lang.String"):
                        if (((String) columnValue).compareTo(nextRecord[6]) >= 0
                                && ((String) columnValue).compareTo(nextRecord[7]) <= 0) {
                            correct = true;
                        }
                        break;
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
        try{
            Table.setTupleNullValues(csvAddress,tableName,tuple);
        }catch (Exception e){
            throw new DBAppException(e.getMessage());
        }

    }

    public static void setTupleNullValues(String csvAddress, String tableName, Hashtable<String, Object> tuple) throws DBAppException, CsvValidationException, IOException {

        ArrayList<String> tableColumns = Table.getAllColumnsForTableFromCSV(csvAddress, tableName);
        Enumeration<String> keys = tuple.keys();
        while (keys.hasMoreElements()) {
            String currColumnName = keys.nextElement();
            tableColumns.remove(currColumnName);
        }
        for (int i = 0; i < tableColumns.size(); i++) {
            tuple.put(tableColumns.get(i), new NullObject());
        }
    }

    public static void checkTupleClusterKeyExists(Table table, Hashtable<String, Object> tuple) throws DBAppException {

        String clusterKey = table.clusteringKey;
        Boolean exist = false;
        Enumeration<String> keys = tuple.keys();
        while (keys.hasMoreElements()) {
            String currColumnName = keys.nextElement();
            if (currColumnName.equals(clusterKey)) {
                exist = true;
            }
        }
        if (!exist) {
            throw new DBAppException("Cluster key column not provided");
        }
    }

    public static void checkTupleColumnsInTable(String csvAddress, String tableName, Hashtable<String, Object> tuple) throws DBAppException, CsvValidationException, IOException {

        ArrayList<String> tableColumns = Table.getAllColumnsForTableFromCSV(csvAddress, tableName);
        Boolean exist = true;
        Enumeration<String> keys = tuple.keys();
        while (keys.hasMoreElements()) {
            String currColumnName = keys.nextElement();
            if (!tableColumns.contains(currColumnName)) {
                exist = false;
            }
        }
        if (!exist) {
            throw new DBAppException("not all column names are in table");
        }
    }

    public static boolean validateTypes(Hashtable<String, String> nameType) {
        boolean correct = false;
        Enumeration<String> keys = nameType.keys();
        while (keys.hasMoreElements()) {
            String currColumnName = keys.nextElement();
            String currColumnValue = nameType.get(currColumnName);

            if (currColumnValue.equals("java.lang.Integer")
                    || currColumnValue.equals("java.lang.Double")
                    || currColumnValue.equals("java.lang.String")
                    || currColumnValue.equals("java.util.Date")) {
                correct = true;
            }

        }
        return correct;
    }

    public static String getClusteringKeyTypeFromCSV(String csvAddress, String tableName) throws CsvValidationException, IOException {

        String result = null;
        FileReader filereader = new FileReader(csvAddress);
        CSVReader csvReader = new CSVReader(filereader);
        String[] nextRecord;
        while ((nextRecord = csvReader.readNext()) != null) {
            if (nextRecord[0].equals(tableName) && nextRecord[3].equals("True")) {
                result = nextRecord[2];
            }
        }
        return result;

    }

    public static ArrayList<String> getAllColumnsForTableFromCSV(String csvAddress, String tableName) throws CsvValidationException, IOException {

        ArrayList<String> result = new ArrayList<String>();
        FileReader filereader = new FileReader(csvAddress);
        CSVReader csvReader = new CSVReader(filereader);
        String[] nextRecord;
        while ((nextRecord = csvReader.readNext()) != null) {
            if (nextRecord[0].equals(tableName)) {
                result.add(nextRecord[2]);
            }
        }
        return result;

    }

    public static int compareClusterKey(String csvAddress, String tableName, Object o1, Object o2) throws CsvValidationException, IOException {
        String clusteringKeyType = Table.getClusteringKeyTypeFromCSV(csvAddress, tableName);
        switch (clusteringKeyType) {
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

    public static void printAllPagesClusterKey(String tableName, String clusterKey) throws IOException, ClassNotFoundException {
        FileInputStream fileIn = new FileInputStream("src/resources/" + tableName + "_table.ser");
        ObjectInputStream in = new ObjectInputStream(fileIn);
        Table table = (Table) in.readObject();
        in.close();
        fileIn.close();

        for (int i = 0; i < table.pages.size(); i++) {
            Vector<Hashtable<String, Object>> page = Page.readPage(table.pages.get(i).address);
            System.out.print("page " + (i + 1) + " : ");
            for (int j = 0; j < page.size(); j++) {
                System.out.print(page.get(j).get(clusterKey) + "  ");
            }
            System.out.println();
        }
    }

    public static void printAllPages(String tableName) throws IOException, ClassNotFoundException {
        FileInputStream fileIn = new FileInputStream("src/resources/" + tableName + "_table.ser");
        ObjectInputStream in = new ObjectInputStream(fileIn);
        Table table = (Table) in.readObject();
        in.close();
        fileIn.close();

        for (int i = 0; i < table.pages.size(); i++) {
            Vector<Hashtable<String, Object>> page = Page.readPage(table.pages.get(i).address);
            System.out.println("page " + (i + 1) + " : ");
            for (int j = 0; j < page.size(); j++) {
                Enumeration<String> keys = page.get(j).keys();
                while (keys.hasMoreElements()) {
                    String currColumnName = keys.nextElement();
                    System.out.print("  " + currColumnName + ": ");
                    System.out.print(page.get(j).get(currColumnName));
                }
                System.out.println();
            }
            System.out.println();
        }
    }

    public void renamePageFiles() {
        for (int i = 0; i < this.pages.size(); i++) {
            String newAddress = "src/resources/" + this.name + "_" + (i + 1) + ".ser";
            this.pages.set(i, Page.renameFile(this.pages.get(i), newAddress));
        }
    }

    public static void shiftDown(Table table, int i) throws Exception {
        if (i == table.pages.size() - 1) {
            if (table.pages.get(i).isFull) {
                Hashtable<String, Object> lastTuple = Page.getLastTuple(table.pages.get(i));
                table.pages.set(i, Page.removeLastTuple(table.pages.get(i), table.clusteringKey));
                String pageAddress = "src/resources/" + table.name + "_" + (table.pages.size() + 1) + ".ser";
                PageInfo newPage = new PageInfo(table.maxRows, pageAddress);
                table.pages.add(newPage);
                Page.createPage(pageAddress);
                table.pages.set(i + 1, Page.insertToFirstPage(table.pages.get(i + 1), lastTuple, table.clusteringKey));
            }
        } else {
            if (table.pages.get(i).isFull) {
                Hashtable<String, Object> lastTuple = Page.getLastTuple(table.pages.get(i));
                table.pages.set(i, Page.removeLastTuple(table.pages.get(i), table.clusteringKey));
                Table.shiftDownHelper(table, i + 1, lastTuple);
            }
        }
        table.save();
    }

    private static void shiftDownHelper(Table table, int i, Hashtable<String, Object> tuple) throws Exception {
        if (i == table.pages.size() - 1) {
            if (table.pages.get(i).isFull) {
                Hashtable<String, Object> lastTuple = Page.getLastTuple(table.pages.get(i));
                table.pages.set(i, Page.removeLastTuple(table.pages.get(i), table.clusteringKey));
                table.pages.set(i, Page.insertToPage(table.pages.get(i), tuple, table.clusteringKey, table.name, table.csvAddress));
                String pageAddress = "src/resources/" + table.name + "_" + (table.pages.size() + 1) + ".ser";
                PageInfo newPage = new PageInfo(table.maxRows, pageAddress);
                table.pages.add(newPage);
                Page.createPage(pageAddress);
                table.pages.set(i + 1, Page.insertToFirstPage(table.pages.get(i + 1), lastTuple, table.clusteringKey));
            } else {
                table.pages.set(i, Page.insertToPage(table.pages.get(i), tuple, table.clusteringKey, table.name, table.csvAddress));
            }
        } else {
            if (table.pages.get(i).isFull) {
                Hashtable<String, Object> lastTuple = Page.getLastTuple(table.pages.get(i));
                table.pages.set(i, Page.removeLastTuple(table.pages.get(i), table.clusteringKey));
                table.pages.set(i, Page.insertToPage(table.pages.get(i), tuple, table.clusteringKey, table.name, table.csvAddress));
                Table.shiftDownHelper(table, i + 1, lastTuple);
            } else {
                table.pages.set(i, Page.insertToPage(table.pages.get(i), tuple, table.clusteringKey, table.name, table.csvAddress));
            }
        }
        table.save();
    }


}
