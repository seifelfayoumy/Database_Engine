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
    ArrayList<OctreeReference> indexes;
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
        this.indexes = new ArrayList<OctreeReference>();
        this.clusteringKey = clusteringKey;
        this.isEmpty = true;
        this.csvAddress = "src/resources/metadata.csv";

        this.save();
    }

    public void addIndex(String[] columns, int index) throws Exception {
        Properties prop = new Properties();
        FileInputStream fis = new FileInputStream("src/resources/DBApp.config");
        prop.load(fis);
        this.addIndexToCsv(columns, columns[0] + columns[1] + columns[2] + "Index");

        Octree tree = new Octree(Table.getMinForColumn(this.csvAddress, this.name, columns[0]),
                Table.getMaxForColumn(this.csvAddress, this.name, columns[0]),
                Table.getTypeForColumn(this.csvAddress, this.name, columns[0]),
                Table.getMinForColumn(this.csvAddress, this.name, columns[1]),
                Table.getMaxForColumn(this.csvAddress, this.name, columns[1]),
                Table.getTypeForColumn(this.csvAddress, this.name, columns[1]),
                Table.getMinForColumn(this.csvAddress, this.name, columns[2]),
                Table.getMaxForColumn(this.csvAddress, this.name, columns[2]),
                Table.getTypeForColumn(this.csvAddress, this.name, columns[2]),
                this.name,
                columns,
                Integer.parseInt(prop.getProperty("MaximumEntriesinOctreeNode")),
                columns[0] + columns[1] + columns[2] + "Index"
        );
        String address = "src/resources/" + this.name + "_" + (index + 1) + "_index.ser";
        this.indexes.add(new OctreeReference(columns, address, columns[0] + columns[1] + columns[2] + "Index"));

        for (PageInfo pageInfo : this.pages) {
            // Load page from disk
            Vector<Hashtable<String, Object>> pageContent = Page.readPage(pageInfo.address);

            for (Hashtable<String, Object> tuple : pageContent) {
                // Create an IndexReference
                Object x = tuple.get(columns[0]);
                Object y = tuple.get(columns[1]);
                Object z = tuple.get(columns[2]);
                IndexReference indexReference = new IndexReference(x, y, z, pageInfo.address);

                // Insert it into the Octree
                tree.insert(indexReference);
            }
        }

        tree.save(address);
        this.save();
    }

    public void addIndex(String[] columns) throws Exception {
        Properties prop = new Properties();
        FileInputStream fis = new FileInputStream("src/resources/DBApp.config");
        prop.load(fis);
        this.addIndexToCsv(columns, columns[0] + columns[1] + columns[2] + "Index");

        Octree tree = new Octree(Table.getMinForColumn(this.csvAddress, this.name, columns[0]),
                Table.getMaxForColumn(this.csvAddress, this.name, columns[0]),
                Table.getTypeForColumn(this.csvAddress, this.name, columns[0]),
                Table.getMinForColumn(this.csvAddress, this.name, columns[1]),
                Table.getMaxForColumn(this.csvAddress, this.name, columns[1]),
                Table.getTypeForColumn(this.csvAddress, this.name, columns[1]),
                Table.getMinForColumn(this.csvAddress, this.name, columns[2]),
                Table.getMaxForColumn(this.csvAddress, this.name, columns[2]),
                Table.getTypeForColumn(this.csvAddress, this.name, columns[2]),
                this.name,
                columns,
                Integer.parseInt(prop.getProperty("MaximumEntriesinOctreeNode")),
                columns[0] + columns[1] + columns[2] + "Index"
        );
        String address = "src/resources/" + this.name + "_" + (this.indexes.size() + 1) + "_index.ser";
        this.indexes.add(new OctreeReference(columns, address, columns[0] + columns[1] + columns[2] + "Index"));

        for (PageInfo pageInfo : this.pages) {
            // Load page from disk
            Vector<Hashtable<String, Object>> pageContent = Page.readPage(pageInfo.address);

            for (Hashtable<String, Object> tuple : pageContent) {
                // Create an IndexReference
                Object x = tuple.get(columns[0]);
                Object y = tuple.get(columns[1]);
                Object z = tuple.get(columns[2]);
                IndexReference indexReference = new IndexReference(x, y, z, pageInfo.address);

                // Insert it into the Octree
                tree.insert(indexReference);
            }
        }

        tree.save(address);
        this.save();
    }

    public void insert(Hashtable<String, Object> tuple) throws Exception {
        String address = null;
        Boolean reset = false;
        if (this.pages.size() == 0) {
            String pageAddress = "src/resources/" + this.name + "_1.ser";
            address = pageAddress;
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
                            address = pageAddress;
                            PageInfo newPage = new PageInfo(this.maxRows, pageAddress);
                            this.pages.add(newPage);
                            Page.createPage(pageAddress);
                            this.pages.set(i + 1, Page.insertToFirstPage(this.pages.get(i + 1), tuple, this.clusteringKey));
                            reset = true;
                            break;
                        } else {
                            Hashtable<String, Object> lastTuple = Page.getLastTuple(this.pages.get(i));
                            this.pages.set(i, Page.removeLastTuple(this.pages.get(i), this.clusteringKey));
                            this.pages.set(i, Page.insertToPage(this.pages.get(i), tuple, this.clusteringKey, this.name, this.csvAddress));
                            address = this.pages.get(i).address;
                            String pageAddress = "src/resources/" + this.name + "_" + (this.pages.size() + 1) + ".ser";
                            PageInfo newPage = new PageInfo(this.maxRows, pageAddress);
                            this.pages.add(newPage);
                            Page.createPage(pageAddress);
                            this.pages.set(i + 1, Page.insertToFirstPage(this.pages.get(i + 1), lastTuple, this.clusteringKey));
                            reset = true;
                            break;
                        }

                    } else {
                        this.pages.set(i, Page.insertToPage(this.pages.get(i), tuple, this.clusteringKey, this.name, this.csvAddress));
                        address = this.pages.get(i).address;
                        break;
                    }

                } else {
                    if (Table.compareClusterKey(this.csvAddress, this.name, tuple.get(this.clusteringKey), this.pages.get(i + 1).minValue) < 0) {
                        if (this.pages.get(i).isFull) {
                            if (Table.compareClusterKey(this.csvAddress, this.name, tuple.get(this.clusteringKey), this.pages.get(i).maxValue) > 0) {
                                if (this.pages.get(i + 1).isFull) {
                                    Table.shiftDown(this, i + 1);
                                    reset = true;
                                }
                                this.pages.set(i + 1, Page.insertToPage(this.pages.get(i + 1), tuple, this.clusteringKey, this.name, this.csvAddress));
                                address = this.pages.get(i + 1).address;
                                break;
                            } else if (Table.compareClusterKey(this.csvAddress, this.name, tuple.get(this.clusteringKey), this.pages.get(i).maxValue) < 0) {
                                Table.shiftDown(this, i);
                                reset = true;
                                this.pages.set(i, Page.insertToPage(this.pages.get(i), tuple, this.clusteringKey, this.name, this.csvAddress));
                                address = this.pages.get(i).address;
                                break;
                            } else {
                                throw new DuplicateRowException("Duplicate clustering key");
                            }
                        } else {
                            this.pages.set(i, Page.insertToPage(this.pages.get(i), tuple, this.clusteringKey, this.name, this.csvAddress));
                            address = this.pages.get(i).address;
                            break;
                        }
                    }
                }
            }
        }


        if (reset) {
            for (int i = 0; i < this.indexes.size(); i++) {
                if (Arrays.stream(this.indexes.get(i).columns).toList().containsAll(tuple.keySet())) {
                    Octree octree = Octree.read(this.indexes.get(i).address);
                    String[] columns = octree.columns;

                    Page.deletePage(this.indexes.get(i).address);
                    this.indexes.remove(i);
                    this.addIndex(columns, i);
                    break;
                }

            }
        } else {
            for (int i = 0; i < this.indexes.size(); i++) {
                if (Arrays.stream(this.indexes.get(i).columns).toList().containsAll(tuple.keySet())) {
                    Octree octree = Octree.read(this.indexes.get(i).address);
                    // Create an IndexReference
                    Object x = tuple.get(octree.columns[0]);
                    Object y = tuple.get(octree.columns[1]);
                    Object z = tuple.get(octree.columns[2]);
                    IndexReference indexReference = new IndexReference(x, y, z, address);
                    // Insert it to the Octree

                    octree.insert(indexReference);

                    octree.save(this.indexes.get(i).address);
                }

            }
        }

        this.isEmpty = false;
        this.save();
    }

    public void update(String tableName, String clusteringKeyValue, Hashtable<String, Object> tuple) throws Exception {
        String clusteringKeyType = Table.getClusteringKeyTypeFromCSV(csvAddress, tableName);
        Object clusterKeyValueObject = null;
        switch (clusteringKeyType) {
            case "java.lang.String":
                clusterKeyValueObject = (String) clusteringKeyValue;
                break;
            case "java.lang.Integer":
                clusterKeyValueObject = Integer.parseInt(clusteringKeyValue);
                break;
            case "java.lang.Double":
                clusterKeyValueObject = Double.parseDouble(clusteringKeyValue);
                break;
            case "java.util.Date":
                clusterKeyValueObject = new Date(clusteringKeyValue);
                break;
            default:
                clusterKeyValueObject = new NullObject();
        }
        Boolean hasIndex = false;
        for (int i = 0; i < this.indexes.size(); i++) {
            if (Arrays.stream(this.indexes.get(i).columns).toList().containsAll(tuple.keySet())) {
                Octree octree = Octree.read(this.indexes.get(i).address);

                Object x = tuple.get(octree.columns[0]);
                Object y = tuple.get(octree.columns[1]);
                Object z = tuple.get(octree.columns[2]);


                IndexReference ref = octree.search(x, y, z);

                String address = ref.pageAddress;
                for (int j = 0; j < this.pages.size(); j++) {
                    if (this.pages.get(j).address.equals(address)) {
                        Page.updateFromPage(this.csvAddress, this.name, this.pages.get(i), tuple, this.clusteringKey, clusterKeyValueObject);
                        octree.delete(x, y, z);

                        hasIndex = true;
                    }
                }
            }

        }
        if (!hasIndex) {
            for (int i = 0; i < this.pages.size(); i++) {
                if (i != this.pages.size() - 1) {
                    if (Table.compareClusterKey(this.csvAddress, this.name, clusterKeyValueObject, this.pages.get(i).minValue) >= 0
                            && Table.compareClusterKey(this.csvAddress, this.name, clusterKeyValueObject, this.pages.get(i + 1).minValue) < 0) {
                        Page.updateFromPage(this.csvAddress, this.name, this.pages.get(i), tuple, this.clusteringKey, clusterKeyValueObject);
                    }
                } else {
                    if (Table.compareClusterKey(this.csvAddress, this.name, clusterKeyValueObject, this.pages.get(i).minValue) >= 0) {
                        Page.updateFromPage(this.csvAddress, this.name, this.pages.get(i), tuple, this.clusteringKey, clusterKeyValueObject);
                    }
                }
            }
        }
        this.save();

    }

    public void delete(Hashtable<String, Object> tuple) throws Exception {
        ArrayList<PageInfo> deletedPages = new ArrayList<PageInfo>();
        ArrayList<IndexReference> deletedTuples = new ArrayList<>();
        String[] columns;
        Boolean hasIndex = false;
        for (int i = 0; i < this.indexes.size(); i++) {
            if (Arrays.stream(this.indexes.get(i).columns).toList().containsAll(tuple.keySet())) {
                Octree octree = Octree.read(this.indexes.get(i).address);
                columns = octree.columns;

                Object x = tuple.get(octree.columns[0]);
                Object y = tuple.get(octree.columns[1]);
                Object z = tuple.get(octree.columns[2]);
                IndexReference ref = octree.search(x, y, z);

                if (ref != null) {


                    String address = ref.pageAddress;
                    for (int j = 0; j < this.pages.size(); j++) {
                        if (this.pages.get(j).address.equals(address)) {
                            PageInfo deletedFrom = Page.deleteFromPage(this.csvAddress, this.name, this.pages.get(i), tuple, this.clusteringKey);
                            if (deletedFrom.noOfTuples == 0) {
                                Page.deletePage(deletedFrom.address);
                                deletedPages.add(deletedFrom);
                            } else {
                                this.pages.set(i, deletedFrom);
                            }
                            octree.delete(x, y, z);
                            octree.save(this.indexes.get(i).address);
                            hasIndex = true;
                        }
                    }
                }
            }

        }
        if (!hasIndex) {
            for (int i = 0; i < this.pages.size(); i++) {
                PageInfo deletedFrom = Page.deleteFromPage(this.csvAddress, this.name, this.pages.get(i), tuple, this.clusteringKey);
                if (deletedFrom.noOfTuples == 0) {
                    Page.deletePage(deletedFrom.address);
                    deletedPages.add(deletedFrom);
                } else {
                    this.pages.set(i, deletedFrom);
                }
            }
        }

        for (int i = 0; i < deletedPages.size(); i++) {
            this.pages.remove(deletedPages.get(i));
        }
        this.renamePageFiles();
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

    public void addIndexToCsv(String[] columns, String name) throws IOException, CsvValidationException {
        String csvAddress = this.csvAddress;
        Page.createPage("src/resources/temp.csv");
        CSVWriter writer = new CSVWriter(new FileWriter("src/resources/temp.csv", true));


        FileReader filereader = new FileReader(csvAddress);
        CSVReader csvReader = new CSVReader(filereader);
        String[] nextRecord;
        while ((nextRecord = csvReader.readNext()) != null) {
            String[] record = nextRecord;
            if (nextRecord[0].equals(this.name) && Arrays.stream(columns).toList().contains(nextRecord[1])) {
                nextRecord[4] = name;
                nextRecord[5] = "Octree";

            }
            writer.writeNext(record, false);

        }
        writer.close();

        Page.deletePage(this.csvAddress);
        Page.renameFile("src/resources/temp.csv", this.csvAddress);

    }

    public static Table getTableFromDisk(String tableName) throws IOException, ClassNotFoundException {
        Table table = null;
        FileInputStream fileIn = new FileInputStream("src/resources/" + tableName + "_table.ser");
        ObjectInputStream in = new ObjectInputStream(fileIn);
        table = (Table) in.readObject();
        in.close();
        fileIn.close();
        return table;
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

    public static Object getMinForColumn(String csvAddress, String tableName, String columnName) throws IOException, CsvValidationException {
        FileReader filereader = new FileReader(csvAddress);
        CSVReader csvReader = new CSVReader(filereader);
        String[] nextRecord;
        Object result = null;
        while ((nextRecord = csvReader.readNext()) != null) {
            if (nextRecord[0].equals(tableName) && nextRecord[1].equals(columnName)) {
                if (nextRecord[2].equals("java.lang.String")) {
                    result = nextRecord[6];
                } else if (nextRecord[2].equals("java.lang.Double")) {
                    result = Double.parseDouble(nextRecord[6]);
                } else if (nextRecord[2].equals("java.lang.Integer")) {
                    result = Integer.parseInt(nextRecord[6]);
                } else if (nextRecord[2].equals("java.util.Date")) {
                    result = new Date(nextRecord[6]);
                }

            }
        }
        return result;
    }

    public static String getTypeForColumn(String csvAddress, String tableName, String columnName) throws IOException, CsvValidationException {
        FileReader filereader = new FileReader(csvAddress);
        CSVReader csvReader = new CSVReader(filereader);
        String[] nextRecord;
        String result = null;
        while ((nextRecord = csvReader.readNext()) != null) {
            if (nextRecord[0].equals(tableName) && nextRecord[1].equals(columnName)) {
                result = nextRecord[2];
            }
        }
        return result;
    }

    public static Object getMaxForColumn(String csvAddress, String tableName, String columnName) throws IOException, CsvValidationException {
        FileReader filereader = new FileReader(csvAddress);
        CSVReader csvReader = new CSVReader(filereader);
        String[] nextRecord;
        Object result = null;
        while ((nextRecord = csvReader.readNext()) != null) {
            if (nextRecord[0].equals(tableName) && nextRecord[1].equals(columnName)) {
                if (nextRecord[2].equals("java.lang.String")) {
                    result = nextRecord[7];
                } else if (nextRecord[2].equals("java.lang.Double")) {
                    result = Double.parseDouble(nextRecord[7]);
                } else if (nextRecord[2].equals("java.lang.Integer")) {
                    result = Integer.parseInt(nextRecord[7]);
                } else if (nextRecord[2].equals("java.util.Date")) {
                    result = new Date(nextRecord[7]);
                }

            }
        }
        return result;
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

    public static void validateTupleOnDelete(String csvAddress, String tableName, Hashtable<String, Object> tuple) throws DBAppException {
        try {
            boolean tableExists = Table.tableExistsOnDisk(csvAddress, tableName);
            if (!tableExists) {
                throw new DBAppException("table does not exist");
            }
            Table.checkTupleColumnsInTable(csvAddress, tableName, tuple);
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

    public static Hashtable<String, Object> validateTupleOnInsert(String csvAddress, String tableName, Hashtable<String, Object> tuple) throws DBAppException {
        try {
            boolean tableExists = Table.tableExistsOnDisk(csvAddress, tableName);
            if (!tableExists) {
                throw new DBAppException("table does not exist");
            }
            Table.checkTupleColumnsInTable(csvAddress, tableName, tuple);
        } catch (Exception e) {
            throw new DBAppException(e.getMessage());
        }


        Enumeration<String> keys = tuple.keys();
        while (keys.hasMoreElements()) {
            String currColumnName = keys.nextElement();
            Object currColumnValue = tuple.get(currColumnName);
            Table.validateTableColumn(csvAddress, tableName, currColumnName, currColumnValue);
        }
        try {
            Table table = Table.getTableFromDisk(tableName);
            Table.checkTupleClusterKeyExists(table, tuple);
            Hashtable<String, Object> newTuple = Table.setTupleNullValues(csvAddress, tableName, tuple);
            if (!tuple.containsKey(table.clusteringKey)) {
                throw new DBAppException("clustering key not provided");
            }
            return newTuple;
        } catch (Exception e) {
            throw new DBAppException(e.getMessage());
        }


    }

    public static void validateTupleOnUpdate(String csvAddress, String tableName, Hashtable<String, Object> tuple) throws DBAppException {
        try {
            boolean tableExists = Table.tableExistsOnDisk(csvAddress, tableName);
            if (!tableExists) {
                throw new DBAppException("table does not exist");
            }
            Table.checkTupleColumnsInTable(csvAddress, tableName, tuple);
            Table table = Table.getTableFromDisk(tableName);
            Table.checkTupleClusterKeyNotExists(table, tuple);
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

    public static Hashtable<String, Object> setTupleNullValues(String csvAddress, String tableName, Hashtable<String, Object> tuple) throws DBAppException, CsvValidationException, IOException {

        ArrayList<String> tableColumns = Table.getAllColumnsForTableFromCSV(csvAddress, tableName);
        Enumeration<String> keys = tuple.keys();
        while (keys.hasMoreElements()) {
            String currColumnName = keys.nextElement();
            tableColumns.remove(currColumnName);
        }
        for (int i = 0; i < tableColumns.size(); i++) {
            tuple.put(tableColumns.get(i), new NullObject());
        }
        return tuple;
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

    public static void checkTupleClusterKeyNotExists(Table table, Hashtable<String, Object> tuple) throws DBAppException {

        String clusterKey = table.clusteringKey;
        Boolean exist = false;
        Enumeration<String> keys = tuple.keys();
        while (keys.hasMoreElements()) {
            String currColumnName = keys.nextElement();
            if (currColumnName.equals(clusterKey)) {
                exist = true;
            }
        }
        if (exist) {
            throw new DBAppException("Cluster key column should not be changed");
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
            } else {
                correct = false;
                break;
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
                result.add(nextRecord[1]);
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

    public static void printAllIndexes(String tableName) throws Exception {
        FileInputStream fileIn = new FileInputStream("src/resources/" + tableName + "_table.ser");
        ObjectInputStream in = new ObjectInputStream(fileIn);
        Table table = (Table) in.readObject();
        in.close();
        fileIn.close();

        for (int i = 0; i < table.indexes.size(); i++) {
            System.out.println(i + 1);
            Octree tree = Octree.read(table.indexes.get(i).address);
            tree.printOctree();
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

    public static ArrayList<String> getAllIndexedColumns(String tableName) throws IOException, ClassNotFoundException {
        FileInputStream fileIn = new FileInputStream("src/resources/" + tableName + "_table.ser");
        ObjectInputStream in = new ObjectInputStream(fileIn);
        Table table = (Table) in.readObject();
        in.close();
        fileIn.close();
        ArrayList<String> result = new ArrayList<>();

        for (int i = 0; i < table.indexes.size(); i++) {
            result.add(table.indexes.get(i).columns[0]);
            result.add(table.indexes.get(i).columns[1]);
            result.add(table.indexes.get(i).columns[2]);
        }
        return result;
    }

    public Iterator<Hashtable<String, Object>> selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws Exception {
        List<Hashtable<String, Object>> result = new ArrayList<>();
        TableIterator it = null;
        Boolean hasindex = false;
        ArrayList<String[]> columnsList = findAndedTriple(arrSQLTerms,strarrOperators);
        for(int i =0;i<columnsList.size();i++){
            String[] columns = columnsList.get(i);
            if(Arrays.stream(this.indexes.get(i).columns).toList().containsAll(Arrays.stream(columns).toList())){
                Octree tree = Octree.read(this.indexes.get(i).address);
                 it = this.getTableIteratorIndex(tree.getAllNodesAddresses());
                 hasindex = true;
            }
        }
        if(!hasindex){
            it = this.getTableIterator();
        }

        while (it.hasNext()) {
            Hashtable<String, Object> row = it.next();

            boolean rowMatches = checkRowMatchesTerms(row, arrSQLTerms, strarrOperators);
            if (rowMatches) {
                result.add(row);
            }
        }

        return result.iterator();
    }

    private boolean checkRowMatchesTerms(Hashtable<String, Object> row, SQLTerm[] arrSQLTerms, String[] strarrOperators) throws CsvValidationException, IOException {
        if (arrSQLTerms.length == 0) {
            return true;
        }

        boolean matches = checkRowMatchesTerm(row, arrSQLTerms[0]);
        for (int i = 1; i < arrSQLTerms.length; i++) {

            boolean termMatches = checkRowMatchesTerm(row, arrSQLTerms[i]);
            switch (strarrOperators[i - 1]) {
                case "AND":
                    matches = matches && termMatches;
                    break;
                case "OR":
                    matches = matches || termMatches;
                    break;
                case "XOR":
                    matches = matches ^ termMatches;
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported operator " + strarrOperators[i - 1]);
            }
        }

        return matches;
    }

    private boolean checkRowMatchesTerm(Hashtable<String, Object> row, SQLTerm term) throws CsvValidationException, IOException {
        Object value = row.get(term._strColumnName);
        switch (term._strOperator) {
            case "=":
                return Octree.compareKey(value,term._objValue,Table.getTypeForColumn(this.csvAddress,this.name,term._strColumnName)) == 0;
            case "!=":
                return Octree.compareKey(value,term._objValue,Table.getTypeForColumn(this.csvAddress,this.name,term._strColumnName)) != 0;
            case "<":
                return Octree.compareKey(value,term._objValue,Table.getTypeForColumn(this.csvAddress,this.name,term._strColumnName)) < 0;
            case ">":
                return Octree.compareKey(value,term._objValue,Table.getTypeForColumn(this.csvAddress,this.name,term._strColumnName)) > 0;
            case "<=":
                return Octree.compareKey(value,term._objValue,Table.getTypeForColumn(this.csvAddress,this.name,term._strColumnName)) <= 0;
            case ">=":
                return Octree.compareKey(value,term._objValue,Table.getTypeForColumn(this.csvAddress,this.name,term._strColumnName)) >= 0;
            default:
                throw new UnsupportedOperationException("Unsupported operator " + term._strOperator);
        }
    }



    public TableIterator getTableIterator() throws IOException, ClassNotFoundException {
        List<String> pageAddresses = new ArrayList<>();
        for (int i =0;i<this.pages.size();i++){
            pageAddresses.add(this.pages.get(i).address);
        }
        return new TableIterator(pageAddresses);
    }
    public TableIterator getTableIteratorIndex(List<String> pageAddresses) throws IOException, ClassNotFoundException {
        return new TableIterator(pageAddresses);
    }

    public ArrayList<String[]> findAndedTriple(SQLTerm[] arrSQLTerms, String[] strarrOperators) {
        ArrayList<String[]> results = new ArrayList<>();

        // We need at least 3 terms and 2 operators for a triple
        if (arrSQLTerms.length < 3 || strarrOperators.length < 2) {
            return results;
        }

        for (int i = 0; i < arrSQLTerms.length - 2; i++) {
            for (int j = i + 1; j < arrSQLTerms.length - 1; j++) {
                for (int k = j + 1; k < arrSQLTerms.length; k++) {
                    // All operators between the terms need to be AND
                    if (areAllAnds(strarrOperators, i, j, k)) {
                        results.add(new String[]{
                                arrSQLTerms[i]._strColumnName,
                                arrSQLTerms[j]._strColumnName,
                                arrSQLTerms[k]._strColumnName
                        });
                    }
                }
            }
        }
        return results;
    }

    private boolean areAllAnds(String[] strarrOperators, int i, int j, int k) {
        for (int index = i; index < k; index++) {
            if (!strarrOperators[index].equals("AND")) {
                return false;
            }
        }
        return true;
    }




}
