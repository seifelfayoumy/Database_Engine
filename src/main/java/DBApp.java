import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvValidationException;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.tree.ParseTree;

import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

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
            if (updatedTuple.get(currColumnName) instanceof String) {
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
        Enumeration<String> keys = htblColNameValue.keys();
        while (keys.hasMoreElements()) {
            String currColumnName = keys.nextElement();
            if (htblColNameValue.get(currColumnName) instanceof String) {
                String newValue = htblColNameValue.get(currColumnName).toString().toLowerCase();
                htblColNameValue.replace(currColumnName, newValue);
            }
        }
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
                            String[] strarrColName) throws DBAppException {
        if (strarrColName.length != 3) {
            throw new DBAppException("columns number should be 3 to make an index");
        }

        try {
            boolean tableExists = Table.tableExistsOnDisk("src/resources/metadata.csv", strTableName);
            if (!tableExists) {
                throw new DBAppException("table does not exist");
            }
            ArrayList indexedCols = Table.getAllIndexedColumns(strTableName);
            if (indexedCols.contains(strarrColName[0]) || indexedCols.contains(strarrColName[1]) || indexedCols.contains(strarrColName[2])) {
                throw new DBAppException("index already exists on one or more of the columns");
            }
            ArrayList<String> cols = Table.getAllColumnsForTableFromCSV("src/resources/metadata.csv", strTableName);
            if (!cols.containsAll(List.of(strarrColName))) {
                throw new DBAppException("invalid columns");
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

    public Iterator<Hashtable<String, Object>> selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators)
            throws DBAppException {
        String tableName = arrSQLTerms[0]._strTableName;
        for (int i = 0; i < this.tables.size(); i++) {
            if (this.tables.get(i).name.equals(tableName)) {
                try {
                    return this.tables.get(i).selectFromTable(arrSQLTerms, strarrOperators);
                } catch (Exception e) {
                    throw new DBAppException(e.getMessage());
                }
            }
        }

        try {
            ArrayList<String> emptySet = new ArrayList<String>();
            return new TableIterator(emptySet);
        } catch (Exception e) {
            throw new DBAppException(e.getMessage());
        }
    }

    //BONUS
    public Iterator parseSQL(StringBuffer strbufSQL) throws DBAppException {
        try {


            String sql = strbufSQL.toString();
            CharStream charStream = CharStreams.fromString(sql);
            MySqlLexer lexer = new MySqlLexer(charStream);
            CommonTokenStream tokens = new CommonTokenStream(lexer);
            MySqlParser parser = new MySqlParser(tokens);
            MySqlParser.DmlStatementContext context = parser.dmlStatement();

            ParseTree sqlStatement = context.getChild(0);

            if (sqlStatement instanceof MySqlParser.CreateTableContext) {


                MySqlParser.CreateTableContext createTableContext = (MySqlParser.CreateTableContext) sqlStatement;

            } else if (sqlStatement instanceof MySqlParser.InsertStatementContext) {
                MySqlParser.InsertStatementContext insertStatementContext = (MySqlParser.InsertStatementContext) sqlStatement;
                String tableName = insertStatementContext.tableName().getText();

                Hashtable<String, Object> columns = new Hashtable<>();
                insertStatementContext.columns.fullColumnName().size();
                for (int i = 0; i < insertStatementContext.columns.fullColumnName().size(); i++) {
                    String columnName = insertStatementContext.columns.fullColumnName().get(i).getText();
                    String type = Table.getTypeForColumn("src/resources/metadata.csv", tableName, columnName);
                    String columnValue = insertStatementContext.insertStatementValue().getChild(2).getText().split(",")[i];

                    Object value = null;
                    if (type.equals("java.lang.String")) {
                        value = columnValue;
                    } else if (type.equals("java.lang.Double")) {
                        value = Double.parseDouble(columnValue);
                    } else if (type.equals("java.lang.Integer")) {
                        value = Integer.parseInt(columnValue);
                    } else if (type.equals("java.util.Date")) {
                        value = new Date(columnValue);
                    }
                    columns.put(columnName, value);
                }
                this.insertIntoTable(tableName, columns);
            } else if (sqlStatement instanceof MySqlParser.UpdateStatementContext) {


                MySqlParser.UpdateStatementContext updateStatementContext = (MySqlParser.UpdateStatementContext) sqlStatement;

            } else if (sqlStatement instanceof MySqlParser.DeleteStatementContext) {
                MySqlParser.DeleteStatementContext deleteStatementContext = (MySqlParser.DeleteStatementContext) sqlStatement;

            } else if (sqlStatement instanceof MySqlParser.SelectStatementContext) {


                MySqlParser.SelectStatementContext selectStatementContext = (MySqlParser.SelectStatementContext) sqlStatement;


            } else if (sqlStatement instanceof MySqlParser.CreateIndexContext) {

                MySqlParser.CreateIndexContext createIndexContext = (MySqlParser.CreateIndexContext) sqlStatement;

            } else {
                throw new DBAppException("Unsupported SQL statement type");
            }

        } catch (Exception e) {
            throw new DBAppException("invalid SQL");
        }
        return null;
    }


}
