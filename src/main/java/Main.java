import java.time.Period;
import java.time.temporal.ChronoUnit;
import java.util.*;

public class Main {
    public static void main(String[] args) {
        System.out.println("Started main function...");

        //TEST IN THE TRY CATCH BELOW:

        try {
//            DBApp dbApp = new DBApp();
//////
//            Hashtable htblColNameType = new Hashtable( );
//            htblColNameType.put("id", "java.lang.Integer");
//            htblColNameType.put("name", "java.lang.String");
//            htblColNameType.put("gpa", "java.lang.Double");
//
//            Hashtable htblColNameMin = new Hashtable( );
//            htblColNameMin.put("id", "0");
//            htblColNameMin.put("name", "A");
//            htblColNameMin.put("gpa", "0");
//
//            Hashtable htblColNameMax = new Hashtable( );
//            htblColNameMax.put("id", "10000");
//            htblColNameMax.put("name", "zzzzzzzzzzzz");
//            htblColNameMax.put("gpa", "10000");
//
//
//            String strTableName = "user";
////
//            dbApp.createTable( strTableName,"id",htblColNameType,htblColNameMin,htblColNameMax);
//
//          dbApp.createIndex("user", new String[]{"id", "name", "gpa"});

//


//                            Hashtable htblColNameValue = new Hashtable<String, Object>();
//                            htblColNameValue.put("id", new Integer(7));
//                            htblColNameValue.put("gpa", new Double(11));
//                            htblColNameValue.put("name", new String("Ahmed1" ) );
////            Hashtable htblColNameValue = new Hashtable<String, Object>();
////            htblColNameValue.put("id", new Integer(3));
////            htblColNameValue.put("gpa", new Integer(11));
////            htblColNameValue.put("name", new Integer(20 ) );
//
//                            dbApp.insertIntoTable("user", htblColNameValue);

//            SQLTerm[] arrSQLTerms;
//            arrSQLTerms = new SQLTerm[3];
//            arrSQLTerms[0] = new SQLTerm();
//            arrSQLTerms[1] = new SQLTerm();
//            arrSQLTerms[2] = new SQLTerm();
//            arrSQLTerms[0]._strTableName = "user";
//            arrSQLTerms[0]._strColumnName = "name";
//            arrSQLTerms[0]._strOperator = "=";
//            arrSQLTerms[0]._objValue = "ahmed1";
//            arrSQLTerms[1]._strTableName = "user";
//            arrSQLTerms[1]._strColumnName = "gpa";
//            arrSQLTerms[1]._strOperator = "=";
//            arrSQLTerms[1]._objValue = new Double(11);
//            arrSQLTerms[2]._strTableName = "user";
//            arrSQLTerms[2]._strColumnName = "id";
//            arrSQLTerms[2]._strOperator = "=";
//            arrSQLTerms[2]._objValue = new Integer(1);
//            String[] strarrOperators = new String[2];
//            strarrOperators[0] = "AND";
//            strarrOperators[1] = "OR";
//
//            Iterator<Hashtable<String,Object>> resultSet = dbApp.selectFromTable(arrSQLTerms, strarrOperators);
//            while (resultSet.hasNext()) {
//                Hashtable<String, Object> row = resultSet.next();
//                Enumeration<String> keys = row.keys();
//                while (keys.hasMoreElements()) {
//                    String currColumnName = keys.nextElement();
//                    System.out.print("  " + currColumnName + ": ");
//                    System.out.print(row.get(currColumnName));
//                }
//                System.out.println();
//
//            }

            //     Table.printAllIndexes("user");
//
//                            Hashtable htblColNameValue = new Hashtable<String, Object>();
////                            htblColNameValue.put("gpa", new Double(0.988));
//                            htblColNameValue.put("name", new String("Ahmed123" ) );
////                            htblColNameValue.put("id", new Integer(1 ) );
//
//                            dbApp.updateTable("user","5", htblColNameValue);

            //
//                //
//                            Hashtable htblColNameValue = new Hashtable<String,Object>();
//                            htblColNameValue.put("id", new Integer( 11 ));
//                            htblColNameValue.put("gpa", new Double( 11 ));
//                            htblColNameValue.put("name", new String( "ahmed123" ));
//
//                            dbApp.deleteFromTable("user",htblColNameValue);

            //            Object y = new Double(11.0);
            //            Object x = new Double(11);
            //            System.out.println(y.equals(x));
            //
            //            ((Double) x).compareTo(Double.parseDouble("22"));

            //            Table.printAllPagesClusterKey("user","id");
//                Table.printAllPages("user");


//            Table.printAllIndexes("user");
//            Table.printAllPages("user");


            // ChronoUnit.DAYS.between(d1,d2);
            // LocalDate median = d1.plusDays(ChronoUnit.DAYS.between(gerbutsmin, gerbutsmax) / 2);
            //  System.out.println(test);

//            System.out.println("A".compareTo("zzzzzzzzzz"));

//           StringBuffer sql = new StringBuffer("INSERT INTO user (id, gpa, name) VALUES(1,11,Ahmed)");
//
//           dbApp.parseSQL(sql);
//            Table.printAllPages("user");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }


    }
}