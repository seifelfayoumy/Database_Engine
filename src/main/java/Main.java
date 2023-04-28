import java.util.Hashtable;

public class Main {
    public static void main(String[] args) {
        System.out.println("Started main function...");


        try {
            DBApp dbApp = new DBApp();

////
//            Hashtable htblColNameType = new Hashtable();
//            htblColNameType.put("id", "java.lang.Integer");
//            htblColNameType.put("name", "java.lang.String");
//            htblColNameType.put("gpa", "java.lang.Double");
////
//            Hashtable htblColNameMin = new Hashtable();
//            htblColNameMin.put("id",  "0");
//            htblColNameMin.put("name", "A");
//            htblColNameMin.put("gpa", "0");
//
//            Hashtable htblColNameMax = new Hashtable();
//            htblColNameMax.put("id", "100000");
//            htblColNameMax.put("name", "ZZZZZZZZZ");
//            htblColNameMax.put("gpa", "1000000");
//
//            dbApp.createTable("user", "id", htblColNameType, htblColNameMin, htblColNameMax);


//
//            Hashtable htblColNameValue = new Hashtable<String,Object>( );
//            htblColNameValue.put("id", new Integer( 2 ));
//            htblColNameValue.put("gpa", new Double( 0.95 ) );
////            htblColNameValue.put("name", new String("Ahmed" ) );
//
//            dbApp.insertIntoTable("user",htblColNameValue);
//
//
//            Hashtable htblColNameValue = new Hashtable<String,Object>();
//            htblColNameValue.put("id", new Integer( 90 ));
//
//            dbApp.deleteFromTable("user",htblColNameValue);

//            String y = "100000";
//            Object x = new Integer(2);
//
//            ((Double) x).compareTo(Double.parseDouble("22"));

//            Table.printAllPagesClusterKey("user","id");
//            Table.printAllPages("user");


            //DUE
            //      null wrapper when insert. Done but gives error
            //check al columns in table, done but not implemented
            //check cluster key is available, done but not implemented
            //throwing exceptions when input
            //deleting pages from memory

        } catch (Exception e) {
            throw new RuntimeException(e);
        }


    }
}