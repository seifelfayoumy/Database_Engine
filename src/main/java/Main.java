import java.util.Date;
import java.util.Hashtable;

public class Main {
    public static void main(String[] args) {
        System.out.println("Started main function...");


        try {
            DBApp dbApp = new DBApp();

//            Hashtable htblColNameType = new Hashtable( );
//            htblColNameType.put("id", "java.lang.Integer");
//            htblColNameType.put("name", "java.lang.String");
//            htblColNameType.put("gpa", "java.lang.Double");
//
//            Hashtable htblColNameMin = new Hashtable( );
//            htblColNameMin.put("id", "0");
//            htblColNameMin.put("name", "AAAAAAAA");
//            htblColNameMin.put("gpa", "0");
//
//            Hashtable htblColNameMax = new Hashtable( );
//            htblColNameMax.put("id", "10000");
//            htblColNameMax.put("name", "ZZZZZZZ");
//            htblColNameMax.put("gpa", "10000");
//
//            String strTableName = "user";
//
//            dbApp.createTable( strTableName,"id",htblColNameType,htblColNameMin,htblColNameMax);



//                            Hashtable htblColNameValue = new Hashtable<String, Object>();
//                            htblColNameValue.put("id", new Integer(50));
//                            htblColNameValue.put("gpa", new Double(11));
//                            htblColNameValue.put("name", new String("Ahmed" ) );
//
//                            dbApp.insertIntoTable("user", htblColNameValue);

//                            Hashtable htblColNameValue = new Hashtable<String, Object>();
////                            htblColNameValue.put("gpa", new Double(0.988));
//                            htblColNameValue.put("name", new String("Ahmed123" ) );
//                //            htblColNameValue.put("id", new Integer(1 ) );
//
//                            dbApp.updateTable("user","50", htblColNameValue);

                //
                //
//                            Hashtable htblColNameValue = new Hashtable<String,Object>();
//                            htblColNameValue.put("id", new Integer( 50 ));
////                            htblColNameValue.put("gpa", new Double( 11 ));
//                //            htblColNameValue.put("name", new String( "Ahmed1" ));
//
//                            dbApp.deleteFromTable("user",htblColNameValue);

                //            Object y = new Double(11.0);
                //            Object x = new Double(11);
                //            System.out.println(y.equals(x));
                //
                //            ((Double) x).compareTo(Double.parseDouble("22"));

                //            Table.printAllPagesClusterKey("user","id");
                //Table.printAllPages("user");

        } catch (Exception e) {
            throw new RuntimeException(e);
        }


    }
}