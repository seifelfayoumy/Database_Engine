import java.lang.reflect.Type;
import java.util.Hashtable;
import java.util.Objects;
import java.util.Vector;

public class Main {
    public static void main(String[] args) {
        System.out.println("Started main function...");


        try {
            DBApp dbApp = new DBApp();

//
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
//            htblColNameValue.put("id", new Integer( 11 ));
//            htblColNameValue.put("gpa", new Double( 0.95 ) );
//            htblColNameValue.put("name", new String("Ahmed" ) );
//
//            dbApp.insertIntoTable("user",htblColNameValue);
//
//
//            Hashtable htblColNameValue = new Hashtable<String,Object>( );
//            htblColNameValue.put("id", new Integer( 4 ));
//
//            dbApp.deleteFromTable("user",htblColNameValue);

//            String y = "100000";
//            Object x = new Integer(2);
//
//            ((Double) x).compareTo(Double.parseDouble("22"));

            Table.printAllPages("user","id");


            //DUE
//            insert: resursion , insert after deleteion into same page,
//            inserting to values smaller than first page, inserting to values between pages

        } catch (Exception e) {
            throw new RuntimeException(e);
        }


    }
}