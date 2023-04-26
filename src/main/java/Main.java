import java.util.Hashtable;

public class Main {
    public static void main(String[] args) {
        System.out.println("Started main function...");


        try {
            DBApp dbApp = new DBApp();


//            Hashtable htblColNameType = new Hashtable();
//            htblColNameType.put("id", "java.lang.Integer");
//            htblColNameType.put("name", "java.lang.String");
//            htblColNameType.put("gpa", "java.lang.Double");
////
//            Hashtable htblColNameMin = new Hashtable();
//            htblColNameMin.put("id", "0");
//            htblColNameMin.put("name", "A");
//            htblColNameMin.put("gpa", "0");
//
//            Hashtable htblColNameMax = new Hashtable();
//            htblColNameMax.put("id", "1000000");
//            htblColNameMax.put("name", "ZZZZZZZZZZZ");
//            htblColNameMax.put("gpa", "1000000");
//
//            dbApp.createTable("user", "id", htblColNameType, htblColNameMin, htblColNameMax);


//
//            Hashtable htblColNameValue = new Hashtable<String,Object>( );
//            htblColNameValue.put("id", new Integer( 1000 ));
//            htblColNameValue.put("gpa", new Double( 0.95 ) );
//            htblColNameValue.put("name", new String("Ahmed" ) );
//
//            dbApp.insertIntoTable("user",htblColNameValue);
//
//
            Hashtable htblColNameValue = new Hashtable<String,Object>( );
            htblColNameValue.put("id", new Integer( 1000 ));

            dbApp.deleteFromTable("user",htblColNameValue);



//            String x = "9";
//            String y = "100000000";
//            System.out.println(x.compareTo(y));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }


    }
}