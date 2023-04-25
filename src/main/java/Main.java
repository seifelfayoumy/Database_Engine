import java.util.Hashtable;

public class Main {
    public static void main(String[] args) {
        System.out.println("Started main function...");


        try {
            DBApp dbApp = new DBApp();

            Hashtable htblColNameValue = new Hashtable<String,Object>( );
            htblColNameValue.put("id", new Integer( 2343432 ));
            htblColNameValue.put("gpa", new Double( 0.95 ) );
            htblColNameValue.put("name", new String("Ahmed Noor" ) );

            dbApp.insertIntoTable("user",htblColNameValue);

//            Hashtable htblColNameType = new Hashtable();
//            htblColNameType.put("id", "java.lang.Integer");
//            htblColNameType.put("name", "java.lang.String");
//            htblColNameType.put("gpa", "java.lang.double");
//
//            Hashtable htblColNameMin = new Hashtable();
//            htblColNameMin.put("id", "0");
//            htblColNameMin.put("name", "0");
//            htblColNameMin.put("gpa", "0");
//
//            Hashtable htblColNameMax = new Hashtable();
//            htblColNameMax.put("id", "100");
//            htblColNameMax.put("name", "100");
//            htblColNameMax.put("gpa", "100");
//
//            dbApp.createTable("user", "id", htblColNameType, htblColNameMin, htblColNameMax);
        } catch (DBAppException e) {
            throw new RuntimeException(e);
        }


    }
}