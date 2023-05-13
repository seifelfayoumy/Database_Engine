import java.time.Period;
import java.time.temporal.ChronoUnit;
import java.util.Calendar;
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

             Date d1 = new Date("11/07/1990");
            Date d2 = new Date("11/07/2020");
            long diff = (d2.getTime() - d1.getTime())/2;
            int monthDiff = (d2.getMonth() - d1.getMonth())/2;
            int hourDiff = (d2.getHours() - d1.getHours())/2;
            int yearDiff = (d2.getYear() - d1.getYear())/2;
            Calendar c = Calendar.getInstance();
            c.setTime(d1);
            c.add(Calendar.MONTH,monthDiff);
            c.add(Calendar.HOUR,hourDiff);
            c.add(Calendar.YEAR,yearDiff);
            Date newD = c.getTime();

            System.out.println(newD );

           // ChronoUnit.DAYS.between(d1,d2);
           // LocalDate median = d1.plusDays(ChronoUnit.DAYS.between(gerbutsmin, gerbutsmax) / 2);
         //  System.out.println(test);

           // System.out.println("a".compareTo("z"));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }


    }
}