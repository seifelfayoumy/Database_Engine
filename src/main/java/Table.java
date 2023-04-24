import java.io.*;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Properties;
import java.util.Vector;

public class Table implements Serializable {

    String name;
    ArrayList<String> pagesAddresses;
    int noOfPages;
    String clusteringKey;

    public Table(String name, String clusteringKey) {
        this.name = name;
        this.pagesAddresses = new ArrayList<String>();
        this.noOfPages = 0;
        this.clusteringKey = clusteringKey;
    }

    public void insert(Object tuple) {

    }


    public void save() throws Exception {

        FileOutputStream fileOut = new FileOutputStream("src/resources/" + this.name + ".ser");
        ObjectOutputStream out = new ObjectOutputStream(fileOut);
        out.writeObject(this);
        out.close();
        fileOut.close();


    }

    public void insertToPage(String address, Hashtable<String, Object> tuple) throws IOException, MaxRowsException, ClassNotFoundException {
        Properties prop = new Properties();
        FileInputStream fis = new FileInputStream("src/resources/DBApp.config");
        prop.load(fis);
        Integer maxTuples = Integer.parseInt(prop.getProperty("MaximumRowsCountinTablePage"));

        Vector<Hashtable<String, Object>> page = Page.readPage(address);
        if(page.size() < maxTuples){
            page.add(tuple);
            //page.sort();
        }else{
            throw new MaxRowsException("MAX ROWS");
        }

    }


}
