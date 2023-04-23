import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;

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

    public void insert(Object tuple){

    }


    public void save() throws Exception {

        FileOutputStream fileOut = new FileOutputStream("src/resources/" + this.name + ".ser");
        ObjectOutputStream out = new ObjectOutputStream(fileOut);
        out.writeObject(this);
        out.close();
        fileOut.close();
    }


}
