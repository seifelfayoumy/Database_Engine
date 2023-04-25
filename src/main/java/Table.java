import java.io.*;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Properties;

public class Table implements Serializable {

    String name;
    ArrayList<PageInfo> pages;
    String clusteringKey;
    int maxRows;

    public Table(String name, String clusteringKey) throws Exception {

        Properties prop = new Properties();
        FileInputStream fis = new FileInputStream("src/resources/DBApp.config");
        prop.load(fis);
        this.maxRows = Integer.parseInt(prop.getProperty("MaximumRowsCountinTablePage"));

        this.name = name;
        this.pages = new ArrayList<PageInfo>();
        this.clusteringKey = clusteringKey;

        this.save();
    }

    public void insert(Hashtable<String, Object> tuple) throws MaxRowsException, IOException, ClassNotFoundException, DuplicateRowException {
        if (this.pages.size() == 0) {
            String pageAddress = "src/resources/" + this.name + "_1.ser";
            PageInfo newPage = new PageInfo(this.maxRows, pageAddress);
            this.pages.add(newPage);
            Page.createPage(pageAddress);
            this.pages.add(0, Page.insertToFirstPage(this.pages.get(0), tuple, this.clusteringKey));
        } else {
            for (int i = 0; i < this.pages.size(); i++) {
                if (i != this.pages.size() - 1) {
                    if (tuple.get(this.clusteringKey).toString().compareTo(this.pages.get(i).minValue) >= 0
                            && tuple.get(this.clusteringKey).toString().compareTo(this.pages.get(i + 1).minValue) < 0) {
                        if (this.pages.get(i).isFull) {
                            Hashtable<String, Object> lastTuple = Page.getLastTuple(this.pages.get(i));
                            this.pages.set(i, Page.removeLastTuple(this.pages.get(i), this.clusteringKey));
                            this.pages.set(i, Page.insertToPage(this.pages.get(i), tuple, this.clusteringKey));
                            this.insert(lastTuple);
                        } else {
                            this.pages.set(i, Page.insertToPage(this.pages.get(i), tuple, this.clusteringKey));
                        }
                    }
                } else {
                    if (tuple.get(this.clusteringKey).toString().compareTo(this.pages.get(i).minValue) >= 0
                            && tuple.get(this.clusteringKey).toString().compareTo(this.pages.get(i + 1).minValue) < 0) {
                        if (this.pages.get(i).isFull) {
                            Hashtable<String, Object> lastTuple = Page.getLastTuple(this.pages.get(i));
                            this.pages.set(i, Page.removeLastTuple(this.pages.get(i), this.clusteringKey));
                            this.pages.set(i, Page.insertToPage(this.pages.get(i), tuple, this.clusteringKey));
                            String pageAddress = "src/resources/" + this.name + "_" + this.pages.size() + 1 + ".ser";
                            PageInfo newPage = new PageInfo(this.maxRows, pageAddress);
                            this.pages.add(newPage);
                            Page.createPage(pageAddress);
                            Page.insertToPage(newPage, lastTuple, this.clusteringKey);
                        } else {
                            this.pages.set(i, Page.insertToPage(this.pages.get(i), tuple, this.clusteringKey));
                        }
                    }
                }
            }
        }


    }


    public void save() throws Exception {

        FileOutputStream fileOut = new FileOutputStream("src/resources/" + this.name + "_table.ser");
        ObjectOutputStream out = new ObjectOutputStream(fileOut);
        out.writeObject(this);
        out.close();
        fileOut.close();

    }


}
