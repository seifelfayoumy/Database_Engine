import com.opencsv.exceptions.CsvValidationException;

import java.io.IOException;
import java.util.Comparator;
import java.util.Hashtable;

public class PageComparator implements Comparator {

    public String tableName;
    public String csvAddress;
    public String clusterKey;

    public PageComparator(String tableName, String csvAddress, String clusterKey) {
        this.tableName = tableName;
        this.csvAddress = csvAddress;
        this.clusterKey = clusterKey;
    }


    @Override
    public int compare(Object o1, Object o2) {
        Hashtable<String, Object> firstTuple = (Hashtable<String, Object>) o1;
        Hashtable<String, Object> secondTuple = (Hashtable<String, Object>) o2;
        try {
            return Table.compareClusterKey(this.csvAddress, this.tableName, firstTuple.get(this.clusterKey), secondTuple.get(this.clusterKey));
        } catch (CsvValidationException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }
}
