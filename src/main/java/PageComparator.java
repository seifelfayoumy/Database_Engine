import java.util.Comparator;
import java.util.Hashtable;

public class PageComparator implements Comparator {

    public String key;

    public PageComparator(String key){
        this.key = key;
    }


    @Override
    public int compare(Object o1, Object o2) {
        Hashtable<String, Object> firstTuple = (Hashtable<String, Object>) o1;
        Hashtable<String, Object> secondTuple = (Hashtable<String, Object>) o2;
        return firstTuple.get(this.key).toString().compareTo(secondTuple.get(this.key).toString());
    }
}
