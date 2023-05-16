import java.io.IOException;
import java.util.*;

public class TableIterator implements Iterator<Hashtable<String, Object>> {
    private final List<String> pageAddresses;
    private int currentPageIndex;
    private Vector<Hashtable<String,Object>> currentPage;
    private int currentRowIndex;

    public TableIterator(List<String> pageAddresses) throws IOException, ClassNotFoundException {
        this.pageAddresses = new ArrayList<>(new HashSet<>(pageAddresses));
        this.currentPageIndex = 0;
        this.currentRowIndex = 0;
        loadPage();
    }

    private void loadPage() throws IOException, ClassNotFoundException {
        if (currentPageIndex < pageAddresses.size()) {
            this.currentPage = Page.readPage(this.pageAddresses.get(currentPageIndex));
        } else {
            this.currentPage = null;
        }
    }

    @Override
    public boolean hasNext() {
        if (currentPage == null) {
            return false;
        }
        if (currentRowIndex < currentPage.size()) {
            return true;
        }
        if (currentPageIndex < pageAddresses.size() - 1) {
            currentPageIndex++;
            currentRowIndex = 0;
            try {
                loadPage();
            } catch (IOException e) {
                throw new RuntimeException(e);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
            return hasNext();
        }
        return false;
    }

    @Override
    public Hashtable<String, Object> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return currentPage.get(currentRowIndex++);
    }
}
