import java.io.File;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.regex.Pattern;

public class ReduceCallable implements Callable {
    private final String docName;
    ArrayList<Future<MapResult>> mapResults;

    public ReduceCallable(String docName, ArrayList<Future<MapResult>> mapResults) {
        this.docName = docName;
        this.mapResults = mapResults;
    }

    @Override
    public ReduceResult call() throws Exception {
        HashMap<Integer, Integer> mergedDictionary = new HashMap<>();
        ArrayList<String> maxList = new ArrayList<>();

        mergeResults(mergedDictionary, maxList);
        Double rank = calculateRank(mergedDictionary);

        int max = calculateMax(maxList);
        maxList.removeIf(e -> e.length() < max); // stergere cuvinte cu lungime mai mica decat max

        return new ReduceResult(docName, rank, max, maxList.size());
    }

    // functie care calculeaza lungimea maxima
    private int calculateMax(ArrayList<String> maxList) {
        int max = -1;
        for (String i : maxList) {
            if (i.length() > max) {
                max = i.length();
            }
        }
        return max;
    }

    // functie care calculeaza fibonacci
    private int fib(int n) { return n <= 1 ? n : fib(n-1) + fib(n-2); }

    // functie care calculeaza rangul unui fisier conform functiei din cerinta
    private Double calculateRank(HashMap<Integer, Integer> mergedDictionary) {
        int wordCount = 0;
        double rank = 0d;
        for (var key : mergedDictionary.keySet()) {
            rank += fib(key + 1) * mergedDictionary.get(key);
            wordCount += mergedDictionary.get(key);
        }

        rank /= wordCount;
        return rank;
    }

    // functie care combina rezultatele partiale din faza de Map
    private void mergeResults(HashMap<Integer, Integer> mergedDictionary, ArrayList<String> maxList) throws InterruptedException, ExecutionException {
        for (Future<MapResult> v : mapResults) {
            if (Objects.equals(v.get().docName, docName)) { // combina doar listele provinte din fisierul corespunzator workerului
                v.get().dictionary.forEach( // cheie = cheie, valoare = valoare1 + valoare2
                        (key, value) -> mergedDictionary.merge(key, value, Integer::sum)
                );
                maxList.addAll(v.get().wordList);
            }
        }
    }
}

class ReduceResult {
    String docName;
    Double rank;
    Integer maxLength;
    Integer count;

    public ReduceResult(String docName, Double rank, Integer maxLength, Integer count) {
        this.docName = docName;
        this.rank = rank;
        this.maxLength = maxLength;
        this.count = count;
    }

    @Override
    public String toString() {
        String[] fileName = docName.split(Pattern.quote(File.separator));
        return fileName[fileName.length-1] + "," + String.format("%.2f", rank) + "," + maxLength + "," + count + "\n";
    }
}