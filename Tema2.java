import java.io.*;
import java.util.*;
import java.util.concurrent.*;

public class Tema2 {

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        if (args.length < 3) {
            System.err.println("Usage: Tema2 <workers> <in_file> <out_file>");
            return;
        }

        int workers = Integer.parseInt(args[0]);
        ExecutorService tpe = Executors.newFixedThreadPool(workers);

        String inPath = args[1];
        String outPath = args[2];

        int fragmentSize;
        int numberOfFiles;
        ArrayList<String> filePaths = new ArrayList<>();

        { // parsare date din fisierul de intrare
            BufferedReader reader = new BufferedReader(new FileReader(inPath));

            String line = reader.readLine();
            fragmentSize = Integer.parseInt(line);

            line = reader.readLine();
            numberOfFiles = Integer.parseInt(line);

            for (int i = 0; i < numberOfFiles; i++) {
                line = reader.readLine();
                filePaths.add(line);
            }

            reader.close();
        }
        ArrayList<Long> sizes = docSizes(filePaths);

        // creare workeri Map
        ArrayList<Future<MapResult>> mapResults = new ArrayList<>();
        for (int i = 0; i < numberOfFiles; i++) {
            for (int j = 0; j < Math.ceil(sizes.get(i) / (float)fragmentSize); j++) {
                mapResults.add(tpe.submit(new MapCallable(filePaths.get(i), j * fragmentSize, fragmentSize)));
            }
        }
        tpe.shutdown();
        boolean finished = tpe.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS); // se asteapta rezultatele din Map

        if (finished) {
            // creare workeri Reduce
            ExecutorService tpe2 = Executors.newFixedThreadPool(workers);
            ArrayList<Future<ReduceResult>> reduceResults = new ArrayList<>();
            for (int i = 0; i < numberOfFiles; i++) {
                reduceResults.add(tpe2.submit(new ReduceCallable(filePaths.get(i), mapResults)));
            }
            tpe2.shutdown();
            finished = tpe2.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS); // se asteapta rezultatele din Reduce

            if (finished) {
                // sortare rezultate dupa rank
                reduceResults.sort((o1, o2) -> {
                    try {
                        return o2.get().rank > o1.get().rank ? 1 : -1;
                    } catch (InterruptedException | ExecutionException e) {
                        e.printStackTrace();
                    }
                    return 0;
                });

                // afisare rezultate in formatul cerintei
                BufferedWriter writer = new BufferedWriter(new FileWriter(outPath));
                for (Future<ReduceResult> r : reduceResults) {
                    writer.write(r.get().toString());
                }
                writer.close();
            }
        }
    }

    // functie care intoarce marimile documentelor de parcurs
    public static ArrayList<Long> docSizes(ArrayList<String> docNames) {
        ArrayList<Long> sizes = new ArrayList<>();
        for (String name : docNames) {
            File f = new File(name);
            sizes.add( f.length());
        }
        return sizes;
    }
}