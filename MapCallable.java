import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.Callable;

public class MapCallable implements Callable {

    String docName;
    int offset;
    int fragmentSize;

    public MapCallable(String docName, int offset, int fragmentSize) {
        this.docName = docName;
        this.offset = offset;
        this.fragmentSize = fragmentSize;
    }

    @Override
    public MapResult call() throws Exception {
        HashMap<Integer, Integer> dictionary = new HashMap<>();
        ArrayList<String> wordList = new ArrayList<>();

        StringBuilder text = parseFile();

        // se imparte textul in cuvinte
        String[] words = text.toString().split("[^A-Za-z0-9]");

        int maxLength = -1;
        // creare dictionar si lista de cuvinte cu lungime maximala
        for (String w : words) {
            if (w.length() == 0) continue; // "cuvant" cu lungime 0 => se trece mai departe
            if (!dictionary.containsKey(w.length())) { // cheie noua in dictionar
                dictionary.put(w.length(), 1);
            } else {
                dictionary.put(w.length(), dictionary.get(w.length()) + 1); // actualizare cheie
            }
            if (w.length() >= maxLength) {
                maxLength = w.length();
                wordList.add(w);
            }
        }

        int finalMaxLength = maxLength;
        wordList.removeIf(e -> e.length() < finalMaxLength); // stergere cuvinte cu lungime mai mica decat maxLength

        return new MapResult(docName, dictionary, wordList);
    }

    // functie care parcurge (de la offset-ul corespunzator) fisierul primit in constructor
    private StringBuilder parseFile() throws IOException {
        StringBuilder word = new StringBuilder();
        RandomAccessFile file = null;
        try {
            file = new RandomAccessFile(new File(docName), "r");

            int charactersWritten = 0;
            if (offset == 0) { // inceput de fisier => luam caractere doar din dreapta
                int c = file.read();
                while (c != -1) { // mai exista caractere in fisier
                    if (charactersWritten < fragmentSize - 1) { // mai am de scris
                        word.append((char) c);
                        charactersWritten++;
                    } else {
                        if (Character.isAlphabetic((char) c)) { // ma aflu in mijlocul cuvantului => pun restul cuvantului
                            word.append((char) c);
                            charactersWritten++;

                        } else {
                            break;
                        }
                    }
                    c = file.read();
                }
            } else { // offset != 0 => interiorul fisierului => verificam daca ne aflam in interiorul unui cuvant
                file.seek(offset-1);
                int c = file.read();
                while ( Character.isAlphabetic((char)c) && c != -1) { // ma aflu in mijlocul cuvantului => dau skip
                    c = file.read();
                    charactersWritten++;
                }
                while (c != -1) { // mai exista caractere in fisier
                    if (charactersWritten < fragmentSize) { // mai am de scris
                        word.append((char) c);
                        charactersWritten++;
                    } else {
                        if (Character.isAlphabetic((char) c)) { // ma aflu in mijlocul cuvantului => pun restul
                            word.append((char) c);
                            charactersWritten++;
                        } else {
                            break;
                        }
                    }
                    c = file.read();
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        assert file != null;
        file.close();
        return word;
    }
}

class MapResult {
    String docName;
    HashMap<Integer, Integer> dictionary;
    ArrayList<String> wordList;

    public MapResult(String docName, HashMap<Integer, Integer> dictionary, ArrayList<String> wordList) {
        this.docName = docName;
        this.dictionary = dictionary;
        this.wordList = wordList;
    }
}
