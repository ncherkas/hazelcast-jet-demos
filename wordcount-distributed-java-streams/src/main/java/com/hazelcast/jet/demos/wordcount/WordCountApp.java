package com.hazelcast.jet.demos.wordcount;

import com.hazelcast.jet.IListJet;
import com.hazelcast.jet.IMapJet;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.function.DistributedComparator;
import com.hazelcast.jet.stream.DistributedStream;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;
import java.util.stream.Stream;

import static com.hazelcast.jet.function.DistributedFunctions.wholeItem;
import static com.hazelcast.jet.stream.DistributedCollectors.toIMap;
import static com.hazelcast.jet.stream.DistributedCollectors.toList;
import static java.util.Objects.requireNonNull;

/**
 * java.util.stream API demo.
 */
public class WordCountApp {

    private static final String BOOK = "books/shakespeare-complete-works.txt";
    private static final Set<String> BLACKLIST = initBlacklist();
    private static final int ONE_MB = 1024 * 1024;
    private static final int BUFFER_CAPACITY = 10_000;

    public static void main(String[] args) {
        try {
            System.out.println("Running two Jet members...");

            Jet.newJetInstance();
            Jet.newJetInstance();

            JetInstance jetInstance = Jet.newJetClient();

            // Preparing the input
            System.out.println("Preparing the input...");
            IListJet<String> lines = jetInstance.getList("text");
            populateFromResource(lines, BOOK);
            System.out.println("Total lines: " + lines.size());

            // Step #1: Performing the java.util.stream computation
            System.out.println("Running the Jet java.util.stream job to compute counts...");
            IMapJet<String, Long> counts = DistributedStream.fromList(lines)
                    .flatMap(line -> Stream.of(line.toLowerCase().split("\\W+")))
                    .filter(word -> !word.isEmpty() && !BLACKLIST.contains(word))
                    .collect(toIMap("counts", wholeItem(), word -> 1L, (left, right) -> left + right));

            // Step #2: Checking the results, again using Jet for that
            System.out.println("Again running the Jet java.util.stream job to compute top N...");
            List<Map.Entry<String, Long>> top10 = DistributedStream.fromMap(counts)
                    .sorted((DistributedComparator<Map.Entry<String, Long>>) (e1, e2) -> e2.getValue().compareTo(e1.getValue()))
                    .limit(10)
                    .collect(toList());

            System.out.println("Top 10 word counts:");
            System.out.println(top10);
        } finally {
            Jet.shutdownAll();
        }
    }

    private static Set<String> initBlacklist() {
        Set<String> stopwords = new HashSet<>();
        populateFromResource(stopwords, "stopwords.txt");
        return Collections.unmodifiableSet(stopwords);
    }

    private static void populateFromResource(Collection<String> collection, String resourceName) {
        try (InputStream is = requireNonNull(WordCountApp.class.getClassLoader().getResourceAsStream(resourceName))) {
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(is), ONE_MB);

            List<String> buffer = new ArrayList<>(BUFFER_CAPACITY);
            for (String line; (line = bufferedReader.readLine()) != null;) {
                buffer.add(line);
                if (buffer.size() == BUFFER_CAPACITY) {
                    collection.addAll(buffer);
                    buffer.clear();
                }
            }

            if (!buffer.isEmpty()) {
                collection.addAll(buffer);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to read the resource", e);
        }
    }
}
