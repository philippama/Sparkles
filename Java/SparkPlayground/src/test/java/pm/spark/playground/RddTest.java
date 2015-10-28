package pm.spark.playground;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.storage.StorageLevel;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class RddTest {

    private static JavaSparkContext sc;

    @BeforeClass
    static public void setUp() {
        SparkConf conf = new SparkConf()
                .setMaster(TestConfig.MASTER)
                .setAppName(TestConfig.APP_NAME);
        System.out.println("SparkConf: " + conf);
        sc = new JavaSparkContext(conf);
    }

    @Test
    public void scParallelize() {

        List<Integer> list = Arrays.asList(1, 2, 3, 4);
        JavaRDD<Integer> rdd = sc.parallelize(list);

        int total = rdd.reduce((a, b) -> a + b);

        assertThat(total).isEqualTo(10);
    }

    @Test
    public void simpleMapReduce_countingCharacters() {

        JavaRDD<String> rdd = getSonnet01Rdd();

        int totalLength = rdd
                .map(String::length)
                .reduce((a, b) -> a + b);

        assertThat(totalLength).isEqualTo(598);
    }

    @Test
    public void persistingInterimResult_countingCharactersAndNonSpaceCharacters() {

        JavaRDD<String> rdd = getSonnet01Rdd();

        JavaRDD<String> lowerCaseLines = rdd.map(String::toLowerCase);
        lowerCaseLines.persist(StorageLevel.MEMORY_ONLY());

        int totalLength = rdd
                .map(String::length)
                .reduce((a, b) -> a + b);

        int totalNonSpaceLength = lowerCaseLines
                .map(line -> line.replace(" ", ""))
                .map(String::length)
                .reduce((a, b) -> a + b);

        assertThat(totalLength).isEqualTo(598);
        assertThat(totalNonSpaceLength).isEqualTo(506);
    }

    @Test
    public void simpleIntegerAccumulator() {

        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4));
        Accumulator<Integer> integerAccumulator = sc.accumulator(0);

        rdd.foreach(integerAccumulator::add);

        int total = integerAccumulator.value();

        assertThat(total).isEqualTo(10);
    }

    @Test
    public void moreAccumulator() {

        // See http://spark.apache.org/docs/latest/programming-guide.html#accumulators-a-nameaccumlinka
        fail("Not yet written");
    }

    @Test
    public void flatMap_countingWords() {

        JavaRDD<String> rdd = getSonnet01Rdd();

        int totalWords = rdd
                .flatMap(line -> Arrays.asList(line.split("\\s+")))
                .map(word -> 1)
                .reduce((a, b) -> a + b);

        assertThat(totalWords).isEqualTo(106);
    }

    @Test
    public void flatMap_countingEvenLengthWords() {

        JavaRDD<String> rdd = getSonnet01Rdd();

        int totalWords = rdd
                .flatMap(line -> Arrays.asList(line.split("\\s+")))
                .filter(word -> word.length() % 2 == 0)
                .map(word -> 1)
                .reduce((a, b) -> a + b);

        assertThat(totalWords).isEqualTo(48);
    }

    @Test
    public void keyValuePairs_countingNumbersOfWordsOfEachLength_mapToPair_reduceByKey() {

        JavaRDD<String> rdd = getSonnet01Rdd();

        Map<Integer, Integer> wordLengthsWithCounts = rdd
                .flatMap(line -> Arrays.asList(line.split("\\s+")))
                .mapToPair(word -> new Tuple2<>(word.length(), 1))
                .reduceByKey((a, b) -> a + b)
                .collectAsMap();

        int totalWords = wordLengthsWithCounts.values().stream()
                .mapToInt(Integer::intValue)
                .sum();
        assertThat(totalWords).isEqualTo(106);

        assertThat(wordLengthsWithCounts.get(1)).isEqualTo(1);
        assertThat(wordLengthsWithCounts.get(2)).isEqualTo(10);
        assertThat(wordLengthsWithCounts.get(3)).isEqualTo(25);
        assertThat(wordLengthsWithCounts.get(4)).isEqualTo(20);
        assertThat(wordLengthsWithCounts.get(5)).isEqualTo(17);
        assertThat(wordLengthsWithCounts.get(6)).isEqualTo(13);
        assertThat(wordLengthsWithCounts.get(7)).isEqualTo(10);
        assertThat(wordLengthsWithCounts.get(8)).isEqualTo(3);
        assertThat(wordLengthsWithCounts.get(9)).isEqualTo(4);
        assertThat(wordLengthsWithCounts.get(10)).isEqualTo(1);
        assertThat(wordLengthsWithCounts.get(11)).isEqualTo(1);
        assertThat(wordLengthsWithCounts.get(16)).isEqualTo(1);
    }

    @Test
    public void keyValuePairs_countingNumbersOfWordsOfEachLength_flatMapToPair_reduceByKey() {

        fail("Not yet written");
    }

    @Test
    public void keyValuePairs_countingNumbersOfWordsOfEvenLength_mapToPair_filter_reduceByKey() {

        JavaRDD<String> rdd = getSonnet01Rdd();

        Map<Integer, Integer> wordLengthsWithCounts = rdd
                .flatMap(line -> Arrays.asList(line.split("\\s+")))
                .mapToPair(word -> new Tuple2<>(word.length(), 1))
                .filter(tuple -> tuple._1() % 2 == 0)
                .reduceByKey((a, b) -> a + b)
                .collectAsMap();

        int totalWords = wordLengthsWithCounts.values().stream()
                .mapToInt(Integer::intValue)
                .sum();
        assertThat(totalWords).isEqualTo(48);

        assertThat(wordLengthsWithCounts.get(2)).isEqualTo(10);
        assertThat(wordLengthsWithCounts.get(4)).isEqualTo(20);
        assertThat(wordLengthsWithCounts.get(6)).isEqualTo(13);
        assertThat(wordLengthsWithCounts.get(8)).isEqualTo(3);
        assertThat(wordLengthsWithCounts.get(10)).isEqualTo(1);
        assertThat(wordLengthsWithCounts.get(16)).isEqualTo(1);
    }

    @Test
    public void keyValuePairs_gettingMapOfWordsOfEachLength_mapToPair_combineByKey() {

        JavaRDD<String> rdd = getSonnet01Rdd();

        Function<String, HashSet<String>> createCombiner = value -> {
            HashSet<String> accumulator = new HashSet<>();
            accumulator.add(value);
            return accumulator;
        };
        Function2<HashSet<String>,String,HashSet<String>> mergeValue = (accumulator, value) -> {
                    accumulator.add(value);
                    return accumulator;
                };
        Function2<HashSet<String>, HashSet<String>, HashSet<String>> mergeCombiners = (accumulator1, accumulator2) -> {
                    accumulator1.addAll(accumulator2);
                    return accumulator1;
                };

        Map<Integer, HashSet<String>> wordsOfGivenLength = rdd
                .flatMap(line -> Arrays.asList(line.split("\\s+")))
                .mapToPair(word -> new Tuple2<>(word.length(), word))
                .combineByKey(
                        createCombiner,
                        mergeValue,
                        mergeCombiners
                )
                .collectAsMap();

        assertThat(wordsOfGivenLength.get(1)).containsExactly("a");
        assertThat(wordsOfGivenLength.get(2)).containsExactly("as", "or", "in", "by", "to", "To", "we");
        assertThat(wordsOfGivenLength.get(3)).containsExactly("But", "art", "thy", "Thy", "own", "be,", "the", "bud", "His", "his", "too", "And", "and", "now", "eat");
        assertThat(wordsOfGivenLength.get(4)).containsExactly("That", "foe,", "die,", "this", "And,", "From", "thou", "with", "that", "heir", "else", "due,", "only", "self", "rose", "bear", "time", "Pity", "Thou");
        assertThat(wordsOfGivenLength.get(5)).containsExactly("waste", "fuel,", "lies,", "grave", "might", "gaudy", "thee.", "eyes,", "never", "riper", "where", "flame", "sweet", "fresh", "thine");
        assertThat(wordsOfGivenLength.get(6)).containsExactly("tender", "cruel:", "churl,", "desire", "Making", "famine", "should", "bright", "herald", "Within", "world,", "mak'st");
        assertThat(wordsOfGivenLength.get(7)).containsExactly("Feed'st", "thereby", "world's", "light's", "glutton", "fairest", "spring,", "memory:", "buriest");
        assertThat(wordsOfGivenLength.get(8)).containsExactly("content,", "beauty's", "decease,");
        assertThat(wordsOfGivenLength.get(9)).containsExactly("increase,", "creatures", "ornament,", "abundance");
        assertThat(wordsOfGivenLength.get(10)).containsExactly("contracted");
        assertThat(wordsOfGivenLength.get(11)).containsExactly("niggarding:");
        assertThat(wordsOfGivenLength.get(16)).containsExactly("self-substantial");
    }

    private JavaRDD<String> getSonnet01Rdd() {
        String path = this.getClass().getResource("Sonnet01.txt").getPath();
        return sc.textFile(path).cache();
    }

}
