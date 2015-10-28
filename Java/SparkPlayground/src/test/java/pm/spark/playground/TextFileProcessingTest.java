package pm.spark.playground;

import org.apache.hadoop.mapred.InvalidInputException;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.net.URL;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class TextFileProcessingTest {

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
    @Ignore
    public void testClassGetResource() {
        // In src/test/resources/Sonnet30NotUsed.txt
        printResourcePath("1.1", "/Sonnet30NotUsed.txt"); // works
        printResourcePath("1.2", "Sonnet30NotUsed.txt");
        // In src/test/java/pm/spark/playground/Sonnet27NotUsed.txt
        printResourcePath("2.1", "pm/spark/playground/Sonnet27NotUsed.txt");
        printResourcePath("2.2", "/pm/spark/playground/Sonnet27NotUsed.txt");
        printResourcePath("2.3", "pm.spark.playground.Sonnet27NotUsed.txt");
        // In src/test/resources/pm/spark/playground/Sonnet18NotUsed.txt
        printResourcePath("3.1", "/pm/spark/playground/Sonnet18NotUsed.txt"); // works
        printResourcePath("3.2", "pm.spark.playground.Sonnet18NotUsed.txt");
        printResourcePath("3.3", "Sonnet18NotUsed.txt"); // works

        fail("Not all of these resources can be found");
    }

    private void printResourcePath(String text, String name) {
        System.out.println(text + " " + Optional.ofNullable(this.getClass().getResource(name))
                .map(URL::getPath)
                .orElse("Cannot find resource \"" + name + "\"")
        );
    }

    @Test
    public void processTextFileInProjectResourcesDirectory() {
        String path = this.getClass().getResource("Sonnet01.txt").getPath();
        JavaRDD<String> rdd = sc.textFile(path).cache();

        long count = rdd.filter(line -> line.contains("the")).count(); // Counts whole words only

        assertThat(count).isEqualTo(6);
    }

    @Test(expected = InvalidInputException.class)
    public void processTextFileInTestResourcesPackage() {
        String path = "Sonnet18NotUsed.txt"; // File in Sparkles/Java/SparkPlayground/src/test/java/pm/spark/playground/Sonnet18NotUsed.txt
        JavaRDD<String> rdd = sc.textFile(path).cache(); // Expects the file to be in .../Sparkles/Java/SparkPlayground/Sonnet18NotUsed.txt

        long count = rdd.filter(line -> line.contains("the")).count(); // Counts whole words only

        assertThat(count).isEqualTo(2);
    }

    @Test(expected = InvalidInputException.class)
    public void processTextFileInTestResources() {
        String path = "resources/Sonnet30NotUsed.txt"; // File in Sparkles/Java/SparkPlayground/src/test/resources/Sonnet30NotUsed.txt // sc.textFile(path) cannot find this
        JavaRDD<String> rdd = sc.textFile(path).cache(); // Expects the file to be in .../Sparkles/Java/SparkPlayground/resources/Sonnet30NotUsed.txt

        long count = rdd.filter(line -> line.contains("the")).count(); // Counts whole words only

        assertThat(count).isEqualTo(4);
    }
}
