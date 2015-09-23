package pm.spark.playground;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.BeforeClass;
import org.junit.Test;

public class TextFileProcessingTest {

    private static JavaSparkContext sc;

    private static final String TEXT_FILE = "resources/sonnet.txt";

    @BeforeClass
    static public void setUp() {
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("Simple Application");
        System.out.println("SparkConf: " + conf);
        sc = new JavaSparkContext(conf);
    }

    @Test
    public void test() {
        JavaRDD<String> logData = sc.textFile(TEXT_FILE).cache();

        long numAs = logData.filter(line -> line.contains("a")).count();
        long numBs = logData.filter(line -> line.contains("b")).count();

        System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);

    }
}
