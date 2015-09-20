package pm.spark.playground;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


public class Playground {
    public static void main(String[] args) {
        String logFile = "resources/sonnet.txt";
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("Simple Application");
        System.out.println("SparkConf: " + conf);
        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        JavaRDD<String> logData = sparkContext.textFile(logFile).cache();

        long numAs = logData.filter(line -> line.contains("a")).count();
        long numBs = logData.filter(line -> line.contains("b")).count();

        System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
    }
}
