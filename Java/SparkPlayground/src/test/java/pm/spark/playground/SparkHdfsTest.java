package pm.spark.playground;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

public class SparkHdfsTest {


    private static JavaSparkContext sc;
    private FileSystem fileSystem;
    private Path testDirectoryPath;
    private static final String TEST_FILE_NAME = "Colours.csv";

    @BeforeClass
    static public void setUp() throws Exception {
        SparkConf conf = new SparkConf()
                .setMaster(TestConfig.MASTER)
                .setAppName(TestConfig.APP_NAME);
        System.out.println("SparkConf: " + conf);
        sc = new JavaSparkContext(conf);
    }

    @Before
    public void setUpHadoopFileSystem() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.default.name", TestConfig.HDFS_URL);
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

        fileSystem = FileSystem.get(conf);

        testDirectoryPath = setUpTestDirectory(TestConfig.HDFS_DIRECTORY);

        copyResourceToHdfs(TEST_FILE_NAME, testDirectoryPath);
    }

    private Path setUpTestDirectory(String directory) throws IOException {
        Path path = new Path(directory);
        if (!fileSystem.isDirectory(path)) {
            fileSystem.mkdirs(path);
        }
        cleanDirectory(path);
        return path;
    }

    private void cleanDirectory(Path directoryPath) throws IOException {
        FileStatus[] fileStatuses = fileSystem.listStatus(directoryPath);
        for (FileStatus fileStatus : fileStatuses) {
            Path path = fileStatus.getPath();
            System.out.println("Deleting " + path);
            if (fileSystem.isDirectory(path)) {
                cleanDirectory(path);
            }
            fileSystem.delete(path, false);
        }
    }

    private void copyResourceToHdfs(String resourceName, Path destPath) throws IOException {
        Path sourcePath = new Path(this.getClass().getResource(resourceName).getPath());
        fileSystem.copyFromLocalFile(sourcePath, destPath);
    }

    @After
    public void tearDownHadoopFileSystem() throws IOException {
        fileSystem.close();
    }

    @Test
    public void processesFile() throws Exception {
        String sourcePath = TestConfig.HDFS_URL + TestConfig.HDFS_DIRECTORY + "/" + TEST_FILE_NAME;

        JavaRDD<String> rdd = sc.textFile(sourcePath);
        long count = rdd
                .filter(line -> line.startsWith("green"))
                .count();
        assertThat(count).isEqualTo(18);
    }

    @Test
    public void processesFileStoresResults() throws Exception {
        String destName = "Green.csv";

        String sourcePath = TestConfig.HDFS_URL + TestConfig.HDFS_DIRECTORY + "/" + TEST_FILE_NAME;
        String destPath = TestConfig.HDFS_URL + TestConfig.HDFS_DIRECTORY + "/" + destName;

        JavaRDD<String> rdd = sc.textFile(sourcePath);
        rdd
                .filter(line -> line.startsWith("green"))
                .saveAsTextFile(destPath);
        // This saves destPath as a directory containing files part-00000, part-00001 and _SUCCESS
        // _SUCCESS is an empty file
        // part-00000 contains 20 lines of the output
        // part-00001 contains 25 lines of the output
        // This is probably what we would usually want because Spark will read it in as one file (as below).
        // From reading around, I think map-reduce does too but I haven't tried that (yet).

        assertThat(sc.textFile(destPath).count()).isEqualTo(18);
    }

}
