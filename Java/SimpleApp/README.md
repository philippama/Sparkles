SimpleApp
=========
From the tutorial in http://spark.apache.org/docs/latest/quick-start.html

For Gradle, which is needed to build the application, see https://gradle.org/ 

To build from the SimpleApp directory:
```
./gradlew build
```
It created:
```
build/libs/SimpleApp-1.0-SNAPSHOT.jar
```
To run, if SPARK_HOME is set to be your Spark installation directory:
```
$SPARK_HOME/bin/spark-submit --class "pm.spark.simpleapp.SimpleApp" --master local[4] build/libs/SimpleApp-1.0-SNAPSHOT.jar
```
