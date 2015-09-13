SimpleApp
=========
From the tutorial in http://spark.apache.org/docs/latest/quick-start.html

For SBT, which is needed to build the application, see http://www.scala-sbt.org/ 

To build from the SimpleApp directory:
```
sbt package
```
The first time this ran it took quite a long time downloading dependencies, kept stopping on "Waiting for lock on &lt;home directory&gt;/.ivy2/.sbt.ivy.lock to be available..."
It created:
```
target/scala-2.11/simpleapp_2.11-1.0.jar
```
To run, if SPARK_HOME is set to be your Spark installation directory:
```
$SPARK_HOME/bin/spark-submit --class "SimpleApp" --master local[4] target/scala-2.11/simpleapp_2.11-1.0.jar
```
