package _06_PairRDDs._04_Using_the_Fluent_API;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class Main {
    public static void main(final String[] args) {
        final List<String> doubleList = new ArrayList<>();

        doubleList.add("WARN: Tuesday 4 September 0405");
        doubleList.add("ERROR: Tuesday 4 September 0408");
        doubleList.add("FATAL: Wednesday 5 September 1632");
        doubleList.add("ERROR: Friday 7 September 1854");
        doubleList.add("WARN: Saturday 8 September 1942");

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        JavaSparkContext javaSparkContext = null;
        try {
            final SparkConf sparkConf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
            javaSparkContext = new JavaSparkContext(sparkConf);
            javaSparkContext
                    .parallelize(doubleList)
                    .mapToPair(rawValue -> new Tuple2<>(rawValue.split(":")[0], 1L))
                    .reduceByKey((value1, value2) -> value1 + value2)
                    .foreach(tuple -> System.out.println(tuple._1 + " has " + tuple._2 + " instances."));

        } finally {
            if (javaSparkContext != null) {
                javaSparkContext.close();
            }

        }

    }
}
