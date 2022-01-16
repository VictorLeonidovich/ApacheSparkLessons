package _06_PairRDDs._02_Building_a_PairRDD;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
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
            final JavaRDD<String> originalLogMessagesRDD = javaSparkContext.parallelize(doubleList);

            final JavaPairRDD<String, String> javaPairRDD = originalLogMessagesRDD.mapToPair(rawValue -> {
                final String[] lines = rawValue.split(":");
                final String level = lines[0];
                final String date = lines[1];
                return new Tuple2<>(level, date);
            });

            javaPairRDD.collect().forEach(System.out::println);
        } finally {
            if (javaSparkContext != null) {
                javaSparkContext.close();
            }

        }

    }
}
