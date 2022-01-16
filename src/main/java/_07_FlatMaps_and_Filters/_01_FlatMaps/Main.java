package _07_FlatMaps_and_Filters._01_FlatMaps;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Main {
    public static void main(final String[] args) {
        final List<String> logs = new ArrayList<>();

        logs.add("WARN: Tuesday 4 September 0405");
        logs.add("ERROR: Tuesday 4 September 0408");
        logs.add("FATAL: Wednesday 5 September 1632");
        logs.add("ERROR: Friday 7 September 1854");
        logs.add("WARN: Saturday 8 September 1942");

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        JavaSparkContext javaSparkContext = null;
        try {
            final SparkConf sparkConf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
            javaSparkContext = new JavaSparkContext(sparkConf);
            javaSparkContext
                    .parallelize(logs)                                                   //sentences
                    .flatMap(value -> Arrays.asList(value.split(" ")).iterator())  //words
                    .collect()
                    .forEach(System.out::println);

        } finally {
            if (javaSparkContext != null) {
                javaSparkContext.close();
            }

        }

    }
}
