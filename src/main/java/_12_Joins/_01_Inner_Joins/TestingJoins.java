package _12_Joins._01_Inner_Joins;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class TestingJoins {
    public static void main(final String[] args) {

        System.setProperty("hadoop.home.dir", "C:/hadoop");
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        JavaSparkContext javaSparkContext = null;
        try {
            final SparkConf sparkConf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
            javaSparkContext = new JavaSparkContext(sparkConf);

            final List<Tuple2<Integer, Integer>> visitsRaw = new ArrayList<>();
            visitsRaw.add(new Tuple2<>(4, 18));
            visitsRaw.add(new Tuple2<>(6, 4));
            visitsRaw.add(new Tuple2<>(10, 9));

            final List<Tuple2<Integer, String>> usersRaw = new ArrayList<>();
            usersRaw.add(new Tuple2<>(1, "John"));
            usersRaw.add(new Tuple2<>(2, "Bob"));
            usersRaw.add(new Tuple2<>(3, "Alan"));
            usersRaw.add(new Tuple2<>(4, "Doris"));
            usersRaw.add(new Tuple2<>(5, "Marybelle"));
            usersRaw.add(new Tuple2<>(6, "Raquel"));

            final JavaPairRDD<Integer, Integer> visits = javaSparkContext.parallelizePairs(visitsRaw);
            final JavaPairRDD<Integer, String> users = javaSparkContext.parallelizePairs(usersRaw);

            final JavaPairRDD<Integer, Tuple2<Integer, String>> joinedRDD = visits.join(users);
            joinedRDD.foreach(result -> System.out.println(result));

        } finally {
            if (javaSparkContext != null) {
                javaSparkContext.close();
            }

        }

    }
}
