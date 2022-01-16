package _05_Tuples._01_RDDs_of_Objects;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

public class Main {
    public static void main(final String[] args) {
        final List<Integer> doubleList = new ArrayList<>();
        doubleList.add(22);
        doubleList.add(41);
        doubleList.add(18);
        doubleList.add(90);

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        JavaSparkContext javaSparkContext = null;
        try {
            final SparkConf sparkConf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
            javaSparkContext = new JavaSparkContext(sparkConf);
            final JavaRDD<Integer> originalIntegerJavaRDD = javaSparkContext.parallelize(doubleList);
            final JavaRDD<IntegerWithSquareRoot> sqrtDoubleJavaRDD = originalIntegerJavaRDD.map(value -> new IntegerWithSquareRoot(value));



        } finally {
            if (javaSparkContext != null) {
                javaSparkContext.close();
            }

        }

    }
}
