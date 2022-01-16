package _03_01_Reduces_on_RDDs;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

public class Main {
    public static void main(final String[] args) {
        final List<Double> doubleList = new ArrayList<>();
        doubleList.add(22.32);
        doubleList.add(41.999995);
        doubleList.add(18.16);
        doubleList.add(90.35);

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        JavaSparkContext javaSparkContext = null;
        try {
            final SparkConf sparkConf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
            javaSparkContext = new JavaSparkContext(sparkConf);
            final JavaRDD<Double> doubleJavaRDD = javaSparkContext.parallelize(doubleList);
            final Double result = doubleJavaRDD.reduce((value1, value2) -> value1 + value2);
            System.out.println("result=" + result);
        } finally {
            if (javaSparkContext != null) {
                javaSparkContext.close();
            }

        }

    }
}
