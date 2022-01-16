package _04_Mapping_and_Outputting._01_Mapping_Operations;

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
            final JavaRDD<Integer> integerJavaRDD = javaSparkContext.parallelize(doubleList);
            final JavaRDD<Double> result = integerJavaRDD.map(value -> Math.sqrt(value));
            System.out.println("result=" + result.toString());
        } finally {
            if (javaSparkContext != null) {
                javaSparkContext.close();
            }

        }

    }
}
