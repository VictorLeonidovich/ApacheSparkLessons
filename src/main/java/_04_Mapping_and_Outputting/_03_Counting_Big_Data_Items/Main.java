package _04_Mapping_and_Outputting._03_Counting_Big_Data_Items;

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

            System.out.println("count=" + integerJavaRDD.count());

            final JavaRDD<Long> integerJavaRDD1 = integerJavaRDD.map(value -> 1L);
            final Long count = integerJavaRDD1.reduce((value1, value2) -> value1 + value2);
            System.out.println("count2=" + count);

        } finally {
            if (javaSparkContext != null) {
                javaSparkContext.close();
            }

        }

    }
}
