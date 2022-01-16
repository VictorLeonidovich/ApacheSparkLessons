package _04_Mapping_and_Outputting._02_Outputting_Results_to_the_Console;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.io.Serializable;
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

            //result.foreach(value -> System.out.println("value=" + value));
            //or

            /*class MyFunction<Double> implements Serializable, VoidFunction<Double> {

                @Override
                public void call(Double value) throws Exception {
                    System.out.println("value=" + value);
                }
            }
            result.foreach(new MyFunction<>());*/
            //or

            result.foreach(System.out::println);// TODO 21/11/22 21:59:56 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
                                                //Exception in thread "main" org.apache.spark.SparkException: Task not serializable
                                                //Caused by: java.io.NotSerializableException: java.io.PrintStream
                                                //Serialization stack:
                                                //	- object not serializable (class: java.io.PrintStream, value: java.io.PrintStream@3163987e)

        } finally {
            if (javaSparkContext != null) {
                javaSparkContext.close();
            }

        }

    }
}
