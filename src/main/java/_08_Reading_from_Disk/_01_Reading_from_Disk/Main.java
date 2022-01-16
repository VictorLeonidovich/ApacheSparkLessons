package _08_Reading_from_Disk._01_Reading_from_Disk;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Main {
    public static void main(final String[] args) {

        System.setProperty("hadoop.home.dir", "C:/hadoop");

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        JavaSparkContext javaSparkContext = null;
        try {
            final SparkConf sparkConf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
            javaSparkContext = new JavaSparkContext(sparkConf);

            final JavaRDD<String> initialJavaRDD = javaSparkContext.textFile("src/main/resources/subtitles/input.txt");

            initialJavaRDD                                                                //setnences
                    .flatMap(value -> Arrays.asList(value.split(" ")).iterator())   //words
                    .collect()
                    .forEach(System.out::println);

        } finally {
            if (javaSparkContext != null) {
                javaSparkContext.close();
            }

        }

    }
}
