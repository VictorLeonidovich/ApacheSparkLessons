package _10_Sorts_and_Coalesce._01_Why_do_sorts_not_work_with_foreach_in_Spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class Main {
    public static void main(final String[] args) {

        System.setProperty("hadoop.home.dir", "C:/hadoop");

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        JavaSparkContext javaSparkContext = null;
        try {
            final SparkConf sparkConf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
            javaSparkContext = new JavaSparkContext(sparkConf);

            final JavaRDD<String> initialJavaRDD = javaSparkContext.textFile("src/main/resources/subtitles/input.txt");
            //final JavaRDD<String> initialJavaRDD = javaSparkContext.textFile("src/main/resources/subtitles/input-spring.txt");

            initialJavaRDD
                    .map(sentence -> sentence.replaceAll("[^a-zA-Z\\s]", "").toLowerCase())   //letters only
                    .filter(sentences -> sentences.trim().length() > 0)                                       //removed blank lines
                    .flatMap(sentence -> Arrays.asList(sentence.split(" ")).iterator())                 //just words
                    .filter(words -> words.trim().length() > 0)                                               //blank words removed
                    .filter(Util::isNotBoring)                                                                //just interesting words
                    .mapToPair(word -> new Tuple2<String, Long>(word, 1L))                                    //pair RDD
                    .reduceByKey((value1, value2) -> value1 + value2)                                         //totals
                    .mapToPair(tuple -> new Tuple2<Long, String>(tuple._2, tuple._1))                          //switched
                    .sortByKey(false)                                                                          //sorted
                    .coalesce(1)
                    .foreach(integer -> System.out.println(integer));
                    //.collect()
                    //.forEach(System.out::println);
                     /*take(10)
                    .forEach(System.out::println);*/

        } finally {
            if (javaSparkContext != null) {
                javaSparkContext.close();
            }

        }

    }
}
