package _09_Keyword_Ranking_Practical._02_Worked_Solution;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

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

            initialJavaRDD
                    .map(sentence -> sentence.replaceAll("[^a-zA-Z\\s]", "").toLowerCase())   //letters only
                    .filter(sentences -> sentences.trim().length() > 0)                                       //removed blank lines
                    .flatMap(sentence -> Arrays.asList(sentence.split(" ")).iterator())                 //just words
                    .filter(Util::isNotBoring)                                                                //just interesting words
                    .take(50)
                    .forEach(System.out::println);

        } finally {
            if (javaSparkContext != null) {
                javaSparkContext.close();
            }

        }

    }
}
