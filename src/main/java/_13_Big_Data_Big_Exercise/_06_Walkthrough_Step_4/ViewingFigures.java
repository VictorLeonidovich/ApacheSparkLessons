package _13_Big_Data_Big_Exercise._06_Walkthrough_Step_4;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class ViewingFigures {
    public static void main(final String[] args) {
        System.setProperty("hadoop.home.dir", "c:/hadoop");
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        final SparkConf sparkConf = new SparkConf().setAppName("starting_Spark").setMaster("local[*]");
        try (final JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf)) {

            //use true to use hardcoded data identical to that in the PDF guide.
            final boolean testMode = true;

            JavaPairRDD<Integer, Integer> viewData = setUpViewDataRDD(javaSparkContext, testMode);
            final JavaPairRDD<Integer, Integer> chapterData = setUpChapterDataRDD(javaSparkContext, testMode);
            final JavaPairRDD<Integer, String> titlesData = setUpTitlesDataRDD(javaSparkContext, testMode);

            //TODO - over to me

            //Warmup

            final JavaPairRDD<Integer, Integer> chapterCountRDD = chapterData
                    .mapToPair(row -> new Tuple2<Integer, Integer>(row._2, 1))
                    .reduceByKey((value1, value2) -> value1 + value2);

            //chapterCountRDD.foreach(toPrint -> System.out.println(toPrint));

            //Step 1 - Remove any duplicated views
            viewData = viewData.distinct();

            //Step 2 - Get the course IDs into RDD
            viewData = viewData.mapToPair(row -> new Tuple2<>(row._2, row._1));
            final JavaPairRDD<Integer, Tuple2<Integer, Integer>> joinedRDD = viewData.join(chapterData);

            //Step 3 - Don't need chapted IDs, setting up for the reduce
            final JavaPairRDD<Tuple2<Integer, Integer>, Long> step3 = joinedRDD.mapToPair(row -> {
                final Integer userId = row._2._1;
                final Integer cousreId = row._2._2;
                return new Tuple2<>(new Tuple2<>(userId, cousreId), 1L);
            });

            //Step 4 - Count now many views for each user per course
            final JavaPairRDD<Tuple2<Integer, Integer>, Long> reduced = step3.reduceByKey((value1, value2) -> value1 + value2);
            reduced.foreach(toPrint -> System.out.println(toPrint));
        }


    }

    private static JavaPairRDD<Integer, Integer> setUpViewDataRDD(final JavaSparkContext javaSparkContext, final boolean testMode) {
        if (testMode){
            // Chapter Views - (userId, chapterID)
            final List<Tuple2<Integer, Integer>> rawViewData = new ArrayList<>();
            rawViewData.add(new Tuple2<>(14, 96));
            rawViewData.add(new Tuple2<>(14, 97));
            rawViewData.add(new Tuple2<>(13, 96));
            rawViewData.add(new Tuple2<>(13, 96));
            rawViewData.add(new Tuple2<>(13, 96));
            rawViewData.add(new Tuple2<>(14, 99));
            rawViewData.add(new Tuple2<>(13, 100));

            return javaSparkContext.parallelizePairs(rawViewData);
        }
        return getDataFromFile(javaSparkContext, "src/main/resources/viewing_figures/views-*.csv");
    }

    private static JavaPairRDD<Integer, Integer> setUpChapterDataRDD(final JavaSparkContext javaSparkContext, final boolean testMode) {
        if (testMode){
            // (chapterId,(courseId, Title)
            final List<Tuple2<Integer, Integer>> rawChapterData = new ArrayList<>();
            rawChapterData.add(new Tuple2<>(96, 1));
            rawChapterData.add(new Tuple2<>(97, 1));
            rawChapterData.add(new Tuple2<>(98, 1));
            rawChapterData.add(new Tuple2<>(99, 2));
            rawChapterData.add(new Tuple2<>(100, 3));
            rawChapterData.add(new Tuple2<>(101, 3));
            rawChapterData.add(new Tuple2<>(102, 3));
            rawChapterData.add(new Tuple2<>(103, 3));
            rawChapterData.add(new Tuple2<>(104, 3));
            rawChapterData.add(new Tuple2<>(105, 3));
            rawChapterData.add(new Tuple2<>(106, 3));
            rawChapterData.add(new Tuple2<>(107, 3));
            rawChapterData.add(new Tuple2<>(108, 3));
            rawChapterData.add(new Tuple2<>(109, 3));

            return javaSparkContext.parallelizePairs(rawChapterData);
        }
        return getDataFromFile(javaSparkContext, "src/main/resources/viewing_figures/chapters.csv");
    }

    private static JavaPairRDD<Integer, String> setUpTitlesDataRDD(final JavaSparkContext javaSparkContext, final boolean testMode) {
        if (testMode) {
            // (chapterId, Title)
            final List<Tuple2<Integer, String>> rawTitlesData = new ArrayList<>();
            rawTitlesData.add(new Tuple2<>(1, "How to find a better job"));
            rawTitlesData.add(new Tuple2<>(2, "Work faster harder smarter until you drop"));
            rawTitlesData.add(new Tuple2<>(3, "Content creation is Mug's Game"));
            return javaSparkContext.parallelizePairs(rawTitlesData);
        }
        return javaSparkContext
                .textFile("src/main/resources/viewing_figures/titles.csv")
                .mapToPair(commaSeparatedLine -> {final String[] cols = commaSeparatedLine.split(",");
                    return new Tuple2<>(new Integer(cols[0]), cols[1]);});
    }

    private static JavaPairRDD<Integer, Integer> getDataFromFile(final JavaSparkContext javaSparkContext, final String fileName) {
        return javaSparkContext
                .textFile(fileName)
                .mapToPair(commaSeparatedLine -> {final String[] cols = commaSeparatedLine.split(",");
                    return new Tuple2<>(new Integer(cols[0]), new Integer(cols[1]));});
    }
}
