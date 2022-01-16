package _09_Keyword_Ranking_Practical._02_Worked_Solution;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

public class Util {
    private static final Set<String> borings = new HashSet<>();
    static {
        final InputStream inputStream = Util.class.getResourceAsStream("/subtitles/boringwords.txt");
        final BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
        bufferedReader.lines().forEach(it -> borings.add(it));
    }

    public static boolean isBoring(final String word){
        return borings.contains(word);
    }

    public static boolean isNotBoring(final String word){
        return !isBoring(word);
    }
}
