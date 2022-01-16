package _09_Keyword_Ranking_Practical._01_Practical_Requirements;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

public class Util {
    private static final Set<String> borings = new HashSet<>();
    static {
        final InputStream inputStream = Util.class.getResourceAsStream("/boringwords.txt");
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
