package edu.fit.cse5310;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MiscUtils {
    public static String[] fieldsFromLine(String line) {
        String[] fields = line.toString().split(",");
//        for (int i = 0; i < fields.length; i++) {
//            fields[i] = fields[i].replace("\"", "");
//        }
        return fields;
    }

    // Sorting function found on stackoverflow.com
    public static List<Map.Entry<String, Integer>> getTopTen(Map<String, Integer> map) {
        List<Map.Entry<String, Integer>> entries = new ArrayList<>(map.entrySet());
        entries.sort(Map.Entry.<String, Integer>comparingByValue().reversed());
        return entries.size() > 10 ? entries.subList(0, 10) : entries;
    }
}
