package com.kiona.ad_analysis.util;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author yangshuaichao
 */
public class CsvUtil {

    public static List<List<String>> read(String filePath, int startRow) throws IOException {
        List<List<String>> records = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new InputStreamReader(Files.newInputStream(Paths.get(filePath)), "unicode"))) {
            int row = 0;
            String line;
            while ((line = br.readLine()) != null) {
                row ++;
                if(row < startRow){
                    continue;
                }
                String[] values = line.split("\t");
                records.add(Arrays.asList(values));
            }
        }
        return records;
    }

}
