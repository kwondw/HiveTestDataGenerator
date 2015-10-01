package kwon.dongwook;


import org.apache.hadoop.conf.Configuration;

import java.util.Map;

public class Util {

    public static void psa(String[] array, boolean deco) {
        if (deco) {
            System.out.println("=============== Start print String Array : " + array + " ===================");
        }
        for(int i = 0; i < array.length; i++) {
            System.out.println(i + ": " + array[i].toString());
        }
    }

    public static void psa(String[] array) {
        psa(array, true);
    }

    public static void p(String msg) {
        System.out.println(msg);
    }

    private static String quotes = "\"'";
    public static String removeQuote(String string) {
        if(string.charAt(0) == quotes.charAt(0) || string.charAt(string.length() - 1) == quotes.charAt(0)) {
            string = string.replaceAll("\"", "");
        } else if(string.charAt(0) == quotes.charAt(1) || string.charAt(string.length() - 1) == quotes.charAt(1)) {
            string = string.replaceAll("'", "");
        }
        return string;
    }

    private void printConf(Configuration conf) {
        for (Map.Entry<String, String> entry: conf) {
            System.out.printf("%s=%s\n", entry.getKey(), entry.getValue());
        }
    }
}
