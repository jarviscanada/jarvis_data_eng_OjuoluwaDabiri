package ca.jrvs.apps.practice;

import java.util.regex.Pattern;


public class RegexExcImp implements RegexExc {

    @Override
    public boolean matchJpeg(String filename) {
        if (filename == null) return false;
        // Regex: ends with .jpg or .jpeg (case insensitive)
        return Pattern.compile("(?i)^.+\\.(jpg|jpeg)$").matcher(filename).matches();
    }

    @Override
    public boolean matchIp(String ip) {
        if (ip == null) return false;
        // Regex: simplified range 0.0.0.0 to 999.999.999.999
        String ipRegex = "^(\\d{1,3}\\.){3}\\d{1,3}$";
        return Pattern.matches(ipRegex, ip);
    }

    @Override
    public boolean matchisEmptyLine(String line) {
        if (line == null) return false;
        // Regex: line contains only whitespace or nothing
        return Pattern.matches("^\\s*$", line);
    }

    // Optional main() for testing
    public static void main(String[] args) {
        RegexExcImp test = new RegexExcImp();

        System.out.println("matchJpeg Tests:");
        System.out.println(test.matchJpeg("photo.jpg"));   // true
        System.out.println(test.matchJpeg("photo.jpeg"));  // true
        System.out.println(test.matchJpeg("photo.png"));   // false

        System.out.println("\nmatchIp Tests:");
        System.out.println(test.matchIp("192.168.0.1"));   // true
        System.out.println(test.matchIp("999.999.999.999"));// true
        System.out.println(test.matchIp("192.168.0"));     // false

        System.out.println("\nisEmptyLine Tests:");
        System.out.println(test.matchisEmptyLine(""));           // true
        System.out.println(test.matchisEmptyLine("   "));        // true
        System.out.println(test.matchisEmptyLine("hello"));      // false
    }
}
