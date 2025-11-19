package ca.jrvs.apps.grep;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JavaGrepImp implements JavaGrep {

    final Logger logger = LoggerFactory.getLogger(JavaGrepImp.class);

    private String regex;
    private String rootPath;
    private String outFile;



    public static void main(String[] args) {
        if (args.length != 3) {
            throw new IllegalArgumentException("USAGE: JavaGrep regex rootPath outFile");
        }

        // default logger config (optional if Maven already manages log4j)
        //BasicConfigurator.configure();

        JavaGrepImp javaGrep = new JavaGrepImp();
        javaGrep.setRegex(args[0]);
        javaGrep.setRootPath(args[1]);
        javaGrep.setOutFile(args[2]);

        try {
            javaGrep.process();
            javaGrep.logger.info("SUCCESS!");
        } catch (Exception e) {
            javaGrep.logger.error("Error: Unable to process grep job", e);
        }
    }


    @Override
    public void process() throws IOException {
        List<String> matchedLines = new ArrayList<>();

        for (File file : listFiles(getRootPath())) {
            for (String line : readLines(file)) {
                if (containsPattern(line)) {
                    matchedLines.add(line);
                }
            }
        }

        writeToFile(matchedLines);
    }

    @Override
    public List<File> listFiles(String rootDir) {
        List<File> files = new ArrayList<>();
        File root = new File(rootDir);

        if (root.isFile()) {
            files.add(root);
        } else if (root.isDirectory()) {
            for (File file : root.listFiles()) {
                files.addAll(listFiles(file.getAbsolutePath()));
            }
        }

        return files;
    }

    @Override
    public List<String> readLines(File inputFile) {
        List<String> lines = new ArrayList<>();

        try (BufferedReader reader = new BufferedReader(new FileReader(inputFile))) {
            String line;
            while ((line = reader.readLine()) != null) {
                lines.add(line);
            }
        } catch (IOException e) {
            logger.error("Error reading file: " + inputFile.getAbsolutePath(), e);
        }

        return lines;
    }

    @Override
    public boolean containsPattern(String line) {
        return Pattern.compile(getRegex()).matcher(line).find();
    }

    @Override
    public void writeToFile(List<String> lines) throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(getOutFile()))) {
            for (String line : lines) {
                writer.write(line);
                writer.newLine();
            }
        }
    }

    @Override
    public String getRootPath() {
        return rootPath;
    }

    @Override
    public void setRootPath(String rootPath) {
        this.rootPath = rootPath;
    }

    @Override
    public String getRegex() {
        return regex;
    }

    @Override
    public void setRegex(String regex) {
        this.regex = regex;
    }

    @Override
    public String getOutFile() {
        return outFile;
    }

    @Override
    public void setOutFile(String outFile) {
        this.outFile=outFile;
    }
}
