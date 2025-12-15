package ca.jrvs.apps.grep;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.*;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class JavaGrepLambdaImp extends JavaGrepImp {

    private final Logger logger = LoggerFactory.getLogger(JavaGrepLambdaImp.class);

    @Override
    public void process() throws IOException {
        // walkthrought files with stream
        try (Stream<Path> fileStream = Files.walk(Paths.get(getRootPath()))) {
            List<String> matchedLines = fileStream
                    .filter(Files::isRegularFile)
                    .flatMap(this::readLinesStream)
                    .filter(this::containsPattern)
                    .collect(Collectors.toList());

            writeToFile(matchedLines);
        }

    }

    // return a Stream<String> instead of a List<String>
    private Stream<String> readLinesStream(Path path) {
        try {
            return Files.lines(path);
        } catch (IOException e) {
            logger.error("Error reading file: {}", path, e);
            return Stream.empty();
        }
    }

    @Override
    public boolean containsPattern(String line) {
        return Pattern.compile(getRegex()).matcher(line).find();
    }

    @Override
    public void writeToFile(List<String> lines) throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(getOutFile()))) {
            lines.forEach(line -> {
                try {
                    writer.write(line);
                    writer.newLine();
                } catch (IOException e) {
                    logger.error("Error writing line", e);
                }
            });
        }
    }

    //Main with process
    public static void main(String[] args) {
        if (args.length != 3) {
            throw new IllegalArgumentException("USAGE: JavaGrepLambdaImp regex rootPath outFile");
        }

        JavaGrepLambdaImp grep = new JavaGrepLambdaImp();
        grep.setRegex(args[0]);
        grep.setRootPath(args[1]);
        grep.setOutFile(args[2]);

        try {
            grep.process();
            grep.logger.info("SUCCESS!");
        } catch (Exception e) {
            grep.logger.error("Error running JavaGrepLambdaImp", e);
        }
    }
}
