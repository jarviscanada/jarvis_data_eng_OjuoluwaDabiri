package ca.jrvs.apps.grep;

import java.io.File;
import java.io.IOException;
import java.util.List;

public interface JavaGrep {

    /**
     * Top level search workflow
     * @throws IOException
     */
    void process() throws IOException;

    /**
     *
     * @param rootDir
     * @return files under the rootDir
     */
    List<File> listFiles(String rootDir);


    /**
     *
     * @param inputFIle
     * @return lines
     * @throws IllegalArgumentException if a given file is not a file
     */
    List<String> readLines(File inputFile);


    /**
     *
     * @param line input string
     * @return true if there is a match
     */
    boolean containsPattern(String line);


    /**
     *
     * @param lines matched line
     * @throws IOException if write failed
     */
    void writeToFile(List<String> lines) throws IOException;

    String getRootPath();

    void setRootPath(String rootPath);

    String getRegex();

    void setRegex(String regex);

    String getOutFile();

    void setOutFile(String outFile);
}
