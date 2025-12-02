# Introduction
This project implements a simplified version of the Unix grep command in Java. The application recursively searches a directory, reads each file line-by-line, applies a user-defined regex pattern, and writes all matching lines to an output file. The project demonstrates several core software engineering concepts including Java OOP design, Java I/O, regular expressions, lambda & Stream APIs, Maven dependency management, SLF4J logging, and Docker containerization. Multiple implementations were created: a classic Java version and a modern Stream-based version optimized for large datasets.

# Quick Start
### 1. Compile and Package
```bash
cd core_java/grep
mvn clean package
```
### 2. Run the Application (Jar mode)
```bash
java -jar target/grep-1.0-SNAPSHOT.jar ".*Romeo.*Juliet.*" ./data ./out/grep.txt
```

### 3. Run with Docker
```bash
docker run --rm \
   -v `pwd`/data:/data \
   -v `pwd`/log:/log \
   your_docker_username/grep ".*Romeo.*Juliet.*" /data /log/grep.out
```


   

# Implemenation
## Pseudocode
Below is the high-level pseudocode for the process() method:
```
matchedLines = empty list

for file in listFiles(rootDir):
    for line in readLines(file):
        if containsPattern(line):
            add line to matchedLines

writeToFile(matchedLines)
```
Stream-based implementation (lazy evaluation):
```
listFiles(rootDir)
    .flatMap(file -> readLines(file))
    .filter(line -> containsPattern(line))
    .forEach(line -> writeLine(outputFile, line))
```

## Performance Issue
The original implementation of the grep app used `List<String>` inside `readLines()`, which loads the *entire file* into memory before processing. This approach has a time complexity of **O(n)** for scanning the file, but more importantly, it has **O(n) space complexity**, because every line is stored in RAM simultaneously. For small text files, this is fine, but for large inputs (e.g., 10GB to 50GB), the JVM heap is quickly exhausted, causing `OutOfMemoryError` or constant garbage collection (GC thrashing), which slows the application dramatically.

The Stream/Lambda implementation solves this by using `BufferedReader` or `Files.lines()`, which processes the input **lazily** and keeps only *one line in memory at a time*. This reduces space complexity from **O(n)** to **O(1)** and allows the application to handle arbitrarily large files within a tiny heap (10 to 20MB). The time complexity remains **O(n)** but performance improves because no large list allocation, resizing, or GC overhead is required. As a result, the stream-based grep is more memory-efficient, scalable, and suitable for real-world big-data workloads.

# Testing
To test the application manually, I carried out the following steps:

- Prepared sample text files in data/.

- Ran the application with different regex patterns such as:

  - "Romeo"

  - ".*Juliet.*"

  - ".*love.*hate.*"

- Verified the results by:

  - Viewing output files (cat out/grep.txt)
  - Manually comparing matched lines with known text sources

- Created custom test files containing lines purposely designed to match or not match the pattern.

- For Docker testing:

  - Mounted data/ and log/ directories using -v 
  - Ensured grep.out was created properly inside /log/
# Deployment
To simplify distribution, the application was containerized using Docker:

Dockerfile:
```dockerfile
FROM eclipse-temurin:8-jre-alpine

RUN mkdir -p /usr/local/app/grep/lib/

COPY target/grep-1.0-SNAPSHOT.jar /usr/local/app/grep/lib/app.jar
COPY target/dependency/*.jar /usr/local/app/grep/lib/

ENTRYPOINT ["java", "-cp", "/usr/local/app/grep/lib/*", "ca.jrvs.apps.grep.JavaGrepImp"]
```

Build & Push (use correct docker username):
```bash
docker build -t your_docker_username/grep .
docker push your_docker_username/grep
```
Running the Docker image mounts local data folders and executes the grep app inside a lightweight JRE environment.
# Improvements
- Stream all I/O completely:
The first thing I'd improve is replace all List<String> returns in the original interface with Stream<String> to fully support huge file processing.

- Parallel processing:
We can also use parallel Stream pipelines or a thread-pool executor to process files concurrently for better performance on multi-core machines.

- Support more features of UNIX grep which could add flags such as:

  - -i for case-insensitive search 
  - -v for inverted match 
  - -n to show line numbers 
  - -r optional recursion