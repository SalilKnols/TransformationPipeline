package org.nashtech.com.io;

import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.values.PCollection;

public class FileProcessor {
    public PCollection<String> processFiles(PCollection<String> filePatterns) {
        return filePatterns
                .apply("Match Files", FileIO.matchAll())
                .apply("Read Matches", FileIO.readMatches())
                .apply("Read File Contents", TextIO.readFiles());
    }
}