package com.job.applicants.aptitude.test;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DeduplicatorProcessorTest {

    private TestRunner testRunner = TestRunners.newTestRunner(DeduplicatorProcessor.class);

    @Test
    @DisplayName("Processor should remove duplicates for new batch")
    void batchDeduplicateTest() {
        byte[] testData = "TAG1;1561025065;0.1;3\nTAG1;1561025065;0.1;3\nTAG2;1561025998;0.3;3".getBytes();
        testRunner.enqueue(testData);
        testRunner.run();
        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(DeduplicatorProcessor.SUCCESS);
        List<String> result = flowFiles.stream()
                .map(a -> new String(testRunner.getContentAsByteArray(a)))
                .map(a -> new LinkedList<String>(Arrays.asList(a.split("\n"))))
                .flatMap(List::stream)
                .collect(Collectors.toList());

        assertEquals(result.size(), 2);
    }

    @Test
    @DisplayName("Processor should remove duplicates for all new batches")
    void newBatchDeduplicateTest() {
        byte[] testData = "TAG2;1561025998;0.3;3\nTAG3;1561025998;0.9;2".getBytes();
        testRunner.enqueue(testData);
        testRunner.run();
        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(DeduplicatorProcessor.SUCCESS);
        List<String> result = flowFiles.stream()
                .map(a -> new String(testRunner.getContentAsByteArray(a)))
                .map(a -> new LinkedList<String>(Arrays.asList(a.split("\n"))))
                .flatMap(List::stream)
                .collect(Collectors.toList());

        assertEquals(result.size(), 1);
    }

}
