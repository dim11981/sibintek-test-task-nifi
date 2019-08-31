package com.job.applicants.aptitude.test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;

@Tags({"DEDUPLICATE"})
@CapabilityDescription("All duplicates dead here")
public class DeduplicatorProcessor extends AbstractProcessor {

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("SUCCESS")
            .description("Success relationship")
            .build();

    public static final Relationship FAILURE = new Relationship.Builder()
            .name("FAILURE")
            .description("FAILURE relationship")
            .build();

    public static final Set<Relationship> RELATIONSHIPS = Collections.unmodifiableSet(
            new HashSet<>(Arrays.asList(SUCCESS, FAILURE)));

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        //implement your code here
        FlowFile flowFile = session.get();

        if (flowFile == null) {
            return;
        }

        // простой файловый кэш
        CacheSetOperation cache = new SimpleFileCache();

        // словарь для хранения строк, переживших когда-либо deduplication
        final Set<String> dict = cache.load();
        // список строк из текущего ff, переживших deduplication
        final List<String> splits = new LinkedList<>();

        session.read(flowFile, (final InputStream rawIn) -> {
            try (final BufferedReader reader = new BufferedReader(new InputStreamReader(rawIn))) {
                /**
                 * построчное чтение содержимого ff
                 * пустые строки не добавляются в список
                 * строки, уже находящиеся в кэше, не добавляются в список
                 */
                String split = reader.readLine();
                while (split != null) {
                    if (!split.isEmpty() && dict.add(split)) {
                        splits.add(split);
                    }
                    split = reader.readLine();
                }
            }
            // обновить кэш
            cache.save(dict);
        });

        // строки, пережившие deduplication, находятся в списке?
        if (splits.size() > 0) {
            // да: объединить содержимое списка ч/з разделитель, записать результат в текущий ff
            flowFile = session.write(flowFile, (OutputStream out) -> {
                out.write(splits.stream().collect(Collectors.joining("\n")).getBytes());
            });
            session.transfer(flowFile, SUCCESS);
        } else {
            // иначе удалить ff
            session.remove(flowFile);
        }
    }
}
