package com.job.applicants.aptitude.test;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

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
        FlowFile flowfile = session.get();
/*
        dead duplicates
        
        вход: строка с разделителями
        
        инвариант:
        > разделитель \n
        > сплит не содержит разделитель
        > все объекты в кэше уникальны
        
        контракт:
        > пустая строка означает отсутствие данных на входе (в сплите)
        
        последовательность действий:
        создать буфер
        цикл (получить сплиты, получить первый/следующий сплит, продолжать, пока есть следующий):
            тест
                сплит пуст ?
                    да: тест завершен
                    нет:
                        insert! в кэш (побочный эффект - модификация кэша)
                        insert! удался ?
                            да: записать сплит в буфер
        буфер не пуст ?
            да: записать буфер в ff
        */
        session.transfer(flowfile, SUCCESS);
    }
}
