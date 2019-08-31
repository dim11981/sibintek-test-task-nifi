/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.job.applicants.aptitude.test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Простой класс реализации операций кэша.
 *
 */
class SimpleFileCache implements CacheSetOperation<String> {
    
    // каталог и имя файла для хранения данных кэша
    static Path path;
    
    static {
        /**
        * инициализация рабочим каталогом пользователя (в win10 это корневой каталог проекта или экземпляра NiFi)
        * и именем файла по-умолчанию "deduplicated_etalon.data"
        *
        */
        path = Paths.get(System.getProperty("user.dir"), "deduplicated_etalon.data");
    }

    /**
    * Обновить кэш.
    *
    * @param  data
    *         записываемые данные кэша как объекта, реализующего интерфейс {@code Set<String>}
    * 
    * сохранение данных кэша выполняется строка за строкой
    */
    @Override
    public void save(Set<String> data) {
        try {
            // сохранение данных кэша выполняется строка за строкой
            try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(path.toFile())))) {
                for (String line : data) {
                    bw.write(line);
                    bw.newLine();
                }
                bw.flush();
            }
        } catch (FileNotFoundException ex) {
            System.out.println(ex);
        } catch (IOException ex) {
            System.out.println(ex);
        }
    }

    /**
     * Загрузить кэш.
     * 
     * @return {@code Set<String>} объект кэша, реализующего интерфейс {@code Set<String>}
     *
     */
    @Override
    public Set<String> load() {
        try (final BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(path.toFile())))) {
            return new HashSet<>(reader.lines().collect(Collectors.toSet()));
        } catch (FileNotFoundException ex) {
            System.out.println(ex);
            return new HashSet<>();
        } catch (IOException ex) {
            System.out.println(ex);
            return new HashSet<>();
        }
    }
}