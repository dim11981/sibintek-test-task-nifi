/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.job.applicants.aptitude.test;

import java.util.Set;

/**
 * Интерфейс операций кэша.
 *
 */
interface CacheSetOperation<T> {

    /**
     * Обновить кэш.
     *
     * @param  data
     *         записываемые данные кэша как объекта, реализующего интерфейс {@code Set<T>}
     */
    public void save(Set<T> data);

    /**
     * Загрузить кэш.
     * 
     * @return {@code Set<T>} объект кэша, реализующего интерфейс {@code Set<T>}
     *
     */
    public Set<T> load();
}