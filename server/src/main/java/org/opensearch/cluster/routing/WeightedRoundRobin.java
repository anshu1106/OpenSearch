/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing;

import org.opensearch.common.settings.Setting;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

import static java.util.Collections.emptyList;

public class WeightedRoundRobin<T> implements Iterator<T>, Iterable<T> {

    private List<WeightedRoundRobin.Entity<T>> entities;
    private int turn;
    private int lastSelectedEntity;
    private double currentWeight = 0;

//    server/src/main/java/org/opensearch/cluster/routing/WeightedRoundRobin.java 

    public WeightedRoundRobin(int lastSelectedEntity) {
        this.entities = null;
        this.turn = 0;
        this.lastSelectedEntity = lastSelectedEntity;
    }

    /* (non-Javadoc)
     * @see java.util.Iterator#hasNext()
     */
    @Override
    public boolean hasNext() {
        return entities.size() > 0;
    }

    /* (non-Javadoc)
     * @see java.util.Iterator#next()
     */
    @Override
    public T next() {
        Entity<T> entity = entities.get(turn++);
        return entity.getTarget();
    }

    /* (non-Javadoc)
     * @see java.lang.Iterable#iterator()
     */
    @Override
    public Iterator<T> iterator() {
        return this;
    }

    public List<WeightedRoundRobin.Entity<T>> orderEntities(List<WeightedRoundRobin.Entity<T>> entities) {
        int size = entities.size();
        List<WeightedRoundRobin.Entity<T>> orderedWeight = new ArrayList<>();
        if (size <= 0) {
            return null;
        }
        if (size == 1) {
            return entities;
        }

        // Find maximum weight and greatest common divisor of weight across all entities
        double maxWeight = 0;
        double sumWeight = 0;
        Double gcd = null;
        for (WeightedRoundRobin.Entity<T> entity : entities) {
            maxWeight = Math.max(maxWeight, entity.getWeight());
            gcd = (gcd == null) ? entity.getWeight() : gcd(gcd, entity.getWeight());
            sumWeight += entity.getWeight();
        }
        int count = 0;
        while (count < sumWeight) {
            lastSelectedEntity = (lastSelectedEntity + 1) % size;
            if (lastSelectedEntity == 0) {
                currentWeight = currentWeight - gcd;
                if (currentWeight <= 0) {
                    currentWeight = maxWeight;
                    if (currentWeight == 0) {
                        return orderedWeight;
                    }
                }
            }
            if (entities.get(lastSelectedEntity).getWeight() >= currentWeight) {
                orderedWeight.add(entities.get(lastSelectedEntity));
                count++;
            }
        }
        return orderedWeight;

    }

    /**
     * Return greatest common divisor for two integers
     * https://en.wikipedia.org/wiki/Greatest_common_divisor#Using_Euclid.27s_algorithm
     *
     * @param a
     * @param b
     * @return greatest common divisor
     */
    private double gcd(double a, double b) {
        return (b == 0) ? a : gcd(b, a % b);
    }

    static final class Entity<T> {

        private double weight;
        private T target;

        public Entity(double weight, T target) {
            this.weight = weight;
            this.target = target;
        }

        public T getTarget() {
            return this.target;
        }

        public void setTarget(T target) {
            this.target = target;
        }

        public double getWeight() {
            return this.weight;
        }

        public void setWeight(double weight) {
            this.weight = weight;
        }
    }

}
