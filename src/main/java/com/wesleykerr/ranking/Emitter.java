package com.wesleykerr.ranking;

import java.util.List;

import org.apache.log4j.Logger;

import com.google.common.collect.Table;
import com.google.common.collect.TreeBasedTable;

/**
 * Emit scores.
 * @author wkerr
 *
 */
public enum Emitter {
    cosine {
        @Override
        public <T extends Comparable<T>> Table<T, T, Double> emit(List<T> items) {
            Table<T,T,Double> table = TreeBasedTable.create();
            for (int i = 0; i < items.size(); ++i) { 
                for (int j = i; j < items.size(); ++j) { 
                    table.put(items.get(i), items.get(j), 1d);
                }
            }
            return table;
        }
    },
    cosineWeighted {
        @Override
        public <T extends Comparable<T>> Table<T, T, Double> emit(List<T> items) {
            Table<T,T,Double> table = TreeBasedTable.create();
            double value = 1.0d / items.size();
            for (int i = 0; i < items.size(); ++i) { 
                for (int j = i; j < items.size(); ++j) { 
                    table.put(items.get(i), items.get(j), value*value);
                }
            }
            return table;
        }
    },
    conditionalProb {
        @Override
        public <T extends Comparable<T>> Table<T, T, Double> emit(List<T> items) {
            throw new RuntimeException("Not yet implemented!");
        }
    };
    
    private static final Logger LOGGER = Logger.getLogger(Emitter.class);
    public abstract <T extends Comparable<T>> Table<T,T,Double> emit(List<T> items);
}
