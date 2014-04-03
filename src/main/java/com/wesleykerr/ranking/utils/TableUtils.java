package com.wesleykerr.ranking.utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Set;

import org.apache.log4j.Logger;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import com.google.common.collect.TreeBasedTable;

/**
 * 
 * @author wkerr
 *
 */
public class TableUtils {
    private static final Logger LOGGER = Logger.getLogger(TableUtils.class);


    /**
     * Grab the value from the matrix.  If non-existent, then
     * return a default value.
     * @param matrix
     * @param row
     * @param col
     * @return
     */
    public static <T extends Comparable<T>> double get(Table<T,T,Double> matrix, T row, T col) { 
        Double value = matrix.get(row, col);
        return value == null ? 0.0 : value;
    }

    /**
     * compute the cosine distance for each item in the
     * matrix.
     * @param matrix
     */
    public static <T extends Comparable<T>> Table<T, T, Double> computeCosine(Table<T,T,Double> table) { 
        LOGGER.info("begin compute cosine");
        Table<T,T,Double> cosMatrix = HashBasedTable.create();
        
        int count = 0;
        for (T row : table.rowKeySet()) {
            for (T col : table.columnKeySet()) {
                double bothCount = get(table, row, col);
                double rowCount = get(table, row, row);
                double colCount = get(table, col, col);
                double cosSim = bothCount / (Math.sqrt(rowCount)*Math.sqrt(colCount));
                
                cosMatrix.put(row, col, cosSim);
                
                ++count;
                if (count % 100000 == 0)
                    LOGGER.info("...matrix ops " + count);
            }
        }
        LOGGER.info("end compute cosine");
        return cosMatrix;
    }
    
    /**
     * 
     * @param allItems
     * @param matrix
     * @return
     */
    public static <T extends Comparable<T>> Table<T, T, Double> rowNormalize(Table<T,T,Double> table) { 
        Table<T,T,Double> normalMatrix = TreeBasedTable.create();
        for (T row : table.rowKeySet()) { 
            double sum = 0.0;
            for (T col : table.columnKeySet()) { 
                sum += get(table, row, col);
            }
            
            for (T col : table.columnKeySet()) { 
                normalMatrix.put(row, col, get(table, row, col) / sum);
            }
        }
        return normalMatrix;
    }
    
    /**
     * Merge the second ItemItemMatrix into the first.  Note that this
     * is a mutable operation.  We are altering m1.
     * @param t1
     * @param t2
     */
    public static <T extends Comparable<T>> void mergeInto(Table<T,T,Double> t1, Table<T,T,Double> t2) { 
        for (Table.Cell<T, T, Double> cell : t2.cellSet()) {
            double oldValue = get(t1, cell.getRowKey(), cell.getColumnKey());
            t1.put(cell.getRowKey(), cell.getColumnKey(), oldValue + cell.getValue());
        }
    }

    /**
     * writes the given table to a CSV file with headers and row names.
     * @param t1
     * @param outputFile
     */
    public static <T extends Comparable<T>> void writeCSVMatrix(Table<T,T,Double> t1, File outputFile) { 
        LOGGER.info("begin writeCSVMatrix");
        Set<T> keySet = Sets.newTreeSet();
        keySet.addAll(t1.rowKeySet());
        keySet.addAll(t1.columnKeySet());
        
        try (BufferedWriter out = new BufferedWriter(new FileWriter(outputFile))) { 
            out.write("item");
            for (T key : keySet) { 
                out.write("," + key.toString());
            }
            out.write("\n");
            
            for (T key1 : keySet) { 
                out.write(key1.toString());
                for (T key2 : keySet) { 
                    out.write("," + t1.get(key1, key2).toString());
                }
                out.write("\n");
            }
        } catch (Exception e) { 
            throw new RuntimeException(e);
        }
        LOGGER.info("end writeCSVMatrix");
    }
}
