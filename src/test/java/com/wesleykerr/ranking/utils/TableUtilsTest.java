package com.wesleykerr.ranking.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import com.google.common.collect.TreeBasedTable;
import com.wesleykerr.ranking.utils.TableUtils;

public class TableUtilsTest {

    Set<String> allItems;
    Table<String,String,Double> matrix;
    
    @Before
    public void setUp() { 
    }
    
    @Test
    public void testGet() { 
        Table<Integer,Integer,Double> table = TreeBasedTable.create();
        table.put(1, 1, 1.0);
        
        assertEquals(table.get(1, 1), TableUtils.get(table, 1, 1), 0.00001);
        assertEquals(0, TableUtils.get(table, 1, 0), 0.00001);
    }
    
    public void testMergeInto() { 
        double[][] array1 = { 
                { 2, 1 },
                { 1, 2 },
            };
        
        Table<String,String,Double> table1 = TreeBasedTable.create();
        for (int i = 0; i < array1.length; ++i) { 
            for (int j = 0; j < array1[i].length; ++j) { 
                table1.put(String.valueOf(i), String.valueOf(j), array1[i][j]);
            }
        }
        
        double[][] array2 = {
                { 1, 0, 1 },
                { 0, 1, 1 },
        };

        Table<String,String,Double> table2 = TreeBasedTable.create();
        for (int i = 0; i < array2.length; ++i) { 
            for (int j = 0; j < array2[i].length; ++j) { 
                table2.put(String.valueOf(i), String.valueOf(j), array2[i][j]);
            }
        }
        
        double[][] array3 = {
                { 3, 1, 1 },
                { 1, 3, 1 },
        };

        Table<String,String,Double> table3 = TreeBasedTable.create();
        for (int i = 0; i < array3.length; ++i) { 
            for (int j = 0; j < array3[i].length; ++j) { 
                table2.put(String.valueOf(i), String.valueOf(j), array3[i][j]);
            }
        }
        
        TableUtils.mergeInto(table1, table2);
        assertEquals(table1, table3);
    }

    @Test
    public void testRowNormalize() { 
        double[][] table = { 
                { 2, 1, 1, 1, 1 },
                { 1, 2, 2, 1, 1 },
                { 1, 2, 2, 1, 1 },
                { 1, 1, 1, 1, 0 },
                { 1, 1, 1, 0, 2 },
            };

        System.out.println("PREPARING MATRIX");
        allItems = Sets.newTreeSet(Arrays.asList("0", "1", "2", "3", "4"));
        matrix = TreeBasedTable.create();
        for (int i = 0; i < table.length; ++i) { 
            for (int j = 0; j < table[i].length; ++j) { 
                matrix.put(String.valueOf(i), String.valueOf(j), table[i][j]);
            }
        }

        double[][] rowNormalTable = { 
                { 2.0/6, 1.0/6, 1.0/6, 1.0/6, 1.0/6 },
                { 1.0/7, 2.0/7, 2.0/7, 1.0/7, 1.0/7 },
                { 1.0/7, 2.0/7, 2.0/7, 1.0/7, 1.0/7 },
                { 1.0/4, 1.0/4, 1.0/4, 1.0/4, 0.0/4 },
                { 1.0/5, 1.0/5, 1.0/5, 0.0/5, 2.0/5 },
            };
            
        
        Table<String,String,Double> row = TableUtils.rowNormalize(matrix);
        for (int i = 0; i < rowNormalTable.length; ++i) { 
            for (int j = 0; j < rowNormalTable[i].length; ++j) { 
                assertTrue(new Double(row.get(String.valueOf(i), String.valueOf(j))).compareTo(rowNormalTable[i][j]) == 0);
            }
        }        
    }
    
    @Test
    public void testCosine() { 
        allItems = Sets.newTreeSet(Arrays.asList("0", "1", "2"));
        matrix = TreeBasedTable.create();
        matrix.put("0", "0", 2d);
        matrix.put("1", "1", 3d);
        matrix.put("2", "2", 2d);

        matrix.put("0", "1", 2d);
        matrix.put("1", "0", 2d);
        
        matrix.put("0", "2", 1d);
        matrix.put("2", "0", 1d);
        
        matrix.put("1", "2", 2d);
        matrix.put("2", "1", 2d);
        
        Table<String,String,Double> expected = TreeBasedTable.create();
        expected.put("0", "0", 1.0);
        expected.put("0", "1", 2.0 / (Math.sqrt(2)*Math.sqrt(3)));
        expected.put("0", "2", 1.0 / 2.0);

        expected.put("1", "0", expected.get("0","1"));
        expected.put("1", "1", 1.0);
        expected.put("1", "2", 2.0 / (Math.sqrt(3)*Math.sqrt(2)));

        expected.put("2", "0", expected.get("0","2"));
        expected.put("2", "1", expected.get("1","2"));
        expected.put("2", "2", 1.0);
        
        Table<String,String,Double> cosineMatrix = TableUtils.computeCosine(matrix);
        for (int i = 0; i < 3; ++i) { 
            for (int j = 0; j < 3; ++j) { 
                double o = cosineMatrix.get(String.valueOf(i),String.valueOf(j));
                double e = expected.get(String.valueOf(i),String.valueOf(j));
                double epsilon = o - e;
                System.out.println(i + "," + j + " o: " + o + " e: " + e);
                assertTrue(Math.abs(epsilon) <= 0.00001);
            }
        }
    }
}
