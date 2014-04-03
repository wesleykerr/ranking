package com.wesleykerr.ranking;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.StringReader;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import com.google.common.collect.TreeBasedTable;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.wesleykerr.ranking.CollaborativeFilter;
import com.wesleykerr.ranking.Emitter;

public class CollaborativeFilterTest {

    List<JsonObject> players;
    CollaborativeFilter cfA;
    CollaborativeFilter cfB;
    

    /**
     * Helper function to compute the cosine similarity of
     * two lists that are small enough to fit into memory.
     * @param a
     * @param b
     * @return
     */
    private Double cosineSim(List<Double> a, List<Double> b) { 
        Preconditions.checkState(a.size() == b.size());
        double sum = 0;
        double sumA = 0;
        double sumB = 0;
        for (int i = 0; i < a.size(); ++i) { 
            sum += (a.get(i) * b.get(i));
            sumA += (a.get(i)*a.get(i));
            sumB += (b.get(i)*b.get(i));
        }
        return sum / (Math.sqrt(sumA)*Math.sqrt(sumB));
    }
    
    @Test
    public void testCosineSim() {
        Double c1 = cosineSim(Lists.newArrayList(0d,1d,0d), Lists.newArrayList(1d,1d,1d));
        assertEquals(new Double(1d / Math.sqrt(3)), c1, 0.00001);
        
        Double c2 = cosineSim(Lists.newArrayList(1d,1d,0d), Lists.newArrayList(1d,1d,1d));
        assertEquals(new Double(2d / (Math.sqrt(3)*Math.sqrt(2))), c2, 0.00001);

        Double c3 = cosineSim(Lists.newArrayList(0.5d,0.5d,0d), Lists.newArrayList(0.5d,0.5d,1d));
        assertEquals(new Double(0.5d / (Math.sqrt(0.5)*Math.sqrt(1.5))), c3, 0.00001);
    }
    
    private JsonObject createPlayerObject(List<Long> items) { 
        JsonArray ratings = new JsonArray();
        for (Long item : items)  {
            JsonObject obj = new JsonObject();
            obj.addProperty("item", item);
            obj.addProperty("rating", "1.0");
            ratings.add(obj);
        }

        JsonObject obj = new JsonObject();
        obj.add("ratings", ratings);
        return obj;
    }
    
    @Before
    public void setUp() { 
        players = Lists.newArrayList();
        players.add(createPlayerObject(Lists.newArrayList(2L, 3L)));
        players.add(createPlayerObject(Lists.newArrayList(1L, 2L, 3L)));
        players.add(createPlayerObject(Lists.newArrayList(2L, 3L, 4L)));
        players.add(createPlayerObject(Lists.newArrayList(1L, 2L, 4L)));
        
        cfA = CollaborativeFilter.Builder.create()
                .withEmitter(Emitter.cosineWeighted)
                .withRowNorm(false)
                .build();

        cfB = CollaborativeFilter.Builder.create()
                .withEmitter(Emitter.cosine)
                .withRowNorm(false)
                .build();
    }
    
    private StringReader getData(int start, int end) { 
        StringBuilder buf = new StringBuilder();
        for (int i = start; i <= end; ++i) {
            buf.append(players.get(i).toString()).append("\n");
        }
        return new StringReader(buf.toString());
    }

    private void evalOneUser(CollaborativeFilter cf) { 
        try { 
            Table<Long,Long,Double> itemItem = cf.processPlayers(getData(0,0));
            Set<Long> expectedItems = Sets.newHashSet(3L, 2L);
            Map<Long, List<Double>> columns = Maps.newHashMap();
            columns.put(2L, Lists.newArrayList(1d, 1d));
            columns.put(3L, Lists.newArrayList(1d, 1d));
            evaluate(Lists.newArrayList(expectedItems), columns, itemItem);

            
            Table<Long,Long,Double> expected = TreeBasedTable.create();
            expected.put(3L, 3L, 1d);
            expected.put(3L, 2L, 1d);
            expected.put(2L, 2L, 1d);
            expected.put(2L, 3L, 1d);
            assertEquals(expected, itemItem);
        } catch (Exception e) { 
            System.err.println(e.getMessage());
            e.printStackTrace();
            assertTrue(false);
        }
    }
    
    @Test
    public void testOneUser() { 
        evalOneUser(cfA);
        evalOneUser(cfB);
    }
    
    @Test
    public void testTwoUsers() { 
        Set<Long> expectedItems = Sets.newHashSet(1L, 2L, 3L);

        try { 
            Table<Long,Long,Double> itemItem = cfA.processPlayers(getData(0,1));
            assertEquals(expectedItems, itemItem.columnKeySet());
            assertEquals(expectedItems, itemItem.rowKeySet());

            Map<Long, List<Double>> columns = Maps.newHashMap();
            columns.put(1L, Lists.newArrayList(0d, 1d/3d));
            columns.put(2L, Lists.newArrayList(0.5, 1d/3d));
            columns.put(3L, Lists.newArrayList(0.5, 1d/3d));
            evaluate(Lists.newArrayList(expectedItems), columns, itemItem);
            
            itemItem = cfB.processPlayers(getData(0,1));
            assertEquals(expectedItems, itemItem.columnKeySet());
            assertEquals(expectedItems, itemItem.rowKeySet());

            columns = Maps.newHashMap();
            columns.put(1L, Lists.newArrayList(0d, 1d));
            columns.put(2L, Lists.newArrayList(1d, 1d));
            columns.put(3L, Lists.newArrayList(1d, 1d));
            evaluate(Lists.newArrayList(expectedItems), columns, itemItem);
        } catch (Exception e) { 
            System.err.println(e.getMessage());
            e.printStackTrace();
            assertTrue(false);
        }
    }
    
    @Test
    public void testThreeUsers() { 
        Set<Long> expectedItems = Sets.newHashSet(1L, 2L, 3L, 4L);

        try { 
            Table<Long,Long,Double> itemItem = cfA.processPlayers(getData(0,2));
            assertEquals(expectedItems, itemItem.columnKeySet());
            assertEquals(expectedItems, itemItem.rowKeySet());

            Map<Long, List<Double>> columns = Maps.newHashMap();
            columns.put(1L, Lists.newArrayList(0d, 1d/3d, 0d));
            columns.put(2L, Lists.newArrayList(0.5, 1d/3d, 1d/3d));
            columns.put(3L, Lists.newArrayList(0.5, 1d/3d, 1d/3d));
            columns.put(4L, Lists.newArrayList(0d, 0d, 1d/3d));
            evaluate(Lists.newArrayList(expectedItems), columns, itemItem);
            
            itemItem = cfB.processPlayers(getData(0,2));
            assertEquals(expectedItems, itemItem.columnKeySet());
            assertEquals(expectedItems, itemItem.rowKeySet());

            columns = Maps.newHashMap();
            columns.put(1L, Lists.newArrayList(0d, 1d, 0d));
            columns.put(2L, Lists.newArrayList(1d, 1d, 1d));
            columns.put(3L, Lists.newArrayList(1d, 1d, 1d));
            columns.put(4L, Lists.newArrayList(0d, 0d, 1d));
            evaluate(Lists.newArrayList(expectedItems), columns, itemItem);
        } catch (Exception e) { 
            System.err.println(e.getMessage());
            e.printStackTrace();
            assertTrue(false);
        }
    }

    @Test
    public void testFourUsers() { 
        Set<Long> expectedItems = Sets.newHashSet(1L, 2L, 3L, 4L);

        try { 
            Table<Long,Long,Double> itemItem = cfA.processPlayers(getData(0,3));
            assertEquals(expectedItems, itemItem.columnKeySet());
            assertEquals(expectedItems, itemItem.rowKeySet());

            Map<Long, List<Double>> columns = Maps.newHashMap();
            columns.put(1L, Lists.newArrayList(0d, 1d/3d, 0d, 1d/3d));
            columns.put(2L, Lists.newArrayList(0.5, 1d/3d, 1d/3d, 1d/3d));
            columns.put(3L, Lists.newArrayList(0.5, 1d/3d, 1d/3d, 0d));
            columns.put(4L, Lists.newArrayList(0d, 0d, 1/3d, 1d/3d));
            evaluate(Lists.newArrayList(expectedItems), columns, itemItem);
            
            itemItem = cfB.processPlayers(getData(0,3));
            assertEquals(expectedItems, itemItem.columnKeySet());
            assertEquals(expectedItems, itemItem.rowKeySet());

            columns = Maps.newHashMap();
            columns.put(1L, Lists.newArrayList(0d, 1d, 0d, 1d));
            columns.put(2L, Lists.newArrayList(1d, 1d, 1d, 1d));
            columns.put(3L, Lists.newArrayList(1d, 1d, 1d, 0d));
            columns.put(4L, Lists.newArrayList(0d, 0d, 1d, 1d));
            evaluate(Lists.newArrayList(expectedItems), columns, itemItem);
            
        } catch (Exception e) { 
            System.err.println(e.getMessage());
            e.printStackTrace();
            assertTrue(false);
        }
    }
    
    private void evaluate(List<Long> items, Map<Long, List<Double>> columns, Table<Long,Long,Double> itemItem) { 
        for (int i = 0; i < items.size(); ++i) { 
            Long item1 = items.get(i);
            for (int j = 0; j < items.size(); ++j) { 
                Long item2 = items.get(j);
                double expected = cosineSim(columns.get(item1), columns.get(item2));
                assertEquals(expected, itemItem.get(item1, item2), 0.00001);
            }
        }
    }
}
