package com.wesleykerr.ranking;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Table;
import com.wesleykerr.ranking.Emitter;

public class EmitterTest {

    @Before
    public void setUp() { 
        assertTrue(true);
    }
    
    @Test
    public void testCosineEmitter() { 
        List<String> items = Lists.newArrayList("a", "b", "c", "d");
        Table<String,String,Double> values = Emitter.cosine.emit(items);

        assertEquals(values.size(), (items.size()*(items.size()-1))/2 + items.size());
        for (String item : items) { 
            assertEquals(values.get(item, item), new Double(1d));
        }
        
        assertEquals(values.get("a", "b"), new Double(1d));
        assertEquals(values.get("a", "c"), new Double(1d));
        assertEquals(values.get("a", "d"), new Double(1d));
        assertEquals(values.get("b", "c"), new Double(1d));
        assertEquals(values.get("b", "d"), new Double(1d));
        assertEquals(values.get("c", "d"), new Double(1d));
        
        items = Lists.newArrayList("a");
        values = Emitter.cosine.emit(items);

        assertEquals(values.size(), (items.size()*(items.size()-1))/2 + items.size());
        for (String item : items) { 
            assertEquals(values.get(item, item), new Double(1d));
        }
    }
    
    @Test
    public void testCosineNormalizedEmitter() { 
        List<String> items = Lists.newArrayList("a", "b", "c", "d");
        Table<String,String,Double> values = Emitter.cosineWeighted.emit(items);

        double expected = 0.25*0.25;
        assertEquals(values.size(), (items.size()*(items.size()-1))/2 + items.size());
        for (String item : items) { 
            assertEquals(values.get(item, item), new Double(expected));
        }
        
        assertEquals(values.get("a", "b"), new Double(expected));
        assertEquals(values.get("a", "c"), new Double(expected));
        assertEquals(values.get("a", "d"), new Double(expected));
        assertEquals(values.get("b", "c"), new Double(expected));
        assertEquals(values.get("b", "d"), new Double(expected));
        assertEquals(values.get("c", "d"), new Double(expected));
        
        items = Lists.newArrayList("a");
        values = Emitter.cosine.emit(items);

        assertEquals(values.size(), (items.size()*(items.size()-1))/2 + items.size());
        for (String item : items) { 
            assertEquals(values.get(item, item), new Double(1d));
        }
    }

}
