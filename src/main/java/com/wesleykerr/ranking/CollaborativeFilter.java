package com.wesleykerr.ranking;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.zip.GZIPInputStream;

import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Table;
import com.google.common.collect.TreeBasedTable;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.wesleykerr.ranking.utils.TableUtils;

/**
 * The assumption is that the data that goes through this
 * training code is "clean" in the sense that inactive items
 * have been removed and only things that can be recommended
 * remain.
 * 
 * @author wkerr
 *
 */
public class CollaborativeFilter {
    private static final Logger LOGGER = Logger.getLogger(CollaborativeFilter.class);

    private Emitter emitter;
    
    private boolean rowNorm;
    private double threshold;
    
    public CollaborativeFilter() { 
        rowNorm = false;
    }
    
    public static class Builder { 
        CollaborativeFilter cf;
        
        private Builder() {
            cf = new CollaborativeFilter();
        }
        
        public Builder withEmitter(Emitter emitter) { 
            cf.emitter = emitter;
            return this;
        }
        
        public Builder withRowNorm(boolean rowNorm) { 
            cf.rowNorm = rowNorm;
            return this;
        }
        
        public Builder withThreshold(double threshold) { 
            cf.threshold = threshold;
            return this;
        }
        
        public CollaborativeFilter build() { 
            Preconditions.checkNotNull(cf);
            CollaborativeFilter tmp = cf;
            cf = null;
            return tmp;
        }
        
        public static Builder create() { 
            return new Builder();
        }
    }
    
    /**
     * The reader is a connection to a list of players that we
     * need to process in order to get values for our matrix.
     * @param reader
     * @return
     * @throws Exception
     */
    public Table<Long,Long,Double> processPlayers(Reader reader) throws Exception { 
        JsonParser parser = new JsonParser();
        Table<Long,Long,Double> table = TreeBasedTable.create();
        int lineCount = 0;
        try (BufferedReader in = new BufferedReader(reader)) { 
            for (String line = in.readLine(); line != null && in.ready(); line = in.readLine()) { 
                JsonObject obj = parser.parse(line).getAsJsonObject();
                TableUtils.mergeInto(table, processPlayer(obj));
                
                lineCount++;
                if (lineCount % 100000 == 0)
                    LOGGER.info("processed " + lineCount + " players");
            }
        }
        LOGGER.info("procsssed " + lineCount + " players");
        return finalizeMatrix(table);
    }

    /**
     * Retrieve a single player's contribution to the item-item matrix.
     * We expect a JSON object with the following template --
     *   { "userId": "xxx", ratings: [ {"item": 123, "rating": 2.3 }, ... ]
     * @param obj
     * @return
     */
    public Table<Long,Long,Double> processPlayer(JsonObject obj) { 
        JsonArray ratingsArray = obj.get("ratings").getAsJsonArray();
        List<Long> playerItems = Lists.newArrayList();
        for (JsonElement node : ratingsArray) { 
            JsonObject nodeObj = node.getAsJsonObject();
            if (nodeObj.get("rating").getAsDouble() >= threshold) { 
                playerItems.add(nodeObj.get("item").getAsLong());
            }
        }
        return emitter.emit(playerItems);
    }
    
    /**
     * Update the values in the matrix to represent cosine similarities
     * instead of just counts.  If this collaborative filter uses  row 
     * normalization, perform that as well.
     * @param matrix
     * @return
     */
    public Table<Long,Long,Double> finalizeMatrix(Table<Long,Long,Double> table) { 
        // At this point the matrix should be a upper diagonal matrix.
        LOGGER.info("finalize matrix " + 
                table.rowKeySet().size() + " rows x " + 
                table.columnKeySet().size() + " cols");
        
        Table<Long,Long,Double> result = HashBasedTable.create();
        for (Table.Cell<Long,Long,Double> cell : table.cellSet()) { 
            result.put(cell.getRowKey(), cell.getColumnKey(), cell.getValue());
            if (!cell.getRowKey().equals(cell.getColumnKey())) { 
                result.put(cell.getColumnKey(), cell.getRowKey(), cell.getValue());
            }
        }
        
        result = TableUtils.computeCosine(result);
        if (rowNorm)
            result = TableUtils.rowNormalize(result);
        return result;
    }
    
    public static void main(String[] args) throws Exception { 
    	// TODO 
    	//   pass in the input file
    	//   pass in the output file
    	//   pass in the emitter name
    	//   pass in threshold
    	
        ExecutorService service = Executors.newFixedThreadPool(3);

        List<Future<Boolean>> futures = Lists.newArrayList();
        futures.add(service.submit(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                CollaborativeFilter cf = CollaborativeFilter.Builder.create()
                        .withEmitter(Emitter.cosineWeighted)
                        .withRowNorm(false)
                        .withThreshold(0.5d)
                        .build();
                
                File inputFile = new File("/data/steam/ratings-file.gz");
                try (InputStream inStream = new FileInputStream(inputFile);
                        InputStream gzipInputStream = new GZIPInputStream(inStream);
                        Reader reader = new InputStreamReader(gzipInputStream, "UTF-8")) { 
                    Table<Long,Long,Double> results = cf.processPlayers(reader);
                    // TODO: make this work again.
                    TableUtils.writeCSVMatrix(results, new File("/data/steam/model.csv"));
                    return Boolean.TRUE;
                }
            } 
        }));

        for (Future<Boolean> f : futures) { 
            LOGGER.info("Received: " + f.get());
        }
        
        service.shutdown();
    }
}
