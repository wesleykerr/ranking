package com.wesleykerr.ranking.utils;

import com.google.gson.Gson;

public class GsonUtils {

    public static Gson DEFAULT_GSON = new Gson();
    
    public static Gson getDefaultGson() { 
        return DEFAULT_GSON;
    }
}
