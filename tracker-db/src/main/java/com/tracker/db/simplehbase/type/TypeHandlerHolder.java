package com.tracker.db.simplehbase.type;

import java.util.HashMap;
import java.util.Map;

import com.tracker.db.simplehbase.type.handler.BooleanHandler;
import com.tracker.db.simplehbase.type.handler.ByteHandler;
import com.tracker.db.simplehbase.type.handler.DoubleHandler;
import com.tracker.db.simplehbase.type.handler.FloatHandler;
import com.tracker.db.simplehbase.type.handler.IntegerHandler;
import com.tracker.db.simplehbase.type.handler.LongHandler;
import com.tracker.db.simplehbase.type.handler.ShortHandler;
import com.tracker.db.simplehbase.type.handler.StringHandler;
import com.tracker.db.util.ClassUtil;
import com.tracker.db.util.Util;

/**
 * The holder of typeHandler's instance.
 * 
 * */
public class TypeHandlerHolder {
    private static Map<Class<?>, TypeHandler> defaultHandlers = new HashMap<Class<?>, TypeHandler>();

    static {
        defaultHandlers.put(String.class, new StringHandler());
        defaultHandlers.put(Boolean.class, new BooleanHandler());
        defaultHandlers.put(Byte.class, new ByteHandler());
        defaultHandlers.put(Short.class, new ShortHandler());
        defaultHandlers.put(Integer.class, new IntegerHandler());
        defaultHandlers.put(Long.class, new LongHandler());
        defaultHandlers.put(Float.class, new FloatHandler());
        defaultHandlers.put(Double.class, new DoubleHandler());
    }

    public static TypeHandler findDefaultHandler(Class<?> type) {
        Util.checkNull(type);
        type = ClassUtil.tryConvertToBoxClass(type);
        return defaultHandlers.get(type);
    }
}
