package com.tracker.db.simplehbase;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import com.tracker.db.util.Util;

/**
 * TypeInfoHolder.
 * 
 * */
public class TypeInfoHolder {

    /** log. */
    final private static Logger                                log       = Logger.getLogger(TypeInfoHolder.class);

    /** type->typeInfo map. */
    private static ConcurrentHashMap<Class<?>, TypeInfo> typeInfos = new ConcurrentHashMap<Class<?>, TypeInfo>();

    /**
     * Find type info by type.
     * 
     * @param type POJO's type.
     * @return TypeInfo.
     */
    public static TypeInfo findTypeInfo(Class<?> type) {
        Util.checkNull(type);

        if (typeInfos.get(type) == null) {
        	TypeInfo oldTypeInfo = typeInfos.putIfAbsent(type,TypeInfo.parse(type));
            if (oldTypeInfo == null) {
                log.info("register TypeInfo\n" + typeInfos.get(type));
            }
        }

        return typeInfos.get(type);
    }
}