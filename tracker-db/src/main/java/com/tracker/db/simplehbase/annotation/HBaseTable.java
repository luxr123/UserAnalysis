package com.tracker.db.simplehbase.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * SimpleHbase POJO-Hbase annotation.
 * 
 * <pre>
 * Applied on POJO type.
 * The family of @HBaseColumn(if not empty) will override @HBaseTable's family.
 * </pre>
 * 
 * */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface HBaseTable {


	public String tableName();
	
    /**
     * Default family name.
     * 
     * @return default family.
     */
    public String defaultFamily() default "";
}
