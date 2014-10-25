package com.tracker.db.simplehbase;

import java.lang.reflect.Field;

import org.apache.hadoop.hbase.util.Bytes;

import com.tracker.db.simplehbase.annotation.HBaseColumn;
import com.tracker.db.simplehbase.annotation.HBaseTable;
import com.tracker.db.simplehbase.exception.SimpleHBaseException;
import com.tracker.db.simplehbase.type.TypeHandler;
import com.tracker.db.simplehbase.type.TypeHandlerHolder;
import com.tracker.db.util.StringUtil;

/**
 * POJO's field and Hbase table's column mapping info.
 * 
 * */
public class ColumnInfo {

    /** POJO's class type. */
    Class<?> type;
    /** POJO's field. */
    Field    field;
    /** hbase's family. */
    String   family;
    /** hbase's family bytes. */
    byte[]   familyBytes;
    /** hbase's qualifier. */
    String   qualifier;
    /** hbase's qualifier bytes. */
    byte[]   qualifierBytes;
    
    /** TypeHandler instance. */
    TypeHandler  typeHandler;
    
    boolean isStoreStringType;
    
    boolean isIncrementType;

    private ColumnInfo() {
    }

    /**
     * Parse ColumnInfo from POJO's field.
     * 
     * @param type POJO's class type.
     * @param field POJO' field.
     * @return ColumnInfo.
     * */
    public static ColumnInfo parse(Class<?> type, Field field) {
        String defaultFamily = null;

        HBaseTable hbaseTable = type.getAnnotation(HBaseTable.class);
        if (hbaseTable != null) {
            defaultFamily = hbaseTable.defaultFamily();
        }

        HBaseColumn hbaseColumn = field.getAnnotation(HBaseColumn.class);
        if (hbaseColumn == null) {
            return null;
        }

        String family = hbaseColumn.family();
        String qualifier = hbaseColumn.qualifier();

        if (StringUtil.isEmptyString(family)) {
            family = defaultFamily;
        }

        if (StringUtil.isEmptyString(family)) {
            throw new SimpleHBaseException("family is null or empty. type="
                    + type + " field=" + field);
        }

        if (StringUtil.isEmptyString(qualifier)) {
            throw new SimpleHBaseException("qualifier is null or empty. type="
                    + type + " field=" + field);
        }

        ColumnInfo columnInfo = new ColumnInfo();
        columnInfo.type = type;
        columnInfo.field = field;
        columnInfo.family = family;
        columnInfo.familyBytes = Bytes.toBytes(family);
        columnInfo.qualifier = qualifier;
        columnInfo.qualifierBytes = Bytes.toBytes(qualifier);
        columnInfo.isStoreStringType = hbaseColumn.isStoreStringType();
        columnInfo.isIncrementType = hbaseColumn.isIncrementType();
        columnInfo.typeHandler = TypeHandlerHolder.findDefaultHandler(field.getType());
        
        return columnInfo;
    }
    
    
    
    public Class<?> getType() {
		return type;
	}

	public Field getField() {
		return field;
	}

	public String getFamily() {
		return family;
	}

	public byte[] getFamilyBytes() {
		return familyBytes;
	}

	public String getQualifier() {
		return qualifier;
	}

	public byte[] getQualifierBytes() {
		return qualifierBytes;
	}

	public TypeHandler getTypeHandler() {
		return typeHandler;
	}

	public boolean isStoreStringType() {
		return isStoreStringType;
	}

	@Override
    public String toString() {
        return "type=" + type + " field=" + field + " family=" + family
                + " qualifier=" + qualifier;
    }
}