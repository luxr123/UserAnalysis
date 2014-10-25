package com.tracker.db.simplehbase;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.util.Bytes;

import com.tracker.db.simplehbase.annotation.HBaseTable;
import com.tracker.db.simplehbase.annotation.NotNullable;
import com.tracker.db.simplehbase.annotation.Nullable;
import com.tracker.db.simplehbase.exception.SimpleHBaseException;
import com.tracker.db.util.Util;

/**
 * POJO type and Hbase table mapping info.
 * 
 * */
public class TypeInfo {
    //POJO's type
    private Class<?> type;
    @NotNullable
    private String	tableName; // tableName. not null.
    private byte[]	tableNameBytes; // table name bytes.
    @Nullable
    private String	defaultFamily; // default family. can be null.
    private byte[]	defaultFamilyBytes; //default family bytes.
    
    // Qualifier->Family-> ColumnInfo.
    private Map<String, Map<String, ColumnInfo>> columnInfosMap = new HashMap<String, Map<String, ColumnInfo>>();

    private TypeInfo() {
    }

    /**
     * Parse TypeInfo from POJO's type.
     * */
    public static TypeInfo parse(Class<?> type) {
        Util.checkNull(type);

        TypeInfo typeInfo = new TypeInfo();
        typeInfo.type = type;

        //table
        HBaseTable hbaseTable = type.getAnnotation(HBaseTable.class);
        if (hbaseTable == null || hbaseTable.tableName() == null) {
            throw new SimpleHBaseException("HBaseTable annotation is null");
        } 
        typeInfo.tableName = hbaseTable.tableName();
        typeInfo.tableNameBytes = Bytes.toBytes(typeInfo.tableName);
        typeInfo.defaultFamily = hbaseTable.defaultFamily();
        if(typeInfo.defaultFamily != null && typeInfo.defaultFamily.length() > 0)
        	typeInfo.defaultFamilyBytes = Bytes.toBytes(typeInfo.defaultFamily);
        
        //field
        Field[] fields = type.getDeclaredFields();
        for (Field field : fields) {
            field.setAccessible(true);
            ColumnInfo columnInfo = ColumnInfo.parse(type, field);
            if (columnInfo == null) {
                continue;
            }
            if (!typeInfo.columnInfosMap.containsKey(columnInfo.qualifier)) {
                typeInfo.columnInfosMap.put(columnInfo.qualifier,
                        new HashMap<String, ColumnInfo>());
            }
            typeInfo.columnInfosMap.get(columnInfo.qualifier).put(
                    columnInfo.family, columnInfo);
        }

        return typeInfo;
    }
    
    /**
     * Find ColumnInfo by family and qualifier.
     * */
    public ColumnInfo findColumnInfo(String family, String qualifier) {
        Util.checkEmptyString(family);
        Util.checkEmptyString(qualifier);

        ColumnInfo result = columnInfosMap.get(qualifier).get(family);
        if (result == null) {
            throw new SimpleHBaseException("no HBaseColumnSchema found.");
        }

        return result;
    }

    /**
     * Find ColumnInfo by qualifier.
     * 
     * <pre>
     * We can use this method if ColumnInfo doesn't have more than one family with same qualifier.
     * </pre>
     * */
    public ColumnInfo findColumnInfo(String qualifier) {
        Util.checkEmptyString(qualifier);

        Map<String, ColumnInfo> tem = columnInfosMap.get(qualifier);
        if (tem.size() == 1) {
            for (ColumnInfo t : tem.values()) {
                return t;
            }
        }

        throw new SimpleHBaseException(
                "0 or many HBaseColumnSchema with qualifier = " + qualifier);
    }

    /**
     * Find all HBaseColumnSchemas.
     * */
    public List<ColumnInfo> findAllColumnInfo() {
        List<ColumnInfo> result = new ArrayList<ColumnInfo>();

        for (Map<String, ColumnInfo> t : columnInfosMap.values()) {
            for (ColumnInfo columnInfo : t.values()) {
                result.add(columnInfo);
            }
        }
        return result;
    }
    
    
    public Class<?> getType() {
        return type;
    }
    
    public String getTableName() {
		return tableName;
	}

	public byte[] getTableNameBytes() {
		return tableNameBytes;
	}

	public String getDefaultFamily() {
		return defaultFamily;
	}

	public byte[] getDefaultFamilyBytes() {
		return defaultFamilyBytes;
	}

	public Map<String, Map<String, ColumnInfo>> getColumnInfosMap() {
		return columnInfosMap;
	}

	@Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("-----------------" + getClass()
                + "-----------------------\n");
        sb.append("type=" + type + "\n");
        for (ColumnInfo columnInfo : findAllColumnInfo()) {
            sb.append(columnInfo + "\n");
        }
        sb.append("-----------------" + getClass()
                + "-----------------------\n");
        return sb.toString();
    }
}
