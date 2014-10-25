package com.tracker.storm.kpiStatistic.bolt.custom;

import java.util.Arrays;
import java.util.List;
import java.util.zip.CRC32;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;

public class CookieHashGrouping implements CustomStreamGrouping{
	private static final long serialVersionUID = 1L;
    int _index;
    List<Integer> _targets;
    
    public CookieHashGrouping(int index) {
        _index = index;
    }
    
    
    @Override
    public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
        _targets = targetTasks;
    }

    @Override
    public List<Integer> chooseTasks(int fromTask, List<Object> values) {
    	String cookieId = values.get(_index).toString();
        int i = objectToIndex(cookieId, _targets.size());
        System.out.println(cookieId + " => " + i + " => _targets:" + _targets);
        return Arrays.asList(_targets.get(i));
    }

    /**
     * 计算hash值
     */
    public static int objectToIndex(String cookieId, int numPartitions) {
    	CRC32 crc32 = new CRC32();
		crc32.update(cookieId.getBytes());
		long  val = crc32.getValue();
        return (int) (val % numPartitions);
    }
}
