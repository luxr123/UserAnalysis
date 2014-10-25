package com.tracker.flume.source.realtime;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * 实时监控日志目录，并实时传输日志文件
 * 
 * 必填值：trackDirPath、filePrefix
 * @author jason.hua
 *
 */
public class DirectoryTailSource extends AbstractSource implements
		Configurable, EventDrivenSource {

	private static final Logger logger = LoggerFactory
			.getLogger(DirectoryTailSource.class);

	public static final String TRACK_DIR_PATH = "trackDirPath";
	public static final String FILE_PREFIX = "filePrefix";
	public static final String COMPLETED_SUFFIX = "completedSuffix";
	public static final String BATCH_SIZE = "batchSize";
	public static final String META_UPDATE_INITIAL_DELAY = "metaUpdateDelay";
	public static final String META_UPDATE_PERIOD = "metaUpdatePeriod";
	public static final String LOG_PARSER_CLASS = "logParserClass";
	public static final String SELECTOR_HEADER = "header";
	public static final String SELECTOR_MAPPING = "mapping";
	public static final int DEFAULT_BATCH_SIZE = 100;
	public static final String SLEEP_TIME   = "sleepTime";
	
	/* Config options */
	private String completedSuffix;
	private String filePrefix;
	private String trackDirPath;
	private int batchSize; 
	private int initialDelay; // meta file update initial delay
	private int period;// meta file update period
	private String logParserClass;
	private String header;
//	private String mapping;
	private int sleepTime;
	
	private SourceCounter sourceCounter;
	private RealTimelFileEventReader reader;
	
	private ExecutorService executorService;

	@Override
	public synchronized void start() {
		logger.info("TraceDirSource source starting with directory: {}", trackDirPath);
	  
		// tail file
		try {
			reader = new RealTimelFileEventReader.Builder()
				.trackDirPath(trackDirPath)
				.filePrefix(filePrefix)
				.completedSuffix(completedSuffix)
				.metaUpdateInitialDelay(initialDelay)
				.metaUpdatePeriod(period)
				.logParserClass(logParserClass)
				.header(header)
//				.mapping(mapping)
				.build();
		} catch (IOException e) {
			throw new FlumeException("Error instantiating RealTimelFileEventReader", e);
		}
		
		executorService = Executors.newSingleThreadExecutor();
		executorService.submit(new TailDirectoryRunnable(reader, sourceCounter));

		super.start();
		logger.debug("DirectoryTailSource source started");
		sourceCounter.start();
	}

	@Override
	public synchronized void stop() {
		super.stop();
		sourceCounter.stop();
		logger.info("DirectorTailSource {} stopped. Metrics: {}", getName(), sourceCounter);
	}

	@Override
	public void configure(Context context) {
		logger.info("Source Configuring ...");

		if (sourceCounter == null) {
			sourceCounter = new SourceCounter(getName());
		}
		
		trackDirPath = context.getString(TRACK_DIR_PATH);
		filePrefix = context.getString(FILE_PREFIX);
		header = context.getString(SELECTOR_HEADER);
//		mapping = context.getString(SELECTOR_MAPPING);
		Preconditions.checkNotNull(trackDirPath);
		Preconditions.checkNotNull(filePrefix);
		
		completedSuffix = context.getString(COMPLETED_SUFFIX, RealTimelFileEventReader.COMPLETED_SUFFIX_VALUE);
	    batchSize = context.getInteger(BATCH_SIZE, DEFAULT_BATCH_SIZE);
	    initialDelay = context.getInteger(META_UPDATE_INITIAL_DELAY, RealTimelFileEventReader.UPDATE_META_FILE_DELAY);
	    period = context.getInteger(META_UPDATE_PERIOD, RealTimelFileEventReader.UPDATE_META_FILE_FREQ);
	    logParserClass = context.getString(LOG_PARSER_CLASS, RealTimelFileEventReader.DEFAULT_LOG_PARSER_CLASS);
	    
	    sleepTime = context.getInteger(SLEEP_TIME, 0); //默认100毫秒
	}
	
	/**
	 * 跟踪日志文件，实时读取并传输数据到channel中
	 */
	private class TailDirectoryRunnable implements Runnable {
		private RealTimelFileEventReader reader;
		private SourceCounter sourceCounter;

		public TailDirectoryRunnable(RealTimelFileEventReader reader,
				SourceCounter sourceCounter) {
			this.reader = reader;
			this.sourceCounter = sourceCounter;
		}

		@Override
		public void run() {
			try {
				while (true) {
					List<Event> events = reader.readEvents(batchSize);
					if (events.isEmpty()) {
						if(sleepTime > 0)
							Thread.sleep(sleepTime);
						continue;
					}
					sourceCounter.addToEventReceivedCount(events.size());
					sourceCounter.incrementAppendBatchReceivedCount();

					getChannelProcessor().processEventBatch(events);
					reader.commit();
					sourceCounter.addToEventAcceptedCount(events.size());
					sourceCounter.incrementAppendBatchAcceptedCount();
				}
			} catch (Throwable t) {
				logger.error("Uncaught exception in Runnable", t);
				if (t instanceof Error) {
					throw (Error) t;
				}
			}
		}
	}
}
