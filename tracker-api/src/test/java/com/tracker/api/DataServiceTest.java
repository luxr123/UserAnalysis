package com.tracker.api;

import java.io.IOException;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import com.tracker.api.thrift.data.DataService;

public class DataServiceTest {
	public static void main(String[] args) throws TException, IOException {
		TSocket socket = new TSocket("10.100.2.93", 44444); 
		socket.setTimeout(10000); 
		TTransport transport = new TFramedTransport(socket);
		transport.open();
		TProtocol protocol = new TCompactProtocol(transport);
		
		DataService.Client client = new DataService.Client(new TMultiplexedProtocol(protocol, "DataService"));
		
		
//		DataServiceHandler client = new DataServiceHandler();
		
//		System.out.println(client.getCountry());
//		System.out.println(client.getProvinceByCountryId(2));
//		System.out.println(client.getSearchEngineData());
		
//		System.out.println(client.getWebSite());
//		System.out.println(client.getSiteSEAndType(1));
//		System.out.println(client.getSiteSearchType(1, 1));
//		System.out.println(client.getSearchCondition(1, 3));
//		System.out.println(client.getSearchCondition(1, 1, 2));
//		System.out.println(client.getSearchPage(1, 1, 1));
//		
		
//		System.out.println(client.getUserType(1));
		
		System.out.println(client.getVisitTypeOfSearch(1, 1, 1));
		transport.close();
//		Servers.shutdown();
	}
}
