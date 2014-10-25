package com.tracker.data.table.searchData;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.tracker.common.utils.ResourceLoader;

public class DicParser {
	private final static String REGEX = ".*\\['(.*)'\\]='(.+)'.*";
	
	public static void main(String[] args) throws Exception {
		DicParser parser = new DicParser();
		parser.parseData("source/search/dd_caseindustry_array_c.js", "data/search/case_industry", false);
		parser.parseData("source/search/dd_casesalary_array_c.js", "data/search/case_salary", false);
		parser.parseData("source/search/dd_commissiontype_array_c.js", "data/search/commission_type", false);
		parser.parseData("source/search/dd_corepos_array_c.js", "data/search/corepos", false);
		parser.parseData("source/search/dd_cosize_array_c.js", "data/search/cosize", false);
		parser.parseData("source/search/dd_cotype_array_c.js", "data/search/cotype", false);
		parser.parseData("source/search/dd_degree_array_c.js", "data/search/degree", false);
		parser.parseData("source/search/dd_gender_array_c.js", "data/search/gender", false);
		parser.parseData("source/search/dd_indtype_array_c.js", "data/search/indtype", false);
		
		parser.parseData("source/search/dd_jobarea_array_c.js", "data/search/jobarea", true);
		parser.parseData("source/search/dd_jobarea_new_array_c.js", "data/search/jobarea", true);
		parser.parseData("source/search/dd_jobareacity_array_c.js", "data/search/jobarea", true);
		parser.parseData("source/search/dd_jobareacityabb_array_c.js", "data/search/jobarea", true);
		
		parser.parseData("source/search/dd_poslevel_array_c.js", "data/search/poslevel", false);
		parser.parseData("source/search/dd_workyear_array_c.js", "data/search/workyear", false);

	}
	
	public void parseData(String inputFile, String outPutFile, boolean append) throws Exception{
		File file = new File(outPutFile);
		if(!file.getParentFile().exists())
			file.getParentFile().mkdir();
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outPutFile, append), "utf-8"));
		BufferedReader br = new BufferedReader(new InputStreamReader(ResourceLoader.getFileInputStream(inputFile), "gbk"));
		String line = null;
		while((line = br.readLine()) != null){
			if(line.contains("Arr[")){
				continue;
			}
			Matcher m = Pattern.compile(REGEX).matcher(line);
			if (m.find()) {
				System.out.println(m.group(1) + " => " + m.group(2));
				bw.write(m.group(1) + "\t" + m.group(2) + "\n");
			}
		}
		bw.flush();
		bw.close();
		br.close();
	}
}
