package com.geca.kafka;

import java.io.FileReader;
import java.util.List;

import com.opencsv.CSVReader;
import com.opencsv.bean.CsvToBean;
import com.opencsv.bean.CsvToBeanBuilder;

public class CsvReader {
	private String fileName;
	private List<KafkaSchema> transactionList;
	
	public CsvReader(String fileName) {
		this.fileName = fileName;
	}
	
	public List<KafkaSchema> getList() {
		CSVReader csvReader = null;
		try {
			csvReader = new CSVReader(new FileReader(fileName));
		}catch(Exception e){
			System.out.println("file for producing kafka records is not found");
		}
		CsvToBean<KafkaSchema> csvToBean = new CsvToBeanBuilder<KafkaSchema>(csvReader)
								.withType(KafkaSchema.class)
								.withIgnoreLeadingWhiteSpace(true)
								.build();
		transactionList = csvToBean.parse();
		
		return transactionList;
	}
}
