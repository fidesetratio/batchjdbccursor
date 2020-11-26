package com.app;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.app.service.BatchService;
//https://blog.csdn.net/zxd1435513775/article/details/99677223
@SpringBootApplication
@EnableBatchProcessing
public class Application implements CommandLineRunner  {
	@Autowired
	private BatchService batchService;
	
	
	@Override
	public void run(String... args) throws Exception {
		// TODO Auto-generated method stub
		batchService.createBatchJob("1010");
	}
	public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }
}
