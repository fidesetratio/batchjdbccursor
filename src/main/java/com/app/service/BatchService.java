package com.app.service;

import java.util.UUID;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service("batchService")
public class BatchService {
	
	@Autowired
    private JobLauncher jobLauncher;
    @Autowired
    private Job updateDeviceJob;
    
    
	public void createBatchJob(String taskId) throws Exception {
        JobParameters jobParameters = new JobParametersBuilder()
                .addString("taskId", taskId)
                .addString("uuid", UUID.randomUUID().toString().replace("-",""))
                .toJobParameters();
     
        jobLauncher.run(updateDeviceJob, jobParameters);
    }
}
