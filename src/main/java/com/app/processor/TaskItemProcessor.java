package com.app.processor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

import com.app.model.ModelRequest;
import com.app.model.ModelResponse;

@Component("taskItemProcessor")
public class TaskItemProcessor implements ItemProcessor<ModelRequest, ModelResponse> {
	private static final Logger log = LoggerFactory.getLogger(TaskItemProcessor.class);

	@Override
	public ModelResponse process(ModelRequest item) throws Exception {
		// TODO Auto-generated method stub
		log.info("model Item Processor"+item.getReg_spaj());
		ModelResponse response = new ModelResponse();
		return response;
	}


}
