package com.app;


import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.DefaultBatchConfigurer;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.explore.support.MapJobExplorerFactoryBean;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.MapJobRepositoryFactoryBean;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.RowMapper;

import com.app.model.ModelRequest;
import com.app.model.ModelResponse;
import com.app.processor.TaskItemProcessor;

@Configuration
public class BatchConfiguration {
	private static final Logger log = LoggerFactory.getLogger(BatchConfiguration.class);
	 @Autowired
	 public JobBuilderFactory jobBuilderFactory;
	 
	 @Autowired
	 public StepBuilderFactory stepBuilderFactory;
	 
	 @Autowired
	 public TaskItemProcessor taskItemProcessor;

	 
	 public Map<String, JobParameter> parameters;
	 
	  public Object taskId;
	  
	 String querySql = "select * from (select a.reg_spaj,a.lca_id,row_number() over (order by a.reg_spaj) r,dist.id_dist,dist.grup_report,ltb.lsgb_id,LGB.LSTB_ID,a.lscb_id, LDB.LSDBS_NAME,lgb.lsgb_linebus,a.lku_id, mpi.mspr_tsi as up,ldb.jn_bank,B.MSPRO_PROD_DATE,mpi.lsbs_id, mpi.lsdbs_number,A.COVID_FLAG,"
	 		+ "mpi.MSPR_PREMIUM as premi_pokok,"
			+ "(select  (case when mb.lku_id = '02' then nvl(sum(mpr.ape_prod * 10000), 0) else nvl(sum(mpr.ape_prod), 0) end) ape_prod "
	 		+ "from eka.mst_billing mb, eka.mst_production mpr "
			+ "where "
	 		+ "mb.reg_spaj = mpr.reg_spaj "
			+ "and mb.msbi_premi_ke = mpr.msbi_premi_ke "
	 		+ "and mb.msbi_tahun_ke = mpr.msbi_tahun_ke "
			+ "and mb.reg_spaj = a.reg_spaj "
	 		+ "and mb.msbi_nb = 1 "
			+ "and mb.MSBI_TAHUN_KE = 1 "
	 		+ "group by mb.lku_id "
			+ ") APE , "
	 		+ "  (select ber.mu_jlh_premi from eka.mst_ulink ber where ber.reg_Spaj=a.reg_spaj and ber.mu_ke in(1,2,3) and ber.lt_id=5 and ber.reg_Spaj=a.reg_Spaj and rownum=1) premi_topup "
	 		+ " from "
	 		+ "eka.mst_policy a,eka.mst_production b,EKA.MST_PRODUCT_INSURED mpi,eka.mst_insured mi,eka.lst_det_bisnis ldb, eka.lst_bisnis ltb, "
	 		+ "EKA.LST_GROUP_BISNIS lgb, eka.lst_cabang e,EKA.LST_JALUR_DIST dist "
	 		+ "where "
	 		+ "a.lspd_id in (6,7) and a.lssp_id = 1 "
	 		+ "and e.lca_id = a.lca_id "
	 		+ "and e.jalurdis = dist.id_dist "
	 		+ "and a.reg_spaj = mpi.reg_spaj "
	 		+ "and LGB.LSGB_ID = ltb.lsgb_id "
	 		+ "and a.reg_spaj = mi.reg_spaj "
	 		+ "and MPI.LSBS_ID = LDB.LSBS_ID "
	 		+ "and MPI.LSDBS_NUMBER = LDB.LSDBS_NUMBER "
	 		+ "and ltb.lsbs_id = MPI.LSBS_ID "
	 		+ "and MPI.LSBS_ID <=300 "
	 		+ "and (a.reg_spaj = b.reg_spaj) "
	 		+ "and b.mspro_prod_ke = 1 "
	 		+ "and b.mspro_prod_date >= to_date('16/11/2020','dd/MM/yyyy') "
	 		+ "and  not exists(select 1 from eka.lst_welcome_call_status wcs where wcs.reg_spaj = a.reg_spaj and wcs.retry = 0)) ";
	 		
	 @Bean
	    DefaultBatchConfigurer batchConfigurer() {
	        return new DefaultBatchConfigurer() {

	            private JobRepository jobRepository;
	            private JobExplorer jobExplorer;
	            private JobLauncher jobLauncher;

	            {
	                MapJobRepositoryFactoryBean jobRepositoryFactory = new MapJobRepositoryFactoryBean();
	                try {
	                    this.jobRepository = jobRepositoryFactory.getObject();
	                    MapJobExplorerFactoryBean jobExplorerFactory = new MapJobExplorerFactoryBean(jobRepositoryFactory);
	                    this.jobExplorer = jobExplorerFactory.getObject();
	                    SimpleJobLauncher jobLauncher = new SimpleJobLauncher();
	                    jobLauncher.setJobRepository(jobRepository);
	                    jobLauncher.afterPropertiesSet();
	                    this.jobLauncher = jobLauncher;

	                } catch (Exception e) {
	                }
	            }

	            @Override
	            public JobRepository getJobRepository() {
	                return jobRepository;
	            }

	            @Override
	            public JobExplorer getJobExplorer() {
	                return jobExplorer;
	            }

	            @Override
	            public JobLauncher getJobLauncher() {
	                return jobLauncher;
	            }
	        };
	    }
	 	@Bean
	    @StepScope
	    public JdbcCursorItemReader<ModelRequest> itemReader(DataSource dataSource) {
	 		
	 		log.info("query :"+ querySql);
	 		 return new JdbcCursorItemReaderBuilder<ModelRequest>()
	                 .name("itemReader")
	                 .sql(querySql)
	                 .dataSource(dataSource)
	               //  .queryArguments(new Object[]{parameters.get("taskId").getValue()})
	                 .rowMapper(new RowMapper<ModelRequest>() {

						@Override
						public ModelRequest mapRow(ResultSet rs, int rowNum) throws SQLException {
							// TODO Auto-generated method stub
							ModelRequest modelRequest = new ModelRequest();
							modelRequest.setReg_spaj(rs.getString("REG_SPAJ"));
							return modelRequest;
						}
					})
	                 .build();
	 	}
	 	
	 	
	 	  	@Bean
	 	    @StepScope
	 	    public ItemWriter<ModelResponse> itemWriter() {
	 	  		return new ItemWriter<ModelResponse>() {

					@Override
					public void write(List<? extends ModelResponse> items) throws Exception {
						// TODO Auto-generated method stub
						log.info("items size:"+items.size());
					}

	 	  		};
	 	  }
	 	   
	 	  	
	 	    @Bean
	 	    public Job updateDeviceJob(Step updateMstInbox) {
	 	        return jobBuilderFactory.get(UUID.randomUUID().toString().replace("-", ""))
	 	                .listener(new JobListener()) 
	 	                .flow(updateMstInbox)
	 	                .end()
	 	                .build();
	 	    }
	 	    @Bean
	 	    public Step updateMstInbox(JdbcCursorItemReader<ModelRequest> itemReader,ItemWriter<ModelResponse> itemWriter) {
	 	        return stepBuilderFactory.get(UUID.randomUUID().toString().replace("-", ""))
	 	                .<ModelRequest, ModelResponse> chunk(500)
	 	                .reader(itemReader)
	 	                .processor(taskItemProcessor)
	 	                .writer(itemWriter)
	 	                .build();
	 	    }
	 	    
	 	    
	 	    
	 	   public class JobListener implements JobExecutionListener {

			@Override
			public void beforeJob(JobExecution jobExecution) {
				// TODO Auto-generated method stub
				 log.info(jobExecution.getJobInstance().getJobName() + " before... ");
		            parameters = jobExecution.getJobParameters().getParameters();
		            if(parameters.get("taskId") != null)
		            taskId = parameters.get("taskId").getValue();
		            
		            if(taskId != null)
		            log.info("job param taskId : " + parameters.get("taskId"));
		     
			}

			@Override
			public void afterJob(JobExecution jobExecution) {
				// TODO Auto-generated method stub
				  log.info(jobExecution.getJobInstance().getJobName() + " after... ");
		          
			}
	 		   
	 	   }


}
