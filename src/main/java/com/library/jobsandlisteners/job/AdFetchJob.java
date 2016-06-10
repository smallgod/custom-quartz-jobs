/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.library.jobsandlisteners.job;

import com.library.datamodel.Constants.ProcessingUnitState;
import com.library.datamodel.Json.AdFetchRequest;
import com.library.httpconnmanager.HttpClientPool;
import com.library.scheduler.JobsData;
import com.library.sgsharedinterface.ExecutableJob;
import com.library.utilities.GeneralUtils;
import org.quartz.InterruptableJob;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.UnableToInterruptJobException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The class doing the work
 *
 * @author smallgod
 */
public class AdFetchJob implements Job, InterruptableJob, ExecutableJob {

    private static final Logger logger = LoggerFactory.getLogger(AdFetchJob.class);

    @Override
    public void execute(JobExecutionContext jec) throws JobExecutionException {

        JobDetail jobDetail = jec.getJobDetail();
        String jobName = jobDetail.getKey().getName();

        JobDataMap jobsDataMap = jec.getMergedJobDataMap();

        JobsData jobsData = (JobsData) jobsDataMap.get(jobName);

        //logger.debug("size of jobMap: " + jobMap.size());
        /*logger.debug("sleeping for 30s at: " + new DateTime().getSecondOfDay());
         try {
         logger.debug("mimick job execution.....");
         Thread.sleep(30000L);
         } catch (InterruptedException ex) {logger.debug("error trying to sleep: " + ex.getMessage());
         }
         logger.debug("done sleeping for 30s at: " + new DateTime().getSecondOfDay());
         */
        AdFetchRequest request = new AdFetchRequest();
        request.setMethodName("ADFETCH_REQUEST");
        AdFetchRequest.Params params = request.new Params();
        params.setStatus(ProcessingUnitState.POLL.getValue()); //need Enum
        request.setParams(params);

        String jsonRequest = GeneralUtils.convertToJson(request, AdFetchRequest.class);

        logger.debug("New AdFetch Request: " + jsonRequest);

        String remoteUrl = jobsData.getRemoteUrl();
        HttpClientPool clientPool = jobsData.getHttpClientPool();

        String response = clientPool.sendRemoteRequest(jsonRequest, remoteUrl, "username", "password");

        logger.info("Response from Central Server: " + response);

    }

    @Override
    public void interrupt() throws UnableToInterruptJobException {
        logger.warn("Failed to interrupt a Job before deleting it..");
    }

}
