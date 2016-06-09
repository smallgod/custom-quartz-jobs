/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.library.jobsandlisteners.joblistener;

import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobListener;
import org.quartz.JobDataMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author smallgod
 */
public class AdFetchJobListener  implements JobListener {
    
    private static final Logger logger = LoggerFactory.getLogger(AdFetchJobListener.class);

    private static final String LISTENER_NAME = "Retries_JOBLISTENER";
    
    @Override
    public String getName() {
        return AdFetchJobListener.LISTENER_NAME;
    }

    @Override
    public void jobToBeExecuted(JobExecutionContext jec) {
        String jobName = jec.getJobDetail().getKey().toString();
	logger.debug("JobToBeExecuted : " + jobName + " is going to start...");    }

    @Override
    public void jobExecutionVetoed(JobExecutionContext jec) { }

    @Override
    public void jobWasExecuted(JobExecutionContext jec, JobExecutionException jee) {
        logger.debug(LISTENER_NAME + " - executed!");
        JobDataMap jobMap = jec.getMergedJobDataMap();
        
        String jobName = jec.getJobDetail().getKey().getName();
        
        logger.debug("JobName: " + jobName + " was executed/deferred - Size of jobMap: " + jobMap.size());
        //CustomerIdentity customerIdentity = (CustomerIdentity)jobMap.get("customerIdentity");
    }
}
