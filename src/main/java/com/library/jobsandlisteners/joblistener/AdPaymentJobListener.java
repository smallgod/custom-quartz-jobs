/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.library.jobsandlisteners.joblistener;

import com.library.sglogger.util.LoggerUtil;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobListener;
import org.quartz.JobDataMap;

/**
 *
 * @author smallgod
 */
public class AdPaymentJobListener  implements JobListener {
    
    private static final LoggerUtil logger = new LoggerUtil(AdPaymentJobListener.class);
    private static final String LISTENER_NAME = "AdPaymentJob_JOB_LISTENER";
    
    @Override
    public String getName() {
        return AdPaymentJobListener.LISTENER_NAME;
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