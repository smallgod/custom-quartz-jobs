package com.library.jobsandlisteners.job;

import com.library.httpconnmanager.HttpClientPool;
import com.library.configs.JobsConfig;
import com.library.customexception.MyCustomException;
import com.library.datamodel.Constants.AdPaymentStatus;
import com.library.datamodel.Constants.CampaignStatus;
import com.library.datamodel.Constants.EntityName;
import com.library.datamodel.Constants.NamedConstants;
import com.library.datamodel.Json.DBSaveResponse;
import com.library.datamodel.model.v1_0.AdPaymentDetails;
import com.library.datamodel.model.v1_0.AdProgram;
import com.library.dbadapter.DatabaseAdapter;
import com.library.scheduler.CustomJobScheduler;
import com.library.sgsharedinterface.ExecutableJob;
import org.quartz.InterruptableJob;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.UnableToInterruptJobException;
import com.library.sgsharedinterface.RemoteRequest;
import com.library.utilities.DateUtils;
import com.library.sglogger.util.LoggerUtil;
import com.library.utilities.SMSSenderUtils;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.joda.time.LocalDateTime;

/**
 * The campaign Processor Job class fetches campaigns at different stages from
 * the database and sends them to the appropriate processors or moves them to
 * next steps
 *
 * @author smallgod
 */
public class AdCampaignProcessorJob implements Job, InterruptableJob, ExecutableJob {

    /*
    DRAFT("DRAFT"), //ad still in draft form, not yet placed
    NEW("NEW"), //An ad just placed
    PENDING_PAYMENT("PENDING PAYMENT"), //ad picked for payment
    IN_REVIEW("IN REVIEW"),
    ACTIVE("ACTIVE"), //when scheduled
    COMPLETED("COMPLETED"), //when reached end_date
    FLAGGED("FLAGGED"),
    REJECTED("REJECTED"),
    REVERSED("REVERSED");

    LOG(1),
    AUTHORISE(2),
    PAY(3),
    OPTIMISE(4),
    SCHEDULE(5),
    DISPLAY(5),
    FINAL(6); //successful or customer refunded
     */
    private static final LoggerUtil logger = new LoggerUtil(AdCampaignProcessorJob.class);

    @Override
    public void execute(JobExecutionContext jec) throws JobExecutionException {

        //synchronized (NamedConstants.FETCH_CAMPAIGNS_MUTEX) {
        try {

            boolean hasAcquiredLock = NamedConstants.FETCH_CAMPAIGNS_LOCK.tryLock(50, TimeUnit.SECONDS);

            if (hasAcquiredLock) {

                JobDetail thisJobDetail = jec.getJobDetail();
                String thisJobName = thisJobDetail.getKey().getName();

                JobDataMap jobsDataMap = jec.getMergedJobDataMap();
                JobsConfig thisJobsData = (JobsConfig) jobsDataMap.get(thisJobName);
                JobsConfig secondJobsData = (JobsConfig) jobsDataMap.get(NamedConstants.SECOND_JOBSDATA);

                RemoteRequest dbManagerUnit = thisJobsData.getRemoteUnitConfig().getAdDbManagerRemoteUnit();
                HttpClientPool clientPool = (HttpClientPool) jobsDataMap.get(NamedConstants.CLIENT_POOL);
                DatabaseAdapter databaseAdapter = (DatabaseAdapter) jobsDataMap.get(NamedConstants.DB_ADAPTER);

                Boolean triggerNow = Boolean.FALSE;
                Object triggerNowObj = jobsDataMap.get(NamedConstants.TRIGGER_NOW_CAMPAIGNPROCESSOR);

                if (null != triggerNowObj) {
                    triggerNow = (Boolean) triggerNowObj;
                }

                //TriggerNow request from central-unit processor
                if (triggerNow) {

                    logger.debug("CAMPAIGN_PROCESS TRIGGER_NOW called!!");

                    AdProgram program = (AdProgram) jobsDataMap.get(NamedConstants.CAMPAIGN_DETAILS);
                    processAdCampaign(program, thisJobsData, secondJobsData, clientPool, databaseAdapter);

                } else {

                    Map<String, Object> resourceProps = new HashMap<>();
                    resourceProps.put("campaignStatuses", new HashSet<>(Arrays.asList(CampaignStatus.NEW, CampaignStatus.PENDING_PAYMENT, CampaignStatus.PROCESSING, CampaignStatus.IN_REVIEW)));

                    Set<AdProgram> campaignPrograms = databaseAdapter.fetchEntitiesByNamedQuery(EntityName.AD_PROGRAM, AdProgram.FETCH_CAMPAIGNS_BY_STATUS, resourceProps);

                    if (!(null == campaignPrograms || campaignPrograms.isEmpty())) {

                        for (AdProgram program : campaignPrograms) {
                            processAdCampaign(program, thisJobsData, secondJobsData, clientPool, databaseAdapter);
                        }

                    } else {
                        logger.debug("No new Campaigns found!");
                    }

                }

            } else {
                logger.warn("Failed to acquire Lock, wait time expired");
            }

        } catch (InterruptedException e) {

            logger.error("Interrrupted exception: " + e.getMessage());
            e.printStackTrace();

        } catch (MyCustomException ex) {

            logger.error("MyCustomException : " + ex.getMessage());
            ex.printStackTrace();

        } catch (Exception ex) {

            logger.error("An Error occurred in AdCampaignProcessorJob: " + ex.getMessage());
            ex.printStackTrace();

        } finally {

            logger.debug("Releasing fetch-campaigns lock!");
            NamedConstants.FETCH_CAMPAIGNS_LOCK.unlock();
        }

        //}
    }

    /**
     * Trigger the campaign processor to execute now
     *
     * @param paymentDetails
     * @param campaignProcessorJobsData
     * @param paymentProcessorJobsData
     * @param clientPool
     * @return
     * @throws MyCustomException
     */
    public boolean triggerPaymentProcessor(AdPaymentDetails paymentDetails, JobsConfig campaignProcessorJobsData, JobsConfig paymentProcessorJobsData, HttpClientPool clientPool) throws MyCustomException {

        CustomJobScheduler jobScheduler = new CustomJobScheduler(clientPool);

        JobDataMap jobsDataMap = jobScheduler.createJobDataMap(campaignProcessorJobsData, paymentProcessorJobsData);
        jobsDataMap.put(NamedConstants.TRIGGER_NOW_PAYPROCESSOR, Boolean.TRUE);
        jobsDataMap.put(NamedConstants.PAYMENTS_DETAILS, paymentDetails);

        String paymentJobName = paymentProcessorJobsData.getJobName();
        String paymentGroupName = paymentProcessorJobsData.getJobGroupName();

        jobScheduler.triggerJobNow(paymentJobName, paymentGroupName, jobsDataMap);

        boolean isTriggered = jobScheduler.triggerJobNow(paymentJobName, paymentGroupName, jobsDataMap);

        return isTriggered;

    }

    /**
     *
     * @param program
     * @param campaignProcessorJobsData
     * @param paymentProcessorJobsData
     * @param clientPool
     * @param databaseAdapter
     * @throws MyCustomException
     */
    private void processAdCampaign(AdProgram program, JobsConfig campaignProcessorJobsData, JobsConfig paymentProcessorJobsData, HttpClientPool clientPool, DatabaseAdapter databaseAdapter) throws MyCustomException {

        boolean isProgramReviewed = program.isIsReviewed();
        boolean isProgramToBeReviewd = program.isIsToBeReviewed();
        int id = (int) program.getId();
        int sameStatusPick = program.getSameStatusPick();
        CampaignStatus status = program.getAdCampaignStatus();

        AdPaymentDetails paymentDetails = program.getAdPaymentDetails();
        AdPaymentStatus paymentStatus = paymentDetails.getPaymentStatus();
        int campaignCost = paymentDetails.getAmount().getAmount();
        String currency = paymentDetails.getAmount().getCurrencycode();
        String createTime = DateUtils.convertLocalDateTimeToString(program.getCreatedOn(), NamedConstants.DATE_TIME_DASH_FORMAT);
        String payerAccount = paymentDetails.getPayerAccount();
        LocalDateTime timeOfLastStatusChange = program.getStatusChangeTime();

        switch (status) {

            case NEW: //move to PENDING_PAYMENT

                //send one sms with all new campaigns or send an sms for each new campaign??
                logger.warn("A new payment has been logged at: " + DateUtils.convertLocalDateTimeToString(timeOfLastStatusChange, NamedConstants.DATE_TIME_DASH_FORMAT));
                SMSSenderUtils.generateAndSendNewCampaignMsg(createTime, campaignCost, payerAccount, NamedConstants.ADMIN_SMS_RECIPIENT, clientPool);
                
                moveCampaignToNextStep(program, CampaignStatus.PENDING_PAYMENT, databaseAdapter);
                triggerPaymentProcessor(paymentDetails, campaignProcessorJobsData, paymentProcessorJobsData, clientPool);
                break;

            case PENDING_PAYMENT: //a txn PENDING_PAYMENT needs to sent for payment before being moved to PROCESSING

                if (sameStatusPick > 0) {
                    //check how long it's been pending_pending & see what todo
                    logger.warn("Payment is still pending payment");
                }

                if (paymentStatus == AdPaymentStatus.PAY_INITIATED) {
                    moveCampaignToNextStep(program, CampaignStatus.PROCESSING, databaseAdapter);

                } else {

                    incrementNoCampaignStatusChange(program, databaseAdapter);//status remains same, flag is incremented
                    triggerPaymentProcessor(paymentDetails, campaignProcessorJobsData, paymentProcessorJobsData, clientPool);

                }
                break;

            case PROCESSING:

                if (sameStatusPick > 0) {
                    //check how long it's been pending_pending & see what todo
                    //timeOfLastStatusChange
                    logger.warn("Payment is still pending payment");
                }

                if (null != paymentStatus) {
                    //1. optimise ad images/videos
                    switch (paymentStatus) {

                        case PAID:

                            moveCampaignToNextStep(program, CampaignStatus.IN_REVIEW, databaseAdapter);
                            break;

                        case PAY_FAILED:

                            program.setDescription("Campaign rejected because required payment of: " + currency + " " + campaignCost + " was not made");
                            moveCampaignToNextStep(program, CampaignStatus.REJECTED, databaseAdapter);
                            break;

                        case PAY_REVERSED:

                            program.setDescription("Campaign rejected because required payment of: " + currency + " " + campaignCost + " was reversed");
                            moveCampaignToNextStep(program, CampaignStatus.REJECTED, databaseAdapter);
                            break;

                        case PAY_INITIATED:

                            logger.warn("Payment is still in initiated state/not completed");
                            incrementNoCampaignStatusChange(program, databaseAdapter);//status remains same, flag is incremented
                            break;

                        case STATUS_UNKNOWN:

                            SMSSenderUtils.generateAndSendCampaignEscalateMsg(id, payerAccount, NamedConstants.ADMIN_SMS_RECIPIENT, clientPool);
                            program.setDescription("Campaign escalated for manual intervention due to the payment status not being readily available");
                            moveCampaignToNextStep(program, CampaignStatus.ESCALATED, databaseAdapter);
                            break;

                        default:
                            incrementNoCampaignStatusChange(program, databaseAdapter);//status remains same, flag is incremented
                            break;
                    }
                }
                break;

            case IN_REVIEW:

                if (sameStatusPick > 0) {
                    logger.warn("Payment is still pending payment");
                }

                if (isProgramReviewed || !isProgramToBeReviewd) {
                    moveCampaignToNextStep(program, CampaignStatus.ACTIVE, databaseAdapter);

                } else {
                    SMSSenderUtils.generateAndSendCampaignReviewAdminMsg(id, payerAccount, NamedConstants.ADMIN_SMS_RECIPIENT, clientPool);
                    incrementNoCampaignStatusChange(program, databaseAdapter);//status remains same, flag is incremented

                }
                break;

            case ACTIVE:
                break;

            case COMPLETED:
                break;

            case FLAGGED:
                break;

            case REJECTED:
                break;

            case DRAFT:
                break;

            case REVERSED:
                break;

            case ESCALATED:
                break;

            default:
                break;
        }
    }

    /**
     * Move a campaign to the next step
     *
     * @param programToMove the campaign to move
     * @param nextStep the next step to move the campaign to
     * @param dbAdapter connection to the database
     * @return True if campaign is moved to next step
     * @throws com.library.customexception.MyCustomException
     */
    public boolean moveCampaignToNextStep(AdProgram programToMove, CampaignStatus nextStep, DatabaseAdapter dbAdapter) throws MyCustomException {

        programToMove.setAdCampaignStatus(nextStep);
        programToMove.setStatusChangeTime(DateUtils.getDateTimeNow());
        programToMove.setSameStatusPick(0);
        DBSaveResponse response = dbAdapter.saveOrUpdateEntity(programToMove, Boolean.FALSE);

        return response.getSuccess();

    }

    /**
     * Increment the flag to show that the number of times this campaign has
     * maintained the same status
     *
     * @param programInSameStatus
     * @param dbAdapter
     * @return True if sameStatusPick flag is incremented
     * @throws MyCustomException
     */
    public boolean incrementNoCampaignStatusChange(AdProgram programInSameStatus, DatabaseAdapter dbAdapter) throws MyCustomException {

        programInSameStatus.setSameStatusPick(programInSameStatus.getSameStatusPick() + 1);
        DBSaveResponse response = dbAdapter.saveOrUpdateEntity(programInSameStatus, Boolean.FALSE);

        return response.getSuccess();

    }

    @Override
    public void interrupt() throws UnableToInterruptJobException {
        logger.warn("Failed to interrupt a Job before deleting it..");
    }
}
