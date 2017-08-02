package com.library.jobsandlisteners.job;

import com.library.httpconnmanager.HttpClientPool;
import com.library.configs.JobsConfig;
import com.library.customexception.MyCustomException;
import com.library.datamodel.Constants.AdPaymentStatus;
import com.library.datamodel.Constants.AdSlotsReserve;
import com.library.datamodel.Constants.CampaignStatus;
import com.library.datamodel.Constants.EntityName;
import com.library.datamodel.Constants.ErrorCode;
import com.library.datamodel.Constants.FetchStatus;
import com.library.datamodel.Constants.NamedConstants;
import com.library.datamodel.Json.DBSaveResponse;
import com.library.datamodel.Json.TimeSlot;
import com.library.datamodel.model.v1_0.AdClient;
import com.library.datamodel.model.v1_0.AdPaymentDetails;
import com.library.datamodel.model.v1_0.AdProgram;
import com.library.datamodel.model.v1_0.AdProgramSlot;
import com.library.datamodel.model.v1_0.AdSchedule;
import com.library.datamodel.model.v1_0.AdScreen;
import com.library.datamodel.model.v1_0.AdTimeSlot;
import com.library.dbadapter.DatabaseAdapter;
import com.library.hibernate.CustomHibernate;
import com.library.hibernate.utils.HibernateUtils;
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
import com.library.utilities.CampaignUtilities;
import com.library.utilities.GeneralUtils;
import com.library.utilities.SMSSenderUtils;
import java.time.DayOfWeek;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDate;
import org.joda.time.LocalDateTime;
import org.joda.time.LocalTime;
import org.openide.util.Exceptions;

/**
 * The campaign Processor Job class fetches campaigns at different stages from
 * the database and sends them to the appropriate processors or moves them to
 * next steps
 *
 * @author smallgod
 */
public class AdCampaignProcessorJob implements Job, InterruptableJob, ExecutableJob {

    ExecutorService taskExecutorService = Executors.newFixedThreadPool(5);

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
    private final Set<Integer> ADHOUR_SLOT_ALLOCATION_ORDER = GeneralUtils.allocateSlotsForAnHour();

    @Override
    public void execute(JobExecutionContext jec) throws JobExecutionException {

        //synchronized (NamedConstants.FETCH_CAMPAIGNS_MUTEX) {
        try {

            boolean hasAcquiredLock = NamedConstants.FETCH_CAMPAIGNS_LOCK.tryLock(30, TimeUnit.SECONDS);

            if (hasAcquiredLock) {

                JobDetail thisJobDetail = jec.getJobDetail();
                String thisJobName = thisJobDetail.getKey().getName();

                JobDataMap jobsDataMap = jec.getMergedJobDataMap();
                JobsConfig thisJobsData = (JobsConfig) jobsDataMap.get(thisJobName);
                JobsConfig secondJobsData = (JobsConfig) jobsDataMap.get(NamedConstants.SECOND_JOBSDATA);
                JobsConfig thirdJobsData = (JobsConfig) jobsDataMap.get(NamedConstants.THIRD_JOBSDATA);

                RemoteRequest dbManagerUnit = thisJobsData.getRemoteUnitConfig().getAdDbManagerRemoteUnit();
                HttpClientPool clientPool = (HttpClientPool) jobsDataMap.get(NamedConstants.CLIENT_POOL);
                //DatabaseAdapter databaseAdapter = (DatabaseAdapter) jobsDataMap.get(NamedConstants.EXTERNAL_DB_ACCESS);
                CustomHibernate customHibernate = (CustomHibernate) jobsDataMap.get(NamedConstants.INTERNAL_DB_ACCESS);

                Boolean triggerNow = Boolean.FALSE;
                Object triggerNowObj = jobsDataMap.get(NamedConstants.TRIGGER_NOW_CAMPAIGNPROCESSOR);

                if (null != triggerNowObj) {
                    triggerNow = (Boolean) triggerNowObj;
                }

                logger.info("CAMPAIGN JOB called to EXECUTE - triggered NOW == " + triggerNow);
                logger.info("Name of thread is: " + Thread.currentThread().getName() + " id: " + Thread.currentThread().getId() + " string: " + Thread.currentThread().toString());

                //TriggerNow request from central-unit processor
                if (triggerNow) {

                    logger.debug("CAMPAIGN_PROCESS TRIGGER_NOW called!!");

                    AdProgram program = (AdProgram) jobsDataMap.get(NamedConstants.CAMPAIGN_DETAILS);
                    processAdCampaign(program, thisJobsData, secondJobsData, thirdJobsData, clientPool, customHibernate);

                } else {

                    Map<String, Object> resourceProps = new HashMap<>();
                    resourceProps.put("campaignStatuses", new HashSet<>(Arrays.asList(CampaignStatus.NEW, CampaignStatus.PENDING_PAYMENT, CampaignStatus.IN_REVIEW)));

                    //Set<AdProgram> campaignPrograms = databaseAdapter.fetchEntitiesByNamedQuery(EntityName.AD_PROGRAM, AdProgram.FETCH_CAMPAIGNS_BY_STATUS, resourceProps);
                    Set<AdProgram> campaignPrograms = customHibernate.fetchEntities(AdProgram.FETCH_CAMPAIGNS_BY_STATUS, resourceProps);

                    if (!(null == campaignPrograms || campaignPrograms.isEmpty())) {

                        for (AdProgram program : campaignPrograms) {
                            processAdCampaign(program, thisJobsData, secondJobsData, thirdJobsData, clientPool, customHibernate);
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

            logger.debug("Releasing FETCH-CAMPAIGNS lock!");
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
     * @param customHibernate
     * @return
     */
    public boolean triggerPaymentProcessor(AdPaymentDetails paymentDetails, JobsConfig campaignProcessorJobsData, JobsConfig paymentProcessorJobsData, HttpClientPool clientPool, CustomHibernate customHibernate) {

        logger.info("TRIGGERING PAYMENT at: " + DateUtils.getDefaultDateTimeNow() + ", WITH STATUS:: " + paymentDetails.getPaymentStatus());

        boolean isTriggered = Boolean.FALSE;

        if (paymentDetails.getPaymentStatus() == AdPaymentStatus.PAY_NEW) {

            try {
                CustomJobScheduler jobScheduler = new CustomJobScheduler(clientPool, customHibernate, null);

                JobDataMap jobsDataMap = jobScheduler.createJobDataMap(campaignProcessorJobsData, paymentProcessorJobsData);
                jobsDataMap.put(NamedConstants.TRIGGER_NOW_PAYPROCESSOR, Boolean.TRUE);
                jobsDataMap.put(NamedConstants.PAYMENTS_DETAILS, paymentDetails);
                jobsDataMap.put(NamedConstants.PAYMENTS_ID, paymentDetails.getId());

                String paymentJobName = paymentProcessorJobsData.getJobName();
                String paymentGroupName = paymentProcessorJobsData.getJobGroupName();

                jobScheduler.triggerJobNow(paymentJobName, paymentGroupName, jobsDataMap);

                isTriggered = jobScheduler.triggerJobNow(paymentJobName, paymentGroupName, jobsDataMap);

            } catch (MyCustomException ex) {
                Exceptions.printStackTrace(ex);
            }

        } else {

            logger.warn("Can't trigger PaymentProcessor, paymentStatus is not 'PAY_NEW'");
        }

        return isTriggered;

    }

    /**
     * Trigger the AdDisplay Processor to execute now
     *
     * @param paymentDetails
     * @param campaignProcessorJobsData
     * @param paymentProcessorJobsData
     * @param adDisplayProcessorJobsData
     * @param clientPool
     * @param customHibernate
     * @return
     */
    public boolean triggerAdDisplayProcessor(AdProgram paymentDetails, JobsConfig campaignProcessorJobsData, JobsConfig paymentProcessorJobsData, JobsConfig adDisplayProcessorJobsData, HttpClientPool clientPool, CustomHibernate customHibernate) {

        boolean isTriggered = Boolean.FALSE;

        if (paymentDetails.getAdCampaignStatus() == CampaignStatus.ACTIVE) {

            try {
                CustomJobScheduler jobScheduler = new CustomJobScheduler(clientPool, customHibernate, null);

                JobDataMap jobsDataMap = jobScheduler.createJobDataMap(campaignProcessorJobsData, paymentProcessorJobsData, adDisplayProcessorJobsData);
                jobsDataMap.put(NamedConstants.TRIGGER_NOW_DISPLAYPROCESSOR, Boolean.TRUE);

                String jobName = adDisplayProcessorJobsData.getJobName();
                String groupName = adDisplayProcessorJobsData.getJobGroupName();

                jobScheduler.triggerJobNow(jobName, groupName, jobsDataMap);

                isTriggered = jobScheduler.triggerJobNow(jobName, groupName, jobsDataMap);

            } catch (MyCustomException ex) {
                Exceptions.printStackTrace(ex);
            }

        } else {

            logger.warn("Can't trigger AdDisplayProcessor, CampaignStatus is not 'ACTIVE'");
        }

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
    private void processAdCampaign(AdProgram program, JobsConfig campaignProcessorJobsData, JobsConfig paymentProcessorJobsData, JobsConfig adDisplayProcessorJobsData, HttpClientPool clientPool, CustomHibernate customHibernate) throws MyCustomException {

        boolean isProgramReviewed = program.isIsReviewed();
        boolean isProgramToBeReviewd = program.isIsToBeReviewed();
        int id = (int) program.getId();
        int sameStatusPick = program.getSameStatusPick();
        CampaignStatus status = program.getAdCampaignStatus();

        AdPaymentDetails paymentDetails = program.getAdPaymentDetails();
        AdSlotsReserve reservation = program.getAdSlotReserve();
        AdPaymentStatus paymentStatus = paymentDetails.getPaymentStatus();
        int campaignCost = paymentDetails.getAmount().getAmount();
        String currency = paymentDetails.getAmount().getCurrencycode();
        String createTime = DateUtils.convertLocalDateTimeToString(program.getCreatedOn(), NamedConstants.DATE_TIME_DASH_FORMAT);
        String payerAccount = paymentDetails.getPayerAccount();
        LocalDateTime timeOfLastStatusChange = program.getStatusChangeTime();

        switch (status) {

            case NEW: //move to PENDING_PAYMENT

                if (sameStatusPick > 0) {
                    //check how long it's been pending_pending & see what todo
                    logger.warn("Campaign is still NEW");
                } else {
                    //send one sms with all new campaigns or send an sms for each new campaign??
                    SMSSenderUtils.generateAndSendNewCampaignMsg(createTime, campaignCost, payerAccount, NamedConstants.ADMIN_SMS_RECIPIENT, clientPool);
                }
                //temporarily reserve booked ad slots pending payment
                program.setAdSlotReserve(AdSlotsReserve.TEMPORAL);
                program.setDescription("Campaign is pending payment before scheduling");
                moveCampaignToNextStep(program, CampaignStatus.PENDING_PAYMENT, customHibernate);
                triggerPaymentProcessor(program.getAdPaymentDetails(), campaignProcessorJobsData, paymentProcessorJobsData, clientPool, customHibernate);

                break;

            case PENDING_PAYMENT: //Pay before Review

                if (sameStatusPick > 0) {
                    //check how long it's been pending_pending & see what todo
                    logger.warn("Campaign is still pending payment");
                }

                CampaignStatus nextStep = status;
                String campaignStatusDesc = "";

                if (null != paymentStatus) {

                    switch (paymentStatus) {

                        case PAY_INITIATED:
                            logger.warn("Payment is still in initiated state/not completed");
                            break;

                        case PAID:

                            nextStep = CampaignStatus.IN_REVIEW;
                            campaignStatusDesc = "Campaign is being reviewed before scheduling";
                            break;

                        case PAY_FAILED:
                            nextStep = CampaignStatus.REJECTED;
                            campaignStatusDesc = "Campaign rejected because required payment of: " + currency + " " + campaignCost + " was not made";
                            break;

                        case PAY_REVERSED:
                            nextStep = CampaignStatus.ESCALATED;
                            campaignStatusDesc = "Campaign rejected because required payment of: " + currency + " " + campaignCost + " was reversed";
                            break;

                        case STATUS_UNKNOWN:
                            SMSSenderUtils.generateAndSendCampaignEscalateMsg(id, payerAccount, NamedConstants.ADMIN_SMS_RECIPIENT, clientPool);
                            nextStep = CampaignStatus.ESCALATED;
                            campaignStatusDesc = "Campaign escalated for manual intervention due to the payment status not being readily available";
                            break;

                        case PAY_NEW:
                            logger.warn("Payment is still PAY_NEW");
                            break;

                        default:
                            break;
                    }

                    if (nextStep == status) {
                        //no status change
                        incrementNoCampaignStatusChange(program, customHibernate);
                        //trigger payment if new - only trigger this once when campaign in new
                        //triggerPaymentProcessor(paymentDetails, campaignProcessorJobsData, paymentProcessorJobsData, clientPool, customHibernate);

                    } else {
                        program.setDescription(campaignStatusDesc);
                        program.setAdSlotReserve(reservation);
                        moveCampaignToNextStep(program, nextStep, customHibernate);
                    }
                }
                break;

            case IN_REVIEW:

                if (sameStatusPick > 0) {
                    logger.warn("Payment is still pending payment");
                } else {
                    SMSSenderUtils.generateAndSendCampaignReviewAdminMsg(id, payerAccount, NamedConstants.ADMIN_SMS_RECIPIENT, clientPool);

                }

                if (isProgramReviewed || !isProgramToBeReviewd) {
                    //if scheduled
                    if (bookNewCampaignSchedule(program, customHibernate)) {
                        program.setAdSlotReserve(AdSlotsReserve.FIXED);
                        moveCampaignToNextStep(program, CampaignStatus.ACTIVE, customHibernate);
                    }

                } else {
                    incrementNoCampaignStatusChange(program, customHibernate);//status remains same, flag is incremented

                }
                break;

            case ACTIVE:
                if (sameStatusPick == 0) {
                    triggerAdDisplayProcessor(program, campaignProcessorJobsData, paymentProcessorJobsData, adDisplayProcessorJobsData, clientPool, customHibernate);

                }
                break;

            case COMPLETED:
                break;

            case FLAGGED:
                break;

            case REJECTED:
                //delete/release booked scheduled/slots if booked before
                reservation = AdSlotsReserve.FREE;

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
     *
     * @param paymentId
     * @param dbAdapter
     * @return
     * @throws MyCustomException
     */
    AdPaymentDetails getLatestPaymentDetailsById(long paymentId, DatabaseAdapter dbAdapter) throws MyCustomException {

        Map<String, Object> resourceProps = new HashMap<>();
        resourceProps.put("id", new HashSet<>(Arrays.asList(paymentId)));
        Set<AdPaymentDetails> payments = dbAdapter.fetchEntity(EntityName.AD_PAYMENT, resourceProps, NamedConstants.ALL_COLUMNS);
        AdPaymentDetails payment = (AdPaymentDetails) payments.toArray()[0];

        return payment;
    }

    /**
     * Move a campaign to the next step
     *
     * @param programToMove the campaign to move
     * @param suggestedNextStep the suggested next step to move the campaign to
     * @param customHibernate
     * @throws com.library.customexception.MyCustomException
     */
    public void moveCampaignToNextStep(AdProgram programToMove, CampaignStatus suggestedNextStep, CustomHibernate customHibernate) throws MyCustomException {

        CampaignStatus nextStep = suggestedNextStep;

//        //move to next step depends on factors such as payment status
//        AdPaymentDetails payment = getLatestPaymentDetailsById(programToMove.getAdPaymentDetails().getId(), dbAdapter);
//
//        programToMove.setAdPaymentDetails(payment);
//        programToMove.setAdCampaignStatus(suggestedNextStep);
//        programToMove.setStatusChangeTime(DateUtils.getDateTimeNow());
//        programToMove.setSameStatusPick(0);
//        DBSaveResponse response = dbAdapter.saveOrUpdateEntity(programToMove, Boolean.FALSE);
//        return response.getSuccess();
        customHibernate.updateCampaignStatusChangeColumns(suggestedNextStep, programToMove.getAdSlotReserve(), programToMove.getDescription(), 0, DateUtils.getDateTimeNow(), programToMove.getId());
    }

    /**
     * Increment the flag to show that the number of times this campaign has
     * maintained the same status
     *
     * @param programInSameStatus
     * @param customHibernate
     * @throws MyCustomException
     */
    public void incrementNoCampaignStatusChange(AdProgram programInSameStatus, CustomHibernate customHibernate) throws MyCustomException {

//        AdPaymentDetails payment = getLatestPaymentDetailsById(programInSameStatus.getAdPaymentDetails().getId(), dbAdapter);
//
//        programInSameStatus.setAdPaymentDetails(payment);
//        programInSameStatus.setSameStatusPick(programInSameStatus.getSameStatusPick() + 1);
//        DBSaveResponse response = dbAdapter.saveOrUpdateEntity(programInSameStatus, Boolean.FALSE);
//        return response.getSuccess();
        customHibernate.updateCampaignSameStatusColumns(programInSameStatus.getSameStatusPick() + 1, programInSameStatus.getId());

    }

    /**
     * Schedule a new campaign, booking all the advertising time slots selected
     *
     * @param campaignId internally system generated unique ID assigned to every
     * new campaign
     * @throws MyCustomException
     */
    private boolean bookNewCampaignSchedule(AdProgram campaignProgram, CustomHibernate customHibernate) {

        //AdProgram campaignProgram = customHibernate.fetchEntity(AdProgram.class, "campaignId", campaignId);
        try {

            if (campaignProgram != null) {

                long programEntityId = campaignProgram.getId();
                int campaignId = campaignProgram.getCampaignId();
                LocalDate startDate = campaignProgram.getStartAdDate();
                int periodOfAdCampaignInDays = campaignProgram.getCampaignDaysPeriod();
                AdClient client = campaignProgram.getClient();
                Set<AdProgramSlot> adProgSlot = customHibernate.fetchEntities(AdProgramSlot.FETCH_PROG_SLOT, "campaignId", campaignId);

                Set<String> fetchedScreenCodes = GeneralUtils.convertCommaDelStringToSet(campaignProgram.getCampaignScreenCodes());

                Map<String, Object> screenProperties = new HashMap<>();
                screenProperties.put("screenId", new HashSet<>(fetchedScreenCodes));

                Set<AdScreen> adScreens = customHibernate.fetchBulk(AdScreen.class, screenProperties);

                if (adScreens == null || adScreens.isEmpty()) {

                    String errorDetails = "No target Screens were found for this campaign, fetched empty adscreens from the database";
                    String errorDescription = "Oops, no target screens found! Can't schedule your campaign, please try again.";
                    MyCustomException error = GeneralUtils.getSingleError(ErrorCode.BAD_REQUEST_ERR, errorDescription, errorDetails);
                    throw error;
                }

                Set<TimeSlot> timeSlots = CampaignUtilities.getProgSlotsHelper(adProgSlot);

                // Interprete the schedule for this adprogram - each schedule represents a screen/terminal
                for (TimeSlot aSlot : timeSlots) {

                    Set<Integer> allowedAdDays = aSlot.getDays(); //e.g. 1 for Monday
                    int preferredHour = aSlot.getPreferredHour();
                    int adFrequency = aSlot.getFrequency(); //number of times we can play this ad within this time-slot
                    adFrequency = CampaignUtilities.resetFreq(adFrequency);

                    String timeSlotCode = aSlot.getName();

                    logger.debug("Preferred Hour   : " + preferredHour);
                    logger.debug("Advert frequency : " + adFrequency);
                    logger.debug("Preferred Slot   : " + timeSlotCode);
                    logger.debug("Allowed Ad Days  : " + allowedAdDays);

                    AdTimeSlot adTimeSlot = customHibernate.fetchEntity(AdTimeSlot.class, "timeSlotCode", timeSlotCode);

                    if (adTimeSlot == null) {
                        logger.error("Unknown Time slot selected by client: " + timeSlotCode + ", continuing to next schedule");
                        continue;
                    }

                    //persist the AdProgramSlot entity as well
                    //persistAdProgramSlotHelper(aSlot, campaignProgram, adTimeSlot);
                    int slotStartTime = adTimeSlot.getStartTime().getMillisOfDay();
                    int slotEndTime = adTimeSlot.getEndTime().getMillisOfDay();
                    int slotPrice = adTimeSlot.getSlotAdPrice().getAmount();
                    float slotDiscount = adTimeSlot.getSlotDiscount();

                    long approvalDelay = 0;

                    if (client.isIsToBeCensored()) {
                        //uncomment after tests -introduces delay
                        //approvalDelay = adTimeSlot.getApprovalDelay();
                    }

                    //For Each ad compaign day, add entry to AdSchedule table
                    for (int day = 0; periodOfAdCampaignInDays > day; day++) {

                        //add all delays to get a 'near' perfect schedule time
                        long timeToNearest = DateUtils.getTimeNowToNearestMinute().getMillisOfDay();
                        long timeNow = DateUtils.getTimeNow().getMillisOfDay();
                        int millisOfDayNow = (int) (timeToNearest + approvalDelay + NamedConstants.SYSTEM_SCHEDULE_DELAY_MILLIS);

                        logger.debug("Time Now            : " + DateUtils.convertLocalTimeToString(DateUtils.convertMillisToLocalTime(timeNow, DateTimeZone.UTC), NamedConstants.HOUR_MINUTE_SEC_FORMAT) + " - in millis: " + timeNow);
                        logger.debug("Time Now to nearest : " + DateUtils.convertLocalTimeToString(DateUtils.convertMillisToLocalTime(timeToNearest, DateTimeZone.UTC), NamedConstants.HOUR_MINUTE_SEC_FORMAT) + " - in millis: " + timeToNearest);
                        logger.debug("Approval Delay      : " + DateUtils.convertLocalTimeToString(DateUtils.convertMillisToLocalTime(approvalDelay, DateTimeZone.UTC), NamedConstants.HOUR_MINUTE_SEC_FORMAT) + " - in millis: " + approvalDelay);
                        logger.debug("System   Delay      : " + DateUtils.convertLocalTimeToString(DateUtils.convertMillisToLocalTime(NamedConstants.SYSTEM_SCHEDULE_DELAY_MILLIS, DateTimeZone.UTC), NamedConstants.HOUR_MINUTE_SEC_FORMAT) + " - in millis: " + NamedConstants.SYSTEM_SCHEDULE_DELAY_MILLIS);
                        logger.debug("Time now with approval delay + schedule delay : " + DateUtils.convertLocalTimeToString(DateUtils.convertMillisToLocalTime(millisOfDayNow, DateTimeZone.UTC), NamedConstants.HOUR_MINUTE_SEC_FORMAT) + " - in millis: " + millisOfDayNow);

                        LocalDate dateOfAd = DateUtils.addDaysToLocalDate(startDate, day);

                        int preferredTimeInMillis;

                        if (adTimeSlot.isIsInstant()) {

                            preferredTimeInMillis = millisOfDayNow;
                            logger.debug("Instant Time Slots dont need allowed days! proceed to schedule ad");

                        } else if (CampaignUtilities.canAdvertiseOnThisDay(dateOfAd, allowedAdDays)) {

                            logger.info("Scheduling Advert on Date: " + dateOfAd);
                            logger.info("Scheduling Advert on Day : " + DayOfWeek.of(dateOfAd.getDayOfWeek()));

                            //deal with a client who has chosen a time slot in the past today e.g. time is midday and time slot chosen ends at 11.59am
                            boolean slotInThePast = CampaignUtilities.isSlotInThePast(millisOfDayNow, slotStartTime, slotEndTime, dateOfAd);

                            //Chosen slot time (end-time) has already elapsed
                            if (slotInThePast && !adTimeSlot.isIsInstant()) {

                                logger.error("Can't schedule this slot, slot time for the day has ended for non-instant slot");
                                continue;

                            } //Preferred Hour not set
                            else if (preferredHour == -1) {

                                if (millisOfDayNow > slotStartTime) {

                                    preferredTimeInMillis = millisOfDayNow; //schedule now

                                } else {
                                    //schedule at start of time slot
                                    preferredTimeInMillis = (int) (slotStartTime + approvalDelay + NamedConstants.SYSTEM_SCHEDULE_DELAY_MILLIS);

                                }

                            } //prefereed Hour set
                            else {

                                //check to make sure chosen hour is not in the past of this time slot
                                LocalTime preferredLocalTime = new LocalTime(preferredHour, 0); //until we let users enter the minute part, for now it will be zero (0)
                                preferredTimeInMillis = (int) (preferredLocalTime.getMillisOfDay() + approvalDelay + NamedConstants.SYSTEM_SCHEDULE_DELAY_MILLIS);

                                if (preferredTimeInMillis < millisOfDayNow) {

                                    //preferred time is within slot but in the past, and time now is within slot time, 
                                    //change it to tine now
                                    if (millisOfDayNow > slotStartTime) {
                                        preferredTimeInMillis = millisOfDayNow;

                                    } //preferred time is within slot and in the past, but time now is before slot time, change it to slot start time
                                    else {
                                        preferredTimeInMillis = (int) (slotStartTime + approvalDelay + NamedConstants.SYSTEM_SCHEDULE_DELAY_MILLIS);
                                    }
                                }
                            }

                        } else {
                            logger.debug("Skipping this day of the week: " + DayOfWeek.of(dateOfAd.getDayOfWeek()) + ", because client has chosen not to advertise on this day");
                            continue;
                        }

                        boolean assignedSlot = Boolean.FALSE; //If schedule has gotten a slot

                        //Add schedule to all targeted screens
                        for (AdScreen adScreen : adScreens) {

                            Set<Object> screenIds = new HashSet<>();
                            screenIds.add(adScreen.getScreenId());
                            Set<Object> displayDateSet = new HashSet<>();
                            displayDateSet.add(dateOfAd);

                            //Get schedule for this day for given screen_id and modify it else insert it new
                            Map<String, Set<Object>> scheduleProperties = new HashMap<>();
                            scheduleProperties.put("adScreen.screenId", screenIds);
                            scheduleProperties.put("displayDate", displayDateSet);

                            //"764::4563;905::2355;" ----> "prog_entity_id":"time"
                            AdSchedule adSchedule = customHibernate.fetchEntity(AdSchedule.class, scheduleProperties);

                            if (adSchedule == null) {

                                logger.debug("Insert new AdSchedule..");

                                adSchedule = new AdSchedule();
                                adSchedule.setScheduleId(HibernateUtils.generateIntegerID(customHibernate, AdSchedule.class, "scheduleId"));
                                adSchedule.setScheduleDetail(programEntityId + "::" + preferredTimeInMillis + ";");
                                adSchedule.setAdScreen(adScreen);
                                adSchedule.setDisplayDate(dateOfAd);
                                adSchedule.setFetchStatus(FetchStatus.TO_FETCH);

                                customHibernate.saveEntity(adSchedule);
                                assignedSlot = Boolean.TRUE;

                            } else {

                                logger.debug("Update old AdSchedule");

                                String schedString = adSchedule.getScheduleDetail();
                                //add program ids and their schedule times to an iterable
                                Map<Integer, Long> mapOfScheduleAndProgIds = GeneralUtils.convertToMapStringOfSchedulesAndProgIds(schedString);

                                //get all schedules for this ad ->> schedule time (now) to play on all screens, move/edit schedule programs to accomodate this program for instant ads
                                Set<Integer> scheduleTimes = mapOfScheduleAndProgIds.keySet();

                                if (scheduleTimes.contains(preferredTimeInMillis)) {

                                    logger.info("Scheduling an Instant Advert..");

                                    long progIdWithConflictSched = mapOfScheduleAndProgIds.get(preferredTimeInMillis);

                                    for (int slot : ADHOUR_SLOT_ALLOCATION_ORDER) { //slot is in minutes

                                        int slotInMillis = (int) DateUtils.convertToMillis(TimeUnit.MINUTES, slot);

                                        if (adTimeSlot.isIsInstant()) {
                                            //re-assign a different schedule time to program that has this particular ad slot time
                                            int relocatedTimeInMillis = preferredTimeInMillis;//start here to look for next best time to relocate to
                                            relocatedTimeInMillis += slotInMillis;
                                            if (!scheduleTimes.contains(relocatedTimeInMillis)) {

                                                mapOfScheduleAndProgIds.put(relocatedTimeInMillis, progIdWithConflictSched);
                                                logger.info("Re-assigned program Entity Id: " + progIdWithConflictSched + ", a new slot: " + DateUtils.convertLocalTimeToString(DateUtils.convertMillisToLocalTime(relocatedTimeInMillis, DateTimeZone.UTC), NamedConstants.HOUR_MINUTE_SEC_FORMAT));
                                                break;
                                            }

                                        } else {
                                            //get next available slot greater or lesser than this pivotal preferred hour, till all slots are covered in this Time Slot and move to next cheaper slot or more expensive slot?? who will pay or lose, cuz of this move
                                            //Fill any of the slots before this preferred hour (hour but closest), else fill any of the slots after this pref. hour but closest
                                            preferredTimeInMillis += slotInMillis;
                                            if (!scheduleTimes.contains(preferredTimeInMillis)) {
                                                break;
                                            }
                                        }
                                    }
                                }

                                mapOfScheduleAndProgIds.put(preferredTimeInMillis, programEntityId);
                                schedString = GeneralUtils.convertToStringMapTheOfSchedulesAndProgIds(mapOfScheduleAndProgIds);

                                adSchedule.setScheduleDetail(schedString);
                                adSchedule.setFetchStatus(FetchStatus.TO_FETCH);
                                adSchedule.setAdScreen(adScreen);

                                customHibernate.updateEntity(adSchedule);

                            }

                            logger.debug("Adding schedule for schedId    : " + adScreen.getId());
                            logger.debug("Date of schedule               : " + dateOfAd);
                            logger.debug("Preferred time in milliseconds : " + preferredTimeInMillis);
                            logger.debug("Preferred time in LocalTime    : " + DateUtils.convertLocalTimeToString(DateUtils.convertMillisToLocalTime(preferredTimeInMillis, DateTimeZone.UTC), NamedConstants.HOUR_MINUTE_SEC_FORMAT));
                            logger.debug("Slot start-time in milliseconds: " + DateUtils.convertLocalTimeToString(DateUtils.convertMillisToLocalTime(slotStartTime, DateTimeZone.UTC), NamedConstants.HOUR_MINUTE_SEC_FORMAT) + " - in millis: " + slotStartTime);
                            logger.debug("Slot end-time in milliseconds  : " + DateUtils.convertLocalTimeToString(DateUtils.convertMillisToLocalTime(slotEndTime, DateTimeZone.UTC), NamedConstants.HOUR_MINUTE_SEC_FORMAT) + " - in millis: " + slotEndTime);
                            logger.debug("Time now (with delays)         : " + DateUtils.convertLocalTimeToString(DateUtils.convertMillisToLocalTime(millisOfDayNow, DateTimeZone.UTC), NamedConstants.HOUR_MINUTE_SEC_FORMAT) + " - in millis: " + millisOfDayNow);

                        }//end for-loop -> screens

                    } // end for-loop -> iterating through campaign days               
                }

            } else {

                String errorDetails = "Failed to fetch advert programs from the database, program == null";
                String errorDescription = "Oops, error occurred! Failed to schedule your campaign, please try again.";
                MyCustomException error = GeneralUtils.getSingleError(ErrorCode.DATABASE_ERR, errorDescription, errorDetails);
                throw error;
            }

            return Boolean.TRUE;

        } catch (MyCustomException exc) {

        }
        return Boolean.FALSE;
    }

    @Override
    public void interrupt() throws UnableToInterruptJobException {
        logger.warn("Failed to interrupt a Job before deleting it..");
    }
}
