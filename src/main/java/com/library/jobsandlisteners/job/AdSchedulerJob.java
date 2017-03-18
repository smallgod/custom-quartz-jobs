/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.library.jobsandlisteners.job;

import com.library.datamodel.Constants.ProcessingUnitState;
import com.library.datamodel.Json.AdFetchRequest;
import com.library.httpconnmanager.HttpClientPool;
import com.library.configs.JobsConfig;
import com.library.datamodel.Constants.APIMethodName;
import com.library.datamodel.Constants.EntityName;
import com.library.datamodel.Constants.FetchStatus;
import com.library.datamodel.Constants.GenerateId;
import com.library.datamodel.Constants.GenerateIdType;
import com.library.datamodel.Constants.NamedConstants;
import com.library.datamodel.Constants.ProgDisplayLayout;
import com.library.datamodel.Constants.ResourceType;
import com.library.datamodel.Json.GenerateIdRequest;
import com.library.datamodel.Json.GeneratedIdResponse;
import com.library.datamodel.Json.AdSetupRequest;
import com.library.datamodel.Json.AdSetupRequest.ProgramDetail;
import com.library.datamodel.Json.AdSetupRequest.ProgramDetail.Program.DisplayTime;
import com.library.datamodel.Json.AdSetupRequest.ProgramDetail.Program.Resources;
import com.library.datamodel.Json.AdSetupRequest.ProgramDetail.Program.Text;
import com.library.datamodel.model.v1_0.AdProgram;
import com.library.datamodel.model.v1_0.AdResource;
import com.library.datamodel.model.v1_0.AdSchedule;
import com.library.datamodel.model.v1_0.AdScreen;
import com.library.datamodel.model.v1_0.AdTerminal;
import com.library.datamodel.model.v1_0.AdText;
import com.library.dbadapter.DatabaseAdapter;
import com.library.fileuploadclient.MultipartFileUploader;
import com.library.sgsharedinterface.ExecutableJob;
import com.library.utilities.GeneralUtils;
import org.quartz.InterruptableJob;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.UnableToInterruptJobException;
import com.library.sgsharedinterface.RemoteRequest;
import com.library.utilities.DateUtils;
import com.library.utilities.FileUtilities;
import com.library.utilities.LoggerUtil;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDate;
import org.joda.time.LocalTime;

/**
 * The class doing the work
 *
 * @author smallgod
 */
public class AdSchedulerJob implements Job, InterruptableJob, ExecutableJob {

    private static final LoggerUtil logger = new LoggerUtil(AdSchedulerJob.class);

    //this method is for testing purpose only, delete after
    @Override
    public void execute(JobExecutionContext jec) throws JobExecutionException {

        logger.debug("Current thread here is  : " + Thread.currentThread().getName());
        logger.debug("Current thread in state : " + Thread.currentThread().getState().name());

        synchronized (NamedConstants.FETCH_SCHEDULE_MUTEX) {

            try {

                //assume we are generating 2 file ids only
                //we should also send ids we think exist to the dsm bridge service to confirm for us, so that if they don't exist, new ids are generated
                JobDetail jobDetail = jec.getJobDetail();
                String jobName = jobDetail.getKey().getName();

                JobDataMap jobsDataMap = jec.getMergedJobDataMap();

                JobsConfig jobsData = (JobsConfig) jobsDataMap.get(jobName);

                RemoteRequest dbManagerUnit = jobsData.getRemoteUnitConfig().getAdDbManagerRemoteUnit();
                RemoteRequest centralUnit = jobsData.getRemoteUnitConfig().getAdCentralRemoteUnit();
                RemoteRequest dsmRemoteUnit = jobsData.getRemoteUnitConfig().getDSMBridgeRemoteUnit();
                HttpClientPool clientPool = (HttpClientPool) jobsDataMap.get(NamedConstants.CLIENT_POOL);
                DatabaseAdapter databaseAdapter = (DatabaseAdapter) jobsDataMap.get(NamedConstants.DB_ADAPTER);

                Set<String> columnsToFetch = new HashSet<>();
                columnsToFetch.add("ALL");

                Set<AdSchedule> fetchedSchedules = null;

                try {
                    if (NamedConstants.FETCH_SCHEDULE_LOCK.tryLock(30, TimeUnit.SECONDS)) {

                        logger.debug("Thread, " + Thread.currentThread().getName() + ", Inside lock, about to fetch!");

                        //Fetch pending ad programs from DB
                        //Fetch Records from AdScheduler Table for date (today) and with to_update field == False
                        //If found, get screen_id / terminal_id for this pending entry
                        //If found, do the needful and send request to DSM
                        Map<String, Object> pendingAdsProps = new HashMap<>();

                        Set<FetchStatus> fetch = new HashSet<>();
                        fetch.add(FetchStatus.TO_FETCH);

                        Set<LocalDate> displayDateSet = new HashSet<>();
                        displayDateSet.add(DateUtils.getDateNow());

                        pendingAdsProps.put("fetchStatus", fetch);  //whether or not we need to fetch this schedule and update DSM and the row as well
                        pendingAdsProps.put("displayDate", displayDateSet);

                        fetchedSchedules = databaseAdapter.fetchBulk(EntityName.AD_SCHEDULE, pendingAdsProps, columnsToFetch);

                        if (null == fetchedSchedules || fetchedSchedules.isEmpty()) {

                            logger.info("No Pending AdSchedules where fetched from the database");
                            return;

                        } else {

                            logger.debug("Fetched Records from DB, size: " + fetchedSchedules.size());

                            Set<AdSchedule> schedulesToUpdate = new HashSet<>();

                            for (AdSchedule adSchedule : fetchedSchedules) {
                                //Update the schedule in DB so that we dont fetch it again
                                adSchedule.setFetchStatus(FetchStatus.FETCHED);
                                schedulesToUpdate.add(adSchedule);

                                //databaseAdapter.saveOrUpdateEntity(adSchedule, Boolean.FALSE);
                            }

                            databaseAdapter.SaveOrUpdateBulk(EntityName.AD_SCHEDULE, schedulesToUpdate, Boolean.FALSE);
                        }

                    } else {
                        logger.debug("Lock is held by some other thread, return now!");
                        return;
                    }
                } catch (InterruptedException e) {

                    logger.error("Interrrupted exception: " + e.getMessage());
                    e.printStackTrace();

                } finally {
                    //release lock
                    logger.debug("Releasing lock!");
                    NamedConstants.FETCH_SCHEDULE_LOCK.unlock();
                }

                logger.debug("Sent AdSchedule request, size of schedules got == " + fetchedSchedules.size());

                AdSetupRequest adSetupRequest = new AdSetupRequest();

                //Each Schedule represents a single(1) screen for that given date/day
                for (AdSchedule adSchedule : fetchedSchedules) {

                    //AdScreen adScreen = null;
                    AdScreen adScreen = adSchedule.getAdScreen(); //"764::4563::T;905::2355::F;" ----> "prog_entity_id"::"time_in_millis::whether_prog_is_sent_to_dsm"
                    LocalDate displayDate = adSchedule.getDisplayDate();
                    String scheduleString = adSchedule.getScheduleDetail();
                    long scheduleId = adSchedule.getScheduleId();

                    //remove older times from schedule string
                    int millisOfDayNow = DateUtils.getTimeNowToNearestMinute().getMillisOfDay();
                    Map<Integer, Long> mapOfSchedulesAndProgIds = GeneralUtils.convertToMapStringOfSchedulesAndProgIds(scheduleString);
                    String newerScheduleString = "";

                    for (Map.Entry<Integer, Long> entry : mapOfSchedulesAndProgIds.entrySet()) {

                        int scheduleTime = entry.getKey();
                        long progEntityId = entry.getValue();

                        logger.debug("ProgEntityId : " + progEntityId);
                        logger.debug("Schedule time: " + DateUtils.convertLocalTimeToString(DateUtils.convertMillisToLocalTime(scheduleTime, DateTimeZone.UTC), NamedConstants.HOUR_MINUTE_SEC_FORMAT) + " - in millis: " + scheduleTime);
                        logger.debug("millisOfDay  : " + DateUtils.convertLocalTimeToString(DateUtils.convertMillisToLocalTime(millisOfDayNow, DateTimeZone.UTC), NamedConstants.HOUR_MINUTE_SEC_FORMAT) + " - in millis: " + millisOfDayNow);

                        if (scheduleTime >= millisOfDayNow) {

                            newerScheduleString += (progEntityId + "::" + scheduleTime + ";");
                        }

                    }

                    if (newerScheduleString.trim().isEmpty()) {
                        logger.warn("Fetched Schedules for display but found display times already passed now!");
                        continue;
                    }

                    logger.debug("Display Date                    : " + displayDate);
                    logger.debug("Schedule str with all times     : " + scheduleString);
                    logger.debug("Schedule ID                     : " + scheduleId);
                    logger.debug("Schedule str, old times removed : " + newerScheduleString);

                    String[] progTimeArray = newerScheduleString.trim().split("\\s*;\\s*"); // ["764::4563::T", "905::2355::F"]

                    logger.debug("ProgTimeArray: " + Arrays.toString(progTimeArray));

                    List<Integer> displayTimeList = new ArrayList<>();

                    Map<Integer, List<Integer>> mapOfProgEntityIdsAndDisplayTime = new HashMap<>();//"1->3445555" prog_entity_id -> displaytime(millis)

                    for (String element : progTimeArray) {

                        int progEntityId = Integer.parseInt(element.split("\\s*::\\s*")[0]);
                        int displayTime = Integer.parseInt(element.split("\\s*::\\s*")[1]);

                        logger.debug("ProgEntityId: " + progEntityId);
                        logger.debug("DisplayTime : " + displayTime);

                        if (mapOfProgEntityIdsAndDisplayTime.containsKey(progEntityId)) {

                            displayTimeList = mapOfProgEntityIdsAndDisplayTime.get(progEntityId);
                        }
                        displayTimeList.add(displayTime);
                        mapOfProgEntityIdsAndDisplayTime.put(progEntityId, displayTimeList);
                    }

                    Map<String, Object> programProps = new HashMap<>();
                    programProps.put("id", mapOfProgEntityIdsAndDisplayTime.keySet());

                    //Fetch Program details to check which ones we need to generate ids for
                    Set<AdProgram> fetchedPrograms = databaseAdapter.fetchBulk(EntityName.AD_PROGRAM, programProps, columnsToFetch);

                    if (!(null == fetchedPrograms || fetchedPrograms.isEmpty())) {

                        int numOfFileIdsNeeded = 0;

                        int numOfProgIdsNeeded = fetchedPrograms.size(); //need to check this, some fetched programs might have program Ids and no need to generate
                        List<String> existingProgIds = new ArrayList<>();

                        //need to have the Ids generated before performing some operations below, cuze they depend on those Ids
                        for (AdProgram adProgram : fetchedPrograms) {
                            numOfFileIdsNeeded += adProgram.getNumOfFileResources();
                            Boolean isDSMUpdated = adProgram.isIsDSMUpdated();
                            if (isDSMUpdated) {
                                existingProgIds.add("" + adProgram.getAdvertProgramId());

                                numOfProgIdsNeeded -= 1; //decrement needed program Ids
                            }
                        }

                        //logger.debug(">>>>>>>>>>>>>>SSSSSSSSSSSSSSSSSsize of resources is: " + adResourceList.get(0).getAdResourceList().size());
                        //To-Do         ->> For Existing resources check making sure we don't double upload resources, we will work on this later..
                        //To-DO         ->> Call to Generate Ids here <<-
                        //To-DO         ->> Assign the generated Ids to each of the programs and the Resources <<-
                        //To-DO         ->> Update Each of the Programs and Resources in the DB with these generated Ids <<- I think do this after sending successful to DSM
                        //generate file ids
                        /////
                        /////
                        /////
                        GenerateIdRequest generateIdRequest = new GenerateIdRequest();
                        List<GenerateIdRequest.Params> params = new ArrayList<>();

                        GenerateIdRequest.Params fileIdParams = generateIdRequest.new Params();
                        fileIdParams.setId(GenerateId.FILE_ID.getValue());
                        fileIdParams.setIdTypeToGenerate(GenerateIdType.LONG.getValue());
                        fileIdParams.setNumOfIds(numOfFileIdsNeeded);
                        fileIdParams.setExistingIdList(new ArrayList<String>()); //To-Do work on this
                        params.add(fileIdParams);

                        GenerateIdRequest.Params progIdParams = generateIdRequest.new Params();
                        progIdParams.setId(GenerateId.PROGRAM_ID.getValue());
                        progIdParams.setIdTypeToGenerate(GenerateIdType.INTEGER.getValue());
                        progIdParams.setNumOfIds(numOfProgIdsNeeded);
                        progIdParams.setExistingIdList(existingProgIds);
                        params.add(progIdParams);

                        generateIdRequest.setMethodName(APIMethodName.GENERATE_ID.getValue());
                        generateIdRequest.setParams(params);

                        String generateIdJsonRequest = GeneralUtils.convertToJson(generateIdRequest, GenerateIdRequest.class);
                        String generateIdJsonResponse = clientPool.sendRemoteRequest(generateIdJsonRequest, dsmRemoteUnit);

                        GeneratedIdResponse genIdResponse = GeneralUtils.convertFromJson(generateIdJsonResponse, GeneratedIdResponse.class);
                        List<GeneratedIdResponse.Response> responseList = genIdResponse.getResponse();

                        List<GeneratedIdResponse.Response.ExistingIDCheck> existingProgIdCheck;
                        List<GeneratedIdResponse.Response.ExistingIDCheck> existingFileIdCheck;
                        List<String> progIdsGenerated = new ArrayList<>();
                        List<String> fileIdsGenerated = new ArrayList<>();
                        int noOfProgIds;
                        int noOfFileIds;

                        for (GeneratedIdResponse.Response response : responseList) {

                            GenerateId generatedId = GenerateId.convertToEnum(response.getId());

                            switch (generatedId) {

                                case PROGRAM_ID:
                                    progIdsGenerated = response.getGeneratedIdList();
                                    existingProgIdCheck = response.getExistingIdCheck();
                                    noOfProgIds = response.getNumOfIds();
                                    break;

                                case FILE_ID:
                                    fileIdsGenerated = response.getGeneratedIdList();
                                    existingFileIdCheck = response.getExistingIdCheck();
                                    noOfFileIds = response.getNumOfIds();
                                    break;

                                default:
                                    progIdsGenerated = response.getGeneratedIdList();
                                    existingProgIdCheck = response.getExistingIdCheck();
                                    noOfProgIds = response.getNumOfIds();
                                    break;
                            }

                        }

                        List<AdSetupRequest.ProgramDetail> programDetailList = new ArrayList<>();
                        List<AdSetupRequest.TerminalDetail> terminalDetailList = new ArrayList<>();

                        AdSetupRequest.ProgramDetail progDetail = adSetupRequest.new ProgramDetail();

                        List<ProgramDetail.Program> programs = createProgramList(databaseAdapter, progDetail, fetchedPrograms, mapOfProgEntityIdsAndDisplayTime, progIdsGenerated, fileIdsGenerated);

                        progDetail.setDisplayDate(DateUtils.convertLocalDateToString(DateUtils.getDateNow(), NamedConstants.DATE_DASH_FORMAT)); //we can have several ProgramDetail with display times, but only doing today/now
                        progDetail.setProgramIds(programs);

                        //add only a single day's ProgramDetail, could add more days but 1 day for now
                        programDetailList.add(progDetail);

                        AdSetupRequest.TerminalDetail terminalDetail = adSetupRequest.new TerminalDetail();
                        terminalDetail.setDisplayDate(DateUtils.convertLocalDateToString(DateUtils.getDateNow(), NamedConstants.DATE_DASH_FORMAT));//we can have TerminalDetail with several display times, but only doing today/now

                        Map<Long, List<ProgramDetail.Program>> programScreenMap = new HashMap<>();
                        programScreenMap.put(adScreen.getId(), programs);

                        List<AdSetupRequest.TerminalDetail.Terminal> terminalList = createTerminalList(terminalDetail, Arrays.asList(adScreen), programScreenMap); //programIdList is from program Ids that have been generated
                        terminalDetail.setTerminals(terminalList);

                        //add only a single day's TerminalDetail, could add more days but 1 day for now
                        terminalDetailList.add(terminalDetail);

                        //In the interim I think we should fetch all resources and upload to DSM first
                        //We will need to rectify this code as it is tightly coupling the adDisplay unit to AdCentral
                        //since it is fetching resources from a machine path as though they were on the same machine e.g.
                        ///Users/smallgod/Desktop/DSM8_stuff/uploadstuff/97354630322880.mp4"
                        //preview URL for now will be used as the file uploads URL
                        //we are uploading resources here, right before calling Adrequest
                        Set<AdResource> allFileResources = new HashSet<>();

                        for (ProgramDetail.Program progee : programs) {

                            List<AdSetupRequest.ProgramDetail.Program.Resources> fileResources = progee.getResources();

                            for (AdSetupRequest.ProgramDetail.Program.Resources resource : fileResources) {

                                AdResource adRes = new AdResource();
                                adRes.setResourceName(resource.getResourceDetail());
                                adRes.setResourceId(resource.getResourceId());
                                adRes.setUploadId(resource.getUploadId());
                                adRes.setId(resource.getEntityId());
                                adRes.setResourceType(ResourceType.convertToEnum(resource.getResourceType()));
                                adRes.setIsUploadedToDSM(resource.isIsUploadedToDSM());

                                allFileResources.add(adRes);
                            }

                        }

                        Set<File> oldFiles = new HashSet<>();
                        Set<File> filesToUpload = new HashSet<>();

//                        Map<String, Object> resourcesToUploadProps = new HashMap<>();
//                        Set<Boolean> isResourceUploaded = new HashSet<>();
//                        isResourceUploaded.add(Boolean.FALSE);
//
//                        resourcesToUploadProps.put("isUploadedToDSM", isResourceUploaded);
//
//                        Set<AdResource> fetchedResources = databaseAdapter.fetchBulk(EntityName.AD_RESOURCE, resourcesToUploadProps, columnsToFetch);
                        String fileUploadDir = "/etc/ug/adcentral/temp/uploads";
                        //these hardcoded things need to change

                        List<AdResource> resourecesToUpdateInDB = new ArrayList<>();

                        logger.debug("The number of resources for this progee is: " + allFileResources.size());
                        logger.debug("And the progee files are:");

                        //get all resources for this program
                        int iteration = 1;

                        Set<Long> ids = new HashSet<>();
                        for (AdResource resource : allFileResources) {

                            logger.debug("Iteration no. " + iteration + ", for resource name: " + resource.getResourceName());
                            logger.debug("Iterating no. " + iteration + ", for resource id  : " + resource.getUploadId());

                            boolean isUploadedToDSM = resource.isIsUploadedToDSM();

                            if (!isUploadedToDSM) {

                                long id = resource.getId();
                                String uploadId = resource.getUploadId();
                                String uploadName = resource.getResourceName();
                                long fileId = resource.getResourceId();

                                logger.debug("Resource, with name: " + resource.getResourceName() + ", file id: " + resource.getResourceId() + ", is not yet uploaded, uploading now!");
                                //update this object
                                String fileName = fileUploadDir + File.separator + uploadId + "_" + uploadName;
                                String newFileName = fileUploadDir + File.separator + fileId;

                                if (!FileUtilities.existFile(newFileName)) {

                                    boolean isCopied = FileUtilities.copyFile(fileName, newFileName);
                                    logger.debug("Renaming of file status: " + isCopied);

                                    if (isCopied) {

                                        filesToUpload.add(new File(newFileName));
                                        oldFiles.add(new File(fileName));

                                    }
                                }

                                Map<String, Object> resourceProps = new HashMap<>();
                                resourceProps.put("id", new HashSet<>(Arrays.asList(id)));

                                AdResource adResourceFromDB = (AdResource) databaseAdapter.fetchEntity(EntityName.AD_RESOURCE, resourceProps, columnsToFetch);

                                adResourceFromDB.setIsUploadedToDSM(Boolean.TRUE);
                                adResourceFromDB.setResourceId(fileId);
                                resourecesToUpdateInDB.add(adResourceFromDB);
                                databaseAdapter.saveOrUpdateEntity(adResourceFromDB, Boolean.FALSE);

                                ids.add(id);

                                iteration++;
                            }

                            boolean isFileUploaded = MultipartFileUploader.uploadFiles(filesToUpload, dsmRemoteUnit.getPreviewUrl());
                            logger.debug("Are files uploaded to DSM server: " + isFileUploaded);

                            if (isFileUploaded) {

                                logger.debug("Going to update resources in DB, no. of resources : " + resourecesToUpdateInDB.size());

                                //boolean isUpdated = databaseAdapter.SaveOrUpdateBulk(EntityName.AD_RESOURCE, resourecesToUpdateInDB, Boolean.FALSE);
                                //logger.debug("resources updated: " + isUpdated);
                                //delete the old files from disk
                                for (File file : oldFiles) {
                                    FileUtilities.deleteFile(file);
                                }

                                adSetupRequest.setMethodName(APIMethodName.DAILY_SETUP_STEP2.getValue());
                                adSetupRequest.setProgramDetail(programDetailList);
                                adSetupRequest.setTerminalDetail(terminalDetailList);

                                String jsonReq = GeneralUtils.convertToJson(adSetupRequest, AdSetupRequest.class);

                                String response = clientPool.sendRemoteRequest(jsonReq, dsmRemoteUnit);

                                logger.debug("Mega wrapper Response: " + response);

                            } else {

                                //delete the old files from disk
                                for (File file : filesToUpload) {
                                    FileUtilities.deleteFile(file);
                                }
                                logger.warn("Failed to upload files to DSM, deleting the new files that were created on disk AND NOT sending request to DSM");
                            }

                        }//end for-loop through resources 

                    } else {
                        logger.warn("There are empty or no AdPrograms returned for this schedule: " + newerScheduleString);
                    }

                }

            } catch (Exception ex) {
                logger.error("An Error occurred in AdScheduleJob: " + ex.getMessage());
                ex.printStackTrace();
            }

        }

    }

    /**
     *
     * @param progDetail
     * @param adProgram
     * @param adResources
     * @return
     */
    List<ProgramDetail.Program> createProgramList(DatabaseAdapter databaseAdapter, AdSetupRequest.ProgramDetail progDetail, Set<AdProgram> adProgramList, Map<Integer, List<Integer>> mapOfProgIdsAndDisplayTime, List<String> progIdsGenerated, List<String> fileIdsGenerated) {

        List<ProgramDetail.Program> programList = new ArrayList<>();
        ProgramDetail.Program prog;

        int generatedProgIdCount = 0;

        for (AdProgram adProgram : adProgramList) {

            prog = progDetail.new Program();

            long adLength = adProgram.getAdLength();
            long id = adProgram.getId();
            ProgDisplayLayout displayLayout = adProgram.getDisplayLayout();

            Set<AdText> textResources = databaseAdapter.fetchResources(EntityName.AD_TEXT, AdText.FETCH_TEXT, "id", id);
            Set<AdResource> adResources = databaseAdapter.fetchResources(EntityName.AD_RESOURCE, AdResource.FETCH_RESOURCE, "id", id);

            //for small values we can just cast a Long to an int, for large values be ware, we loose precision and accuracy so we don't do it
            long progEntityId = adProgram.getId();
            List<Integer> displayTimesFromMap = mapOfProgIdsAndDisplayTime.get((int) progEntityId);
            List<DisplayTime> displayTimes = new ArrayList<>();

            DisplayTime displayTime;
            for (int time : displayTimesFromMap) {

                LocalTime startLocalTime = DateUtils.convertMillisToLocalTime(time, NamedConstants.UTC_TIME_ZONE);
                LocalTime endLocalTime = startLocalTime.plusMillis((int) adLength); //adlength determines display End-time

                displayTime = prog.new DisplayTime();
                displayTime.setStarttime(DateUtils.convertLocalTimeToString(startLocalTime, NamedConstants.HOUR_MINUTE_SEC_FORMAT)); //is this format "hh:mm:ss" a must? can't we do "hh:mm"
                displayTime.setStoptime(DateUtils.convertLocalTimeToString(endLocalTime, NamedConstants.HOUR_MINUTE_SEC_FORMAT));

                displayTimes.add(displayTime);
            }

            //get media resources if available
            List<Resources> resourcesList;

            //get text resources if available
            List<Text> textList;

            switch (displayLayout) {

                case FULL_SCREEN:
                    textList = new ArrayList<>();
                    resourcesList = getResourcesList(prog, adResources, fileIdsGenerated);
                    break;

                case AV_ONLY:
                    textList = new ArrayList<>();
                    resourcesList = new ArrayList<>();
                    break;

                case FULLSCREEN_TEXT:
                    textList = getTextList(prog, textResources);
                    resourcesList = getResourcesList(prog, adResources, fileIdsGenerated);
                    break;

                case TEXT_ONLY:
                    textList = getTextList(prog, textResources);
                    resourcesList = new ArrayList<>();
                    break;

                case THREE_SPLIT:
                    textList = getTextList(prog, textResources);
                    resourcesList = getResourcesList(prog, adResources, fileIdsGenerated);
                    break;

                case TWO_SPLIT:
                    textList = getTextList(prog, textResources);
                    resourcesList = getResourcesList(prog, adResources, fileIdsGenerated);
                    break;

                default:
                    textList = new ArrayList<>();
                    resourcesList = new ArrayList<>();
                    break;

            }

            int advertProgId = adProgram.getAdvertProgramId();
            if (advertProgId == 0) {
                advertProgId = Integer.parseInt(progIdsGenerated.get(generatedProgIdCount));
                generatedProgIdCount++;
            }

            prog.setDisplayLayout(displayLayout.getValue());
            prog.setEntityId(progEntityId);
            prog.setProgramId(advertProgId);
            prog.setStatus("NEW"); //To-Do check if program is already uploaded to DSM and send "NEW" otherwise sending something else..

            prog.setResources(resourcesList);
            prog.setDisplayTimesList(displayTimes);
            prog.setText(textList);

            programList.add(prog);

        }

        return programList;
    }

    /**
     *
     * @param terminalDetail
     * @param adScreenList
     * @param programIdList
     * @return
     */
    List<AdSetupRequest.TerminalDetail.Terminal> createTerminalList(AdSetupRequest.TerminalDetail terminalDetail, List<AdScreen> adScreenList, Map<Long, List<ProgramDetail.Program>> programScreenMap) {

        List<AdSetupRequest.TerminalDetail.Terminal> terminalList = new ArrayList<>();

        for (AdScreen adScreen : adScreenList) {

            List<ProgramDetail.Program> programs = programScreenMap.get(adScreen.getId());
            List<Integer> programIdList = new ArrayList<>();

            for (ProgramDetail.Program program : programs) {
                programIdList.add(program.getProgramId());
            }

            AdTerminal adTerminal = adScreen.getSupportTerminal(); //uncomment this when ready

            AdSetupRequest.TerminalDetail.Terminal terminal = terminalDetail.new Terminal();
            terminal.setProgramIdList(programIdList);
            terminal.setTaskIdX(adTerminal.getTaskIdX());
            terminal.setTaskIdY(adTerminal.getTaskIdY());
            terminal.setTaskName(adTerminal.getTerminalName());
            terminal.setTerminalId(adTerminal.getTerminalId());
            terminal.setTerminalHeight(adScreen.getDisplayHeight()); //get these from screen terminal supports
            terminal.setTerminalWidth(adScreen.getDisplayWidth());

            //adding only one terminal for now I think otherwise, the request can be too big if we do multiple terminals. But don't worry we support multiple terminals
            terminalList.add(terminal);

        }

        return terminalList;
    }

    /**
     *
     *
     * @param adRequest
     * @return
     */
    List<AdSetupRequest.ProgramDetail> createProgramDetailListOLD(AdSetupRequest adRequest) {

        AdSetupRequest.ProgramDetail progDetail = adRequest.new ProgramDetail();

        ProgramDetail.Program prog = progDetail.new Program();
        prog.setDisplayLayout("3SPLIT");
        prog.setProgramId(898899);
        prog.setStatus("NEW");

        DisplayTime displayTime1 = prog.new DisplayTime();
        displayTime1.setStarttime("21:06:49");
        displayTime1.setStoptime("21:07:49");

        DisplayTime displayTime2 = prog.new DisplayTime();
        displayTime2.setStarttime("21:06:49");
        displayTime2.setStoptime("21:07:49");

        List<DisplayTime> displayTimes = new ArrayList<>();
        displayTimes.add(displayTime1);
        displayTimes.add(displayTime2);

        List<Resources> resourcesList = new ArrayList<>();

        Resources resources = prog.new Resources();
        resources.setResourceDetail("restaurant_front.mp4");
        resources.setResourceId(480212808L);
        resources.setResourceType(1); // "VIDEO
        resources.setStatus("OLD");

        resourcesList.add(resources);

        prog.setResources(resourcesList);
        prog.setDisplayTimesList(displayTimes);

        List<ProgramDetail.Program> programIds = new ArrayList<>();
        programIds.add(prog);

        progDetail.setDisplayDate("2017-01-22");
        progDetail.setProgramIds(programIds);

        List<AdSetupRequest.ProgramDetail> programDetailList = new ArrayList<>();
        //add only a single day's ProgramDetail
        programDetailList.add(progDetail);

        return programDetailList;
    }

    /**
     *
     * @param adRequest
     * @return
     */
    List<AdSetupRequest.TerminalDetail> createTerminalDetailListOLD(AdSetupRequest adRequest) {

        AdSetupRequest.TerminalDetail playerDetail = adRequest.new TerminalDetail();
        playerDetail.setDisplayDate("2017-01-22");

        //program IDs
        List<Integer> programIdList = new ArrayList<>();
        programIdList.add(763838330);
        programIdList.add(543838330);
        programIdList.add(913838330);

        //Terminals
        AdSetupRequest.TerminalDetail.Terminal terminal = playerDetail.new Terminal();
        terminal.setProgramIdList(programIdList);
        terminal.setTaskIdX(839392829);
        terminal.setTaskName("First Task");
        terminal.setTerminalId("99877373738333");
        terminal.setTerminalHeight(1920);
        terminal.setTerminalWidth(1080);

        List<AdSetupRequest.TerminalDetail.Terminal> terminalList = new ArrayList<>();
        //adding only one terminal for now
        terminalList.add(terminal);

        playerDetail.setTerminals(terminalList);

        List<AdSetupRequest.TerminalDetail> playerDetailList = new ArrayList<>();
        //add only a single day's terminalDetail
        playerDetailList.add(playerDetail);

        //adRequest.setPlayerDetail(playerDetailList);
        return playerDetailList;
    }

    /**
     *
     * @param prog
     * @param textResources
     * @return
     */
    private List<Text> getTextList(ProgramDetail.Program prog, Set<AdText> textResources) {
        List<Text> textList = new ArrayList<>();

        for (AdText adText : textResources) {

            Text text = prog.new Text();

            text.setText(adText.getTextContent());
            text.setType(adText.getTextType().getValue());
            text.setTextId(adText.getTextId());

            textList.add(text);
        }

        return textList;

    }

    /**
     *
     * @param prog
     * @param adResources
     * @param fileIdsGenerated
     * @return
     */
    private List<Resources> getResourcesList(ProgramDetail.Program prog, Set<AdResource> adResources, List<String> fileIdsGenerated) {

        List<Resources> resourcesList = new ArrayList<>();
        int generatedResourceIdCount = 0;

        for (AdResource adResource : adResources) {

            long resourceId = adResource.getResourceId();

            if (resourceId == 0L) {

                resourceId = Long.parseLong(fileIdsGenerated.get(generatedResourceIdCount));
                generatedResourceIdCount++;
            }

            Resources resources = prog.new Resources();
            resources.setResourceDetail(adResource.getResourceName()); //"restaurant_front.mp4"
            resources.setResourceId(resourceId); //"5480212808"
            resources.setUploadId(adResource.getUploadId());//Id given when uploading to AdCentral, different from the file Id
            resources.setEntityId(adResource.getId());
            resources.setResourceType(adResource.getResourceType().getValue()); //"1" for "VIDEO"
            resources.setStatus("OLD"); //To-Do see how to get this field based on whether this resource is already uploaded onto DSM or not
            resources.setSequence(adResource.getSequence());
            resources.setIsUploadedToDSM(adResource.isIsUploadedToDSM());

            resourcesList.add(resources);
        }

        return resourcesList;
    }

    public void executeOLD(JobExecutionContext jec) throws JobExecutionException {

        JobDetail jobDetail = jec.getJobDetail();
        String jobName = jobDetail.getKey().getName();

        JobDataMap jobsDataMap = jec.getMergedJobDataMap();

        JobsConfig jobsData = (JobsConfig) jobsDataMap.get(jobName);

        RemoteRequest dbManagerUnit = jobsData.getRemoteUnitConfig().getAdDbManagerRemoteUnit();
        RemoteRequest dsmRemoteUnit = jobsData.getRemoteUnitConfig().getDSMBridgeRemoteUnit();
        HttpClientPool clientPool = (HttpClientPool) jobsDataMap.get(NamedConstants.CLIENT_POOL);
        DatabaseAdapter databaseAdapter = (DatabaseAdapter) jobsDataMap.get(NamedConstants.DB_ADAPTER);

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

        String response = clientPool.sendRemoteRequest(jsonRequest, dsmRemoteUnit);

        logger.info("Response from Central Server: " + response);

    }

    @Override
    public void interrupt() throws UnableToInterruptJobException {
        logger.warn("Failed to interrupt a Job before deleting it..");
    }

}
