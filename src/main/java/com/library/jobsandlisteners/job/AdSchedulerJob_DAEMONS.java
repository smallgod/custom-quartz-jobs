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
import com.library.customexception.MyCustomException;
import com.library.datamodel.Constants.APIMethodName;
import com.library.datamodel.Constants.EntityName;
import com.library.datamodel.Constants.FetchStatus;
import com.library.datamodel.Constants.GenerateId;
import com.library.datamodel.Constants.GenerateIdType;
import com.library.datamodel.Constants.NamedConstants;
import com.library.datamodel.Constants.ProgDisplayLayout;
import com.library.datamodel.Json.GenerateIdRequest;
import com.library.datamodel.Json.GeneratedIdResponse;
import com.library.datamodel.Json.AdSetupRequest;
import com.library.datamodel.Json.AdSetupRequest.ProgramDetail;
import com.library.datamodel.Json.AdSetupRequest.ProgramDetail.Program.DisplayTime;
import com.library.datamodel.Json.AdSetupRequest.ProgramDetail.Program.Resources;
import com.library.datamodel.Json.AdSetupRequest.ProgramDetail.Program.Text;
import com.library.datamodel.Json.FileUploadResponse;
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
import com.library.sglogger.util.LoggerUtil;
import com.library.utilities.FileDetails;
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
public class AdSchedulerJob_DAEMONS implements Job, InterruptableJob, ExecutableJob {

    private static final LoggerUtil logger = new LoggerUtil(AdSchedulerJob.class);

    @Override
    public void execute(JobExecutionContext jec) throws JobExecutionException {

        try {

            boolean hasAcquiredLock = NamedConstants.FETCH_SCHEDULE_LOCK.tryLock(30, TimeUnit.SECONDS);

            if (hasAcquiredLock) {

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
                DatabaseAdapter databaseAdapter = (DatabaseAdapter) jobsDataMap.get(NamedConstants.EXTERNAL_DB_ACCESS);

                Boolean triggerNow = Boolean.FALSE;
                Object triggerNowObj = jobsDataMap.get(NamedConstants.TRIGGER_NOW_PAYPROCESSOR);

                if (null != triggerNowObj) {
                    triggerNow = (Boolean) triggerNowObj;
                }

                Set<String> columnsToFetch = new HashSet<>();
                columnsToFetch.add("ALL");

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

                Set<AdSchedule> fetchedSchedules = databaseAdapter.fetchBulk(EntityName.AD_SCHEDULE, pendingAdsProps, columnsToFetch);

                if (null == fetchedSchedules || fetchedSchedules.isEmpty()) {

                    logger.debug("No Pending campaign schedule!");

                } else {

                    Set<AdSchedule> schedulesToUpdate = new HashSet<>();

                    AdSetupRequest adSetupRequest = new AdSetupRequest();

                    //Each Schedule represents a single(1) screen for that given date/day
                    for (AdSchedule adSchedule : fetchedSchedules) {
                        //Update the schedule in DB so that we dont fetch it again
                        adSchedule.setFetchStatus(FetchStatus.FETCHED);
                        schedulesToUpdate.add(adSchedule);

                        //AdScreen adScreen = null;
                        AdScreen adScreen = adSchedule.getAdScreen(); //"764::4563::T;905::2355::F;" ----> "prog_entity_id"::"time_in_millis::whether_prog_is_sent_to_dsm"
                        LocalDate displayDate = adSchedule.getDisplayDate();
                        String scheduleString = adSchedule.getScheduleDetail();
                        long scheduleId = adSchedule.getScheduleId();

                        //remove older times from schedule string
                        //int millisOfDayNow = DateUtils.getTimeNowToNearestMinute().getMillisOfDay();
                        int millisOfDayNow = DateUtils.getTimeNow().getMillisOfDay();

                        Map<Integer, Long> mapOfSchedulesAndProgIds = GeneralUtils.convertToMapStringOfSchedulesAndProgIds(scheduleString);
                        String newerScheduleString = "";

                        for (Map.Entry<Integer, Long> entry : mapOfSchedulesAndProgIds.entrySet()) {

                            int scheduleTime = entry.getKey();
                            long progEntityId = entry.getValue();

                            logger.debug("ProgEntityId : " + progEntityId);
                            logger.debug("millisOfDay  : " + DateUtils.convertLocalTimeToString(DateUtils.convertMillisToLocalTime(millisOfDayNow, DateTimeZone.UTC), NamedConstants.HOUR_MINUTE_SEC_FORMAT) + " - in millis: " + millisOfDayNow);
                            logger.debug("Schedule time: " + DateUtils.convertLocalTimeToString(DateUtils.convertMillisToLocalTime(scheduleTime, DateTimeZone.UTC), NamedConstants.HOUR_MINUTE_SEC_FORMAT) + " - in millis: " + scheduleTime);

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
                            //NO NEED TO GENERATE IDS FOR TEXT-ONLY ADS -> REALLY??? I THINK WE NEED IT FOR PROGRAM_IDS
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
                            //upload files to DSM if any
                            Map<Integer, List<AdSetupRequest.ProgramDetail.Program.Resources>> allUploadableMediaResources = getAllUploadableMediaResources(programs);

                            if (!(allUploadableMediaResources == null || allUploadableMediaResources.isEmpty())) {

                                uploadMediaResources(allUploadableMediaResources, databaseAdapter, dsmRemoteUnit);
                            }

                            //Send schedule Request to DSM
                            adSetupRequest.setMethodName(APIMethodName.UPLOAD_SCHEDULE_TO_DSM.getValue());
                            adSetupRequest.setProgramDetail(programDetailList);
                            adSetupRequest.setTerminalDetail(terminalDetailList);

                            String jsonReq = GeneralUtils.convertToJson(adSetupRequest, AdSetupRequest.class);

                            String response = clientPool.sendRemoteRequest(jsonReq, dsmRemoteUnit);
                            logger.debug("Mega wrapper Response: " + response);

                        } else {
                            logger.warn("There are empty or no AdPrograms returned for this schedule: " + newerScheduleString);
                        }

                    }

                    databaseAdapter.SaveOrUpdateBulk(EntityName.AD_SCHEDULE, schedulesToUpdate, Boolean.FALSE);
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

            logger.debug("Releasing SCHEDULE-CAMPAIGNS lock!");
            NamedConstants.FETCH_SCHEDULE_LOCK.unlock();
        }

    }

    /**
     * Update in db uploaded/deleted files
     *
     * @param deletedFiles
     * @param databaseAdapter
     * @throws MyCustomException
     */
    public void updateUploadedFilesHelper(Set<FileDetails> deletedFiles, DatabaseAdapter databaseAdapter) throws MyCustomException {

        for (FileDetails fileDetails : deletedFiles) {

            int campaignId = fileDetails.getCampaignId();
            String uploadId = fileDetails.getUploadId();
            long fileId = fileDetails.getFileId();

            Map<String, Object> resourceProps = new HashMap<>();
            resourceProps.put("uploadId", new HashSet<>(Arrays.asList(uploadId)));
            resourceProps.put("campaignId", new HashSet<>(Arrays.asList(campaignId)));

            //AdResource adResourceFromDB = (AdResource) databaseAdapter.fetchEntity(EntityName.AD_RESOURCE, resourceProps, columnsToFetch);
            //Set<AdResource> adResources = databaseAdapter.fetchResources(EntityName.AD_RESOURCE, AdResource.FETCH_RES_BY_CAMPNID_N_UPLDID, "uploadId", uploadId);
            Set<AdResource> adResources = databaseAdapter.fetchEntitiesByNamedQuery(EntityName.AD_RESOURCE, AdResource.FETCH_RES_BY_CAMPNID_N_UPLDID, resourceProps);

            if (!(adResources == null || adResources.isEmpty())) {

                AdResource adResourceFromDB = (AdResource) adResources.toArray()[0];

                adResourceFromDB.setIsUploadedToDSM(Boolean.TRUE);
                adResourceFromDB.setResourceId(fileId);

                logger.warn("Num of Res for this prog: " + adResourceFromDB.getAdResourcePrograms().size());

                databaseAdapter.saveOrUpdateEntity(adResourceFromDB, Boolean.FALSE);

            } else {
                logger.warn("AdResource with upload id: " + uploadId + ", NOT found in the database");
            }

        }
    }

    /**
     * Delete uploaded files helper
     *
     * @param fileUploadResponse
     * @param filesToUpload
     * @return
     */
    public Set<FileDetails> deleteFilesHelper(FileUploadResponse fileUploadResponse, Map<Long, FileDetails> filesToUpload) {

        Set<FileDetails> deletedFiles = new HashSet<>();
        if (fileUploadResponse != null) {

            Set<FileUploadResponse.Data> data = fileUploadResponse.getData();

            for (FileUploadResponse.Data item : data) {

                if (item.getIsUploaded()) {

                    if (!(filesToUpload == null || filesToUpload.isEmpty())) {

                        FileDetails fileDetails = filesToUpload.get(item.getFileId());
                        File fileToDelete = fileDetails.getFile();

                        boolean deleted = FileUtilities.deleteFile(fileToDelete);
                        if (deleted) {
                            deletedFiles.add(fileDetails);
                        }
                    } else {
                        logger.info("Files to Upload object is null or empty!");
                    }

                }
            }
        }

        return deletedFiles;

    }

    /**
     *
     * @param allUploadableMediaResources
     * @param databaseAdapter
     * @param dsmRemoteUnit
     * @throws MyCustomException
     */
    public void uploadMediaResources(Map<Integer, List<AdSetupRequest.ProgramDetail.Program.Resources>> allUploadableMediaResources, DatabaseAdapter databaseAdapter, RemoteRequest dsmRemoteUnit) throws MyCustomException {

        String fileUploadDir = NamedConstants.FILE_UPLOAD_DIR; //put this guy in the configs, see how to do it
        Map<Long, FileDetails> filesToUpload = new HashMap<>();
        int iteration = 1;

        for (Map.Entry<Integer, List<AdSetupRequest.ProgramDetail.Program.Resources>> entry : allUploadableMediaResources.entrySet()) {

            int campaignId = entry.getKey();
            List<AdSetupRequest.ProgramDetail.Program.Resources> resources = entry.getValue();

            for (AdSetupRequest.ProgramDetail.Program.Resources resource : resources) {

                boolean isUploadedToDSM = resource.isIsUploadedToDSM();

                logger.debug("Iteration no. " + iteration + ", for resource name: " + resource.getResourceDetail());
                logger.debug("Iterating no. " + iteration + ", for resource id  : " + resource.getUploadId());
                logger.debug("File is uploaded to DSM: " + isUploadedToDSM);

                if (!isUploadedToDSM) {

                    String uploadName = resource.getResourceDetail();
                    String uploadId = resource.getUploadId(); //this is the ID we assigned while uploading with a file extension
                    long fileId = resource.getResourceId(); //this is the ID we created at the DSM in the first call to DSM
                    String fileName = fileUploadDir + File.separator + uploadId;

                    FileDetails fileDetails = new FileDetails();
                    fileDetails.setCampaignId(campaignId);
                    fileDetails.setFileId(fileId);
                    fileDetails.setFileName(uploadName);
                    fileDetails.setUploadId(uploadId);
                    fileDetails.setFile(new File(fileName));

                    filesToUpload.put(fileId, fileDetails);

                }
            }

            //upload files
            FileUploadResponse fileUploadResponse = MultipartFileUploader.uploadFiles(filesToUpload, dsmRemoteUnit.getPreviewUrl());
            logger.debug("Upload Response: " + GeneralUtils.convertToJson(fileUploadResponse, FileUploadResponse.class));

            //delete uploaded files
            Set<FileDetails> deletedFiles = deleteFilesHelper(fileUploadResponse, filesToUpload);

            //update uploaded/deleted files
            updateUploadedFilesHelper(deletedFiles, databaseAdapter);

            iteration++;

        }//end for-loop through map of resources & campaignIds 
    }

    /**
     * Get all uploadable media resources from each of the programs
     *
     * @param programs
     * @return a map of campaignIds & related resources list
     */
    Map<Integer, List<AdSetupRequest.ProgramDetail.Program.Resources>> getAllUploadableMediaResources(List<ProgramDetail.Program> programs) {

        Map<Integer, List<AdSetupRequest.ProgramDetail.Program.Resources>> allUploadableMediaResources = new HashMap<>();

        for (ProgramDetail.Program eachProgram : programs) {

            List<AdSetupRequest.ProgramDetail.Program.Resources> programResources = eachProgram.getResources();
            int campaingId = eachProgram.getCampaignId();

            //create map of campaign Id and resources, we might want to add campaign_id on the adRequest
            allUploadableMediaResources.put(campaingId, programResources);

        }

        return allUploadableMediaResources;
    }

    /**
     *
     * @param progDetail
     * @param adProgram
     * @param adResources
     * @return
     */
    List<ProgramDetail.Program> createProgramList(DatabaseAdapter databaseAdapter, AdSetupRequest.ProgramDetail progDetail, Set<AdProgram> adProgramList, Map<Integer, List<Integer>> mapOfProgIdsAndDisplayTime, List<String> progIdsGenerated, List<String> fileIdsGenerated) throws MyCustomException {

        List<ProgramDetail.Program> programList = new ArrayList<>();
        ProgramDetail.Program prog;

        int generatedProgIdCount = 0;

        for (AdProgram adProgram : adProgramList) {

            prog = progDetail.new Program();

            long adLength = adProgram.getAdLength();
            int campaignId = adProgram.getCampaignId();
            ProgDisplayLayout displayLayout = adProgram.getDisplayLayout();

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
                    resourcesList = getResourcesList(prog, fileIdsGenerated, campaignId, databaseAdapter);
                    break;

                case AV_ONLY:
                    textList = new ArrayList<>();
                    resourcesList = new ArrayList<>();
                    break;

                case FULLSCREEN_TEXT:
                    textList = getTextList(prog, campaignId, databaseAdapter);
                    resourcesList = getResourcesList(prog, fileIdsGenerated, campaignId, databaseAdapter);
                    break;

                case TEXT_ONLY:
                    textList = getTextList(prog, campaignId, databaseAdapter);
                    resourcesList = new ArrayList<>();
                    break;

                case THREE_SPLIT:
                    textList = getTextList(prog, campaignId, databaseAdapter);
                    resourcesList = getResourcesList(prog, fileIdsGenerated, campaignId, databaseAdapter);
                    break;

                case TWO_SPLIT:
                    textList = getTextList(prog, campaignId, databaseAdapter);
                    resourcesList = getResourcesList(prog, fileIdsGenerated, campaignId, databaseAdapter);
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
            prog.setCampaignId(campaignId);
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
     * @param prog
     * @param textResources
     * @return
     */
    private List<Text> getTextList(ProgramDetail.Program prog, int campaignId, DatabaseAdapter databaseAdapter) throws MyCustomException {

        //Set<AdText> textResources = databaseAdapter.fetchResources(EntityName.AD_TEXT, AdText.FETCH_TEXT, "campaignId", campaignId);
        Set<AdText> textResources = databaseAdapter.fetchResources(EntityName.AD_TEXT, AdText.FETCH_TEXT, "adTextPrograms.campaignId", campaignId);

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
    private List<Resources> getResourcesList(ProgramDetail.Program prog, List<String> fileIdsGenerated, int campaignId, DatabaseAdapter databaseAdapter) throws MyCustomException {

        Set<AdResource> adResources = databaseAdapter.fetchResources(EntityName.AD_RESOURCE, AdResource.FETCH_RESOURCE_BY_CAMPAIGNID, "adResourcePrograms.campaignId", campaignId);

        logger.debug("Size of resources for campaignId: " + campaignId + ", is: " + adResources.size());

        List<Resources> resourcesList = new ArrayList<>();
        int generatedResourceIdCount = 0;

        for (AdResource adResource : adResources) {

            long resourceId = adResource.getResourceId();

            if (resourceId == 0L) {

                resourceId = Long.parseLong(fileIdsGenerated.get(generatedResourceIdCount));
                generatedResourceIdCount++;
            }

            Resources resources = prog.new Resources();
            resources.setResourceDetail(adResource.getResourceName());
            resources.setResourceId(resourceId);
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

    public void executeOLD(JobExecutionContext jec) throws JobExecutionException, MyCustomException {

        JobDetail jobDetail = jec.getJobDetail();
        String jobName = jobDetail.getKey().getName();

        JobDataMap jobsDataMap = jec.getMergedJobDataMap();

        JobsConfig jobsData = (JobsConfig) jobsDataMap.get(jobName);

        RemoteRequest dbManagerUnit = jobsData.getRemoteUnitConfig().getAdDbManagerRemoteUnit();
        RemoteRequest dsmRemoteUnit = jobsData.getRemoteUnitConfig().getDSMBridgeRemoteUnit();
        HttpClientPool clientPool = (HttpClientPool) jobsDataMap.get(NamedConstants.CLIENT_POOL);
        DatabaseAdapter databaseAdapter = (DatabaseAdapter) jobsDataMap.get(NamedConstants.EXTERNAL_DB_ACCESS);

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
