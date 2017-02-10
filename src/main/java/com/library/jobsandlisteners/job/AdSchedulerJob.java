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
import com.library.datamodel.Constants.GenerateId;
import com.library.datamodel.Constants.GenerateIdType;
import com.library.datamodel.Constants.NamedConstants;
import com.library.datamodel.Json.GenerateIdRequest;
import com.library.datamodel.Json.GeneratedIdResponse;
import com.library.datamodel.Json.AdSetupRequest;
import com.library.datamodel.Json.AdSetupRequest.ProgramDetail;
import com.library.datamodel.Json.AdSetupRequest.ProgramDetail.Program.DisplayTime;
import com.library.datamodel.Json.AdSetupRequest.ProgramDetail.Program.Resources;
import com.library.datamodel.model.v1_0.AdClient;
import com.library.datamodel.model.v1_0.AdProgram;
import com.library.datamodel.model.v1_0.AdSchedule;
import com.library.datamodel.model.v1_0.AdScreen;
import com.library.dbadapter.DatabaseAdapter;
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
import com.library.utilities.LoggerUtil;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.joda.time.LocalDate;

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

        //assume we are generating 2 file ids only
        //we should also send ids we think exist to the dsm bridge service to confirm for us, so that if they don't exist, new ids are generated
        JobDetail jobDetail = jec.getJobDetail();
        String jobName = jobDetail.getKey().getName();

        JobDataMap jobsDataMap = jec.getMergedJobDataMap();

        JobsConfig jobsData = (JobsConfig) jobsDataMap.get(jobName);

        RemoteRequest dbManagerUnit = jobsData.getRemoteUnitConfig().getAdDbManagerRemoteUnit();
        RemoteRequest dsmRemoteUnit = jobsData.getRemoteUnitConfig().getDSMBridgeRemoteUnit();
        HttpClientPool clientPool = (HttpClientPool) jobsDataMap.get(NamedConstants.CLIENT_POOL);
        DatabaseAdapter databaseAdapter = (DatabaseAdapter) jobsDataMap.get(NamedConstants.DB_ADAPTER);

        //Fetch pending ad programs from DB
        //Fetch Records from AdScheduler Table for date (today) and with to_update field == False
        //If found, get screen_id / terminal_id for this pending entry
        //If found, do the needful and send request to DSM
        List< String> columnsToFetch = new ArrayList<>();
        columnsToFetch.add("ALL");

        Map< String, Object> pendingAdsProps = new HashMap<>();
        pendingAdsProps.put("isToUpdate", Boolean.FALSE);
        pendingAdsProps.put("displayDate", DateUtils.getDateNow());

        List< AdSchedule> fetchedSchedules = databaseAdapter.fetchBulkWithSingleValues(EntityName.AD_SCHEDULE, pendingAdsProps, columnsToFetch);

        if (!fetchedSchedules.isEmpty()) {

            for (AdSchedule adSchedule : fetchedSchedules) {

                AdScreen adScreen = adSchedule.getAdScreen(); //"764::4563::T;905::2355::F;" ----> "prog_entity_id"::"time_in_millis::whether_prog_is_sent_to_dsm"
                LocalDate displayDate = adSchedule.getDisplayDate();
                String scheduleString = adSchedule.getScheduleDetail();
                long scheduleId = adSchedule.getScheduleId();

                String[] progTimeArray = scheduleString.trim().split("\\s*;\\s*"); // ["764::4563::T", "905::2355::F"]

                logger.debug("ProgTimeArray: " + Arrays.toString(progTimeArray));

                List< Integer> displayTimeList = new ArrayList<>();
                List< Integer> progEntityIdList = new ArrayList<>();

                for (String element : progTimeArray) {
                    int progEntityId = Integer.parseInt(element.split("\\s*::\\s*")[0]);
                    int displayTime = Integer.parseInt(element.split("\\s*::\\s*")[1]);

                    displayTimeList.add(displayTime);
                    progEntityIdList.add(progEntityId);
                }

                Map<String, List<Object>> programProps = new HashMap<>();

                List<Object> displayDates = new ArrayList<>();
                displayDates.add(DateUtils.getDateNow()); //putting it in the List<Object> for the sake
                
                programProps.put("id", new ArrayList<Object>(progEntityIdList));
                programProps.put("displayDate", displayDates);

                //Fetch Program details to check which ones we need to generate ids for
                List< AdProgram> fetchedPrograms = databaseAdapter.fetchBulkWithMultipleValues(EntityName.AD_PROGRAM, programProps, columnsToFetch);
                
                int numOfIdsToGenerate = 0;
                List<String> existingProgIds = new ArrayList<>();
                for (AdProgram adProgram : fetchedPrograms){
                    
                    AdClient client = adProgram.getAdClient();
                    Boolean isDSMUpdated = adProgram.isIsDSMUpdated();
                    int adLength = adProgram.getAdLength();
                    numOfIdsToGenerate = adProgram.getNumOfFileResources();
                }

                //generate file ids
                GenerateIdRequest generateIdRequest = new GenerateIdRequest();
                List<GenerateIdRequest.Params> params = new ArrayList<>();

                GenerateIdRequest.Params fileIdParams = generateIdRequest.new Params();
                fileIdParams.setId(GenerateId.FILE_ID.getValue());
                fileIdParams.setIdTypeToGenerate(GenerateIdType.LONG.getValue());
                fileIdParams.setNumOfIds(numOfIdsToGenerate);
                params.add(fileIdParams);
                
                GenerateIdRequest.Params progIdParams = generateIdRequest.new Params();
                progIdParams.setId(GenerateId.PROGRAM_ID.getValue());
                progIdParams.setIdTypeToGenerate(GenerateIdType.INTEGER.getValue());
                progIdParams.setNumOfIds(numOfIdsToGenerate);
                params.add(progIdParams);

                generateIdRequest.setMethodName(APIMethodName.GENERATE_ID.getValue());
                generateIdRequest.setParams(params);

                String generateIdJsonRequest = GeneralUtils.convertToJson(generateIdRequest, GenerateIdRequest.class);

                String generateIdJsonResponse = clientPool.sendRemoteRequest(generateIdJsonRequest, dsmRemoteUnit);
                                                                                    
                GeneratedIdResponse genIdResponse = GeneralUtils.convertFromJson(generateIdJsonResponse, GeneratedIdResponse.class);
                List<String> generatedIdList = genIdResponse.getGeneratedIdList();

                AdSetupRequest adRequest = new AdSetupRequest();
                adRequest.setMethodName(APIMethodName.BULK_ADVERT_SETUP.getValue());

                //update today's resources in DB with the generated Ids for each resource before resources are uploaded to the server
                //updateDb
                //generate task ids
                //send requests
                //
                //
                //program detail
                List<AdSetupRequest.ProgramDetail> progDetailList = createProgramDetailList(adRequest);

                //resource details
                //ResourceDetail resDetail = createResourceDetail();
                //player details
                List<AdSetupRequest.PlayerDetail> playerDetailList = createPlayerDetailList(adRequest);

                adRequest.setPlayerDetail(playerDetailList);
                adRequest.setProgramDetail(progDetailList);

                String jsonReq = GeneralUtils.convertToJson(adRequest, AdSetupRequest.class);

                String response = clientPool.sendRemoteRequest(jsonReq, dsmRemoteUnit);

                logger.debug("Mega wrapper Response: " + response);

                /*
                String jsonRequest = GeneralUtils.convertToJson(adRequest, AdSetupRequest.class);

                logger.debug("Player Detail Request: " + jsonRequest);

                String response = clientPool.sendRemoteRequest(jsonRequest, dsmRemoteUnit);

                logger.info("Response from Central Server: " + response);

                String resourceDetail = "{\"method\":\"RESOURCE_DETAIL\",\"playerDetail\":[{\"display_date\":\"2017-01-10\",\"resources\":[{\"resource_id\":5480212808,\"resource_detail\":\"restaurant_front.mp4\",\"resource_type\":\"VIDEO\",\"status\":\"OLD\"},{\"resource_id\":2481434800,\"resource_detail\":\"This is Header text [DEL] This is scrolling text here..\",\"resource_type\":\"TEXT\",\"status\":\"NEW\"},{\"resource_id\":2481434800,\"resource_detail\":\"swimming pool.jpg\",\"resource_type\":\"IMAGE\",\"status\":\"NEW\"}]}]}";

                String programDetail = "{\"method\":\"PROGRAM_DETAIL\",\"playerDetail\":[{\"display_date\":\"2017-01-10\",\"program_ids\":[{\"program_id\":19011480463480900778,\"status\":\"UPDATED\",\"display_layout\":\"3SPLIT\",\"display_times\":[{\"starttime\":\"21:06:49\",\"stoptime\":\"21:07:49\"},{\"starttime\":\"22:06:49\",\"stoptime\":\"22:07:49\"}],\"resource_ids\":[1580212807,6290434822,2481434800]},{\"program_id\":97011480463480900778,\"status\":\"NEW\",\"display_layout\":\"TEXT_ONLY\",\"display_times\":[{\"starttime\":\"21:06:49\",\"stoptime\":\"21:07:49\"},{\"starttime\":\"22:06:49\",\"stoptime\":\"22:07:49\"}],\"resource_ids\":[5480212808]}]}]}";

                String response2 = clientPool.sendRemoteRequest(resourceDetail, dsmRemoteUnit);

                logger.info("ResourceDetail Response from Server: " + response2);

                String response3 = clientPool.sendRemoteRequest(programDetail, dsmRemoteUnit);

                logger.info("ProgramDetail Response from Server: " + response3);
                 */
            }

        } else {
            logger.info("No Pending AdSchedules where fetched from the database");
        }
    }

//    ResourceDetail createResourceDetail() {
//
//        ResourceDetail resourceDetail = new ResourceDetail();
//        ResourceDetail.PlayerDetail playerDetail = resourceDetail.new PlayerDetail();
//
//        ResourceDetail.PlayerDetail.Resources resources = playerDetail.new Resources();
//        resources.setResourceDetail("restaurant_front.mp4");
//        resources.setResourceId("5480212808");
//        resources.setResourceType("VIDEO");
//        resources.setStatus("OLD");
//
//        List<ResourceDetail.PlayerDetail.Resources> resourcesList = new ArrayList<>();
//
//        playerDetail.setDisplayDate("2017-01-13");
//        playerDetail.setResources(resourcesList);
//
//        return resourceDetail;
//
//    }
    /**
     *
     * @param adRequest
     * @return
     */
    List<AdSetupRequest.ProgramDetail> createProgramDetailList(AdSetupRequest adRequest) {

        AdSetupRequest.ProgramDetail progDetail = adRequest.new ProgramDetail();

        ProgramDetail.Program prog = progDetail.new Program();
        prog.setDisplayLayout("3SPLIT");
        prog.setProgramId("19011480463480900778");
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
        resources.setResourceId("5480212808");
        resources.setResourceType("VIDEO");
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
    List<AdSetupRequest.PlayerDetail> createPlayerDetailList(AdSetupRequest adRequest) {

        AdSetupRequest.PlayerDetail playerDetail = adRequest.new PlayerDetail();
        playerDetail.setDisplayDate("2017-01-22");

        //program IDs
        List<Long> programIdList = new ArrayList<>();
        programIdList.add(763838330L);
        programIdList.add(543838330L);
        programIdList.add(913838330L);

        //Terminals
        AdSetupRequest.PlayerDetail.TerminalDetail terminal = playerDetail.new TerminalDetail();
        terminal.setProgramIdList(programIdList);
        terminal.setTaskId(839392829);
        terminal.setTaskName("First Task");
        terminal.setTerminalId("99877373738333");
        terminal.setTerminalHeight(1920);
        terminal.setTerminalWidth(1080);

        List<AdSetupRequest.PlayerDetail.TerminalDetail> terminalList = new ArrayList<>();
        //adding only one terminal for now
        terminalList.add(terminal);

        playerDetail.setTerminals(terminalList);

        List<AdSetupRequest.PlayerDetail> playerDetailList = new ArrayList<>();
        //add only a single day's playerDetail
        playerDetailList.add(playerDetail);

        //adRequest.setPlayerDetail(playerDetailList);
        return playerDetailList;
    }

    public void executeOLD(JobExecutionContext jec) throws JobExecutionException {

        JobDetail jobDetail = jec.getJobDetail();
        String jobName = jobDetail.getKey().getName();

        JobDataMap jobsDataMap = jec.getMergedJobDataMap();

        JobsConfig jobsData = (JobsConfig) jobsDataMap.get(jobName);
        HttpClientPool clientPool = (HttpClientPool) jobsDataMap.get(NamedConstants.CLIENT_POOL);

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

        Map<String, RemoteRequest> remoteUnits = jobsData.getRemoteRequestUnits();

        RemoteRequest centralUnit = remoteUnits.get(NamedConstants.CENTRAL_UNIT_REQUEST);

        String response = clientPool.sendRemoteRequest(jsonRequest, centralUnit);

        logger.info("Response from Central Server: " + response);

    }

    @Override
    public void interrupt() throws UnableToInterruptJobException {
        logger.warn("Failed to interrupt a Job before deleting it..");
    }

}
