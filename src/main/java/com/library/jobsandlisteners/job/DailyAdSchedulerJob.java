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
import com.library.datamodel.Constants.GenerateId;
import com.library.datamodel.Constants.GenerateIdType;
import com.library.datamodel.Constants.NamedConstants;
import com.library.datamodel.Json.GenerateIdRequest;
import com.library.datamodel.Json.GeneratedIdResponse;
import com.library.datamodel.Json.AdSetupRequest;
import com.library.datamodel.Json.AdSetupRequest.ProgramDetail;
import com.library.datamodel.Json.AdSetupRequest.ProgramDetail.Program.DisplayTime;
import com.library.datamodel.Json.AdSetupRequest.ProgramDetail.Program.Resources;
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
import com.library.utilities.LoggerUtil;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.openide.util.Exceptions;

/**
 * The class doing the work
 *
 * @author smallgod
 */
public class DailyAdSchedulerJob implements Job, InterruptableJob, ExecutableJob {

    private static final LoggerUtil logger = new LoggerUtil(DailyAdSchedulerJob.class);

    //this method is for testing purpose only, delete after
    @Override
    public void execute(JobExecutionContext jec) throws JobExecutionException {

        try {
            //what this job will accomplish
            //1. generate IDs - FileIds, programIds, TaskIds (if need be for new ones everytime)
            //2. Upload to DSM, required resources that are not yet uploaded to server
            //3. Reset the text-startId count tracker
            //4. Unbind players -> Should we consider assigning all the terminals a reset current looop task ID and then re-assigning today's loop task id for those that have tasks to play
            //the reset current loop task ID can be our default adverts that display in the event of no assigned adverts
            //make sure there is very limited or no downloading of resources by the terminals for this reset task id.
            //we can schedule this job for 23.56 to 23.57 so that we don't interfere with midnight. During this 1 minute, terminals are not available for advertising, they are being reset
            //5. Delete all files under the taskID and re-create new ones (this means text iDs might change but resource ids for exisingting programs ont
            //6. Check if there are any newly added terminals so that taskIds can be created against those terminals.
            //Store those terminal Ids against task Ids in the central database
            //I think we should instead have a job creaed that runs on the DSM every 15 minutes, and if a terminal is added, it calls the central unit to inform
            //7. Bind players/terminals to the inidividual tasks
            //tasks 3,4 & 1 should be performed together
            //task 2,5 & 7 should be performed together but in a separate/second call
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

            //1. SETUP STEP-ONE
            GenerateIdRequest generateIdRequest = new GenerateIdRequest();

            List<GenerateIdRequest.Params> paramsList = new ArrayList<>();

            GenerateIdRequest.Params fileIdParam = generateIdRequest.new Params();
            fileIdParam.setId(GenerateId.FILE_ID.getValue());
            fileIdParam.setIdTypeToGenerate(GenerateIdType.LONG.getValue());
            fileIdParam.setNumOfIds(5); //get from DB
            fileIdParam.setExistingIdList(new ArrayList<String>());

            GenerateIdRequest.Params programIdParam = generateIdRequest.new Params();
            programIdParam.setId(GenerateId.FILE_ID.getValue());
            programIdParam.setIdTypeToGenerate(GenerateIdType.LONG.getValue());
            programIdParam.setNumOfIds(5); //get from DB
            programIdParam.setExistingIdList(new ArrayList<String>());

            paramsList.add(fileIdParam);
            paramsList.add(programIdParam);

            generateIdRequest.setMethodName(APIMethodName.GENERATE_IDS_AT_DSM.getValue());
            generateIdRequest.setParams(paramsList);

            String generateIdJsonRequest = GeneralUtils.convertToJson(generateIdRequest, GenerateIdRequest.class);

            String generateIdJsonResponse = clientPool.sendRemoteRequest(generateIdJsonRequest, dsmRemoteUnit);

            GeneratedIdResponse genIdResponse = GeneralUtils.convertFromJson(generateIdJsonResponse, GeneratedIdResponse.class);

            //2. SETUP STEP-TWO
            // List<String> generatedIdList = genIdResponse.getGeneratedIdList();
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
            List<AdSetupRequest.TerminalDetail> playerDetailList = createPlayerDetailList(adRequest);

            adRequest.setTerminalDetail(playerDetailList);
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
        } catch (MyCustomException ex) {
            Exceptions.printStackTrace(ex);
        } catch (Exception ex) {
            logger.error("An Error occurred in AdScheduleJob: " + ex.getMessage());
            ex.printStackTrace();
        }
    }

//    ResourceDetail createResourceDetail() {
//
//        ResourceDetail resourceDetail = new ResourceDetail();
//        ResourceDetail.TerminalDetail playerDetail = resourceDetail.new TerminalDetail();
//
//        ResourceDetail.TerminalDetail.Resources resources = playerDetail.new Resources();
//        resources.setResourceDetail("restaurant_front.mp4");
//        resources.setResourceId("5480212808");
//        resources.setResourceType("VIDEO");
//        resources.setStatus("OLD");
//
//        List<ResourceDetail.TerminalDetail.Resources> resourcesList = new ArrayList<>();
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
        prog.setProgramId(80900778);
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
        resources.setResourceId(5480212808L);
        resources.setResourceType(1);
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
    List<AdSetupRequest.TerminalDetail> createPlayerDetailList(AdSetupRequest adRequest) {

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
        terminal.setTaskIdY(839392829);
        terminal.setTaskName("First Task");
        terminal.setTerminalId("99877373738333");
        terminal.setTerminalHeight(1920);
        terminal.setTerminalWidth(1080);

        List<AdSetupRequest.TerminalDetail.Terminal> terminalList = new ArrayList<>();
        //adding only one terminal for now
        terminalList.add(terminal);

        playerDetail.setTerminals(terminalList);

        List<AdSetupRequest.TerminalDetail> playerDetailList = new ArrayList<>();
        //add only a single day's playerDetail
        playerDetailList.add(playerDetail);

        //adRequest.setPlayerDetail(playerDetailList);
        return playerDetailList;
    }

    public void executeOLD(JobExecutionContext jec) throws JobExecutionException, MyCustomException {

        JobDetail jobDetail = jec.getJobDetail();
        String jobName = jobDetail.getKey().getName();

        JobDataMap jobsDataMap = jec.getMergedJobDataMap();

        JobsConfig jobsData = (JobsConfig) jobsDataMap.get(jobName);

        
        RemoteRequest centralUnit = jobsData.getRemoteUnitConfig().getAdCentralRemoteUnit();
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

        String response = clientPool.sendRemoteRequest(jsonRequest, centralUnit);

        logger.info("Response from Central Server: " + response);

    }

    @Override
    public void interrupt() throws UnableToInterruptJobException {
        logger.warn("Failed to interrupt a Job before deleting it..");
    }

}
