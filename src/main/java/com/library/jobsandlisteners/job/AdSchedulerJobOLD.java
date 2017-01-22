/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.library.jobsandlisteners.job;

//import com.library.datamodel.Constants.ProcessingUnitState;
//import com.library.datamodel.Json.AdFetchRequest;
//import com.library.httpconnmanager.HttpClientPool;
//import com.library.configs.JobsConfig;
//import com.library.datamodel.Constants.APIMethodName;
//import com.library.datamodel.Constants.GenerateId;
//import com.library.datamodel.Constants.GenerateIdType;
//import com.library.datamodel.Constants.NamedConstants;
//import com.library.datamodel.Json.GenerateIdRequest;
//import com.library.datamodel.Json.GeneratedIdResponse;
//import com.library.datamodel.Json.AdSetupRequestWrapper;
//import com.library.datamodel.Json.AdSetupRequest;
//import com.library.datamodel.Json.ProgramDetail;
//import com.library.sgsharedinterface.ExecutableJob;
//import com.library.utilities.GeneralUtils;
//import org.quartz.InterruptableJob;
//import org.quartz.Job;
//import org.quartz.JobDataMap;
//import org.quartz.JobDetail;
//import org.quartz.JobExecutionContext;
//import org.quartz.JobExecutionException;
//import org.quartz.UnableToInterruptJobException;
//import com.library.sgsharedinterface.RemoteRequest;
//import com.library.utilities.LoggerUtil;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Map;
//
///**
// * The class doing the work
// *
// * @author smallgod
// */
//public class AdSchedulerJobOLD implements Job, InterruptableJob, ExecutableJob {
//
//    private static final LoggerUtil logger = new LoggerUtil(AdSchedulerJobOLD.class);
//
//    //this method is for testing purpose only, delete after
//    @Override
//    public void execute(JobExecutionContext jec) throws JobExecutionException {
//
//        //assume we are generating 2 file ids only
//        //we should also send ids we think exist to the dsm bridge service to confirm for us, so that if they don't exist, new ids are generated
//        JobDetail jobDetail = jec.getJobDetail();
//        String jobName = jobDetail.getKey().getName();
//
//        JobDataMap jobsDataMap = jec.getMergedJobDataMap();
//
//        JobsConfig jobsData = (JobsConfig) jobsDataMap.get(jobName);
//        Map<String, RemoteRequest> remoteUnits = jobsData.getRemoteRequestUnits();
//
//        RemoteRequest dsmRemoteUnit = remoteUnits.get(NamedConstants.DSM_UNIT_REQUEST);
//
//        HttpClientPool clientPool = (HttpClientPool) jobsDataMap.get(NamedConstants.CLIENT_POOL);
//
//        //generate file ids
//        GenerateIdRequest generateIdRequest = new GenerateIdRequest();
//
//        GenerateIdRequest.Params params = generateIdRequest.new Params();
//        params.setId(GenerateId.FILE_ID.getValue());
//        params.setIdTypeToGenerate(GenerateIdType.LONG.getValue());
//        params.getNumOfIds();
//
//        generateIdRequest.setMethodName(APIMethodName.GENERATE_ID.getValue());
//        generateIdRequest.setParams(params);
//
//        String generateIdJsonRequest = GeneralUtils.convertToJson(generateIdRequest, GenerateIdRequest.class);
//
//        String generateIdJsonResponse = clientPool.sendRemoteRequest(generateIdJsonRequest, dsmRemoteUnit);
//
//        GeneratedIdResponse genIdResponse = GeneralUtils.convertFromJson(generateIdJsonResponse, GeneratedIdResponse.class);
//        List<String> generatedIdList = genIdResponse.getGeneratedIdList();
//
//        //update today's resources in DB with the generated Ids for each resource before resources are uploaded to the server
//        //updateDb
//        //generate task ids
//        //send requests
//        //
//        //
//        //program detail
//        ProgramDetail progDetail = createProgramDetail();
//
//        //resource details
//        //ResourceDetail resDetail = createResourceDetail();
//
//        //player details
//        AdSetupRequest playerDetail = createPlayerDetail();
//
//        AdSetupRequestWrapper wrapper = new AdSetupRequestWrapper();
//        wrapper.setMethodName(APIMethodName.BULK_ADVERT_SETUP.getValue());
//        wrapper.setPlayerDetail(playerDetail);
//        wrapper.setProgramDetail(progDetail);
//        //wrapper.setResourceDetail(resDetail);
//
//        String jsonReq = GeneralUtils.convertToJson(wrapper, AdSetupRequestWrapper.class);
//
//        String response = clientPool.sendRemoteRequest(jsonReq, dsmRemoteUnit);
//
//        logger.debug("Mega wrapper Response: " + response);
//
//        /*
//        String jsonRequest = GeneralUtils.convertToJson(playerDetail, AdSetupRequest.class);
//
//        logger.debug("Player Detail Request: " + jsonRequest);
//
//        String response = clientPool.sendRemoteRequest(jsonRequest, dsmRemoteUnit);
//
//        logger.info("Response from Central Server: " + response);
//
//        String resourceDetail = "{\"method\":\"RESOURCE_DETAIL\",\"data\":[{\"display_date\":\"2017-01-10\",\"resources\":[{\"resource_id\":5480212808,\"resource_detail\":\"restaurant_front.mp4\",\"resource_type\":\"VIDEO\",\"status\":\"OLD\"},{\"resource_id\":2481434800,\"resource_detail\":\"This is Header text [DEL] This is scrolling text here..\",\"resource_type\":\"TEXT\",\"status\":\"NEW\"},{\"resource_id\":2481434800,\"resource_detail\":\"swimming pool.jpg\",\"resource_type\":\"IMAGE\",\"status\":\"NEW\"}]}]}";
//
//        String programDetail = "{\"method\":\"PROGRAM_DETAIL\",\"data\":[{\"display_date\":\"2017-01-10\",\"program_ids\":[{\"program_id\":19011480463480900778,\"status\":\"UPDATED\",\"display_layout\":\"3SPLIT\",\"display_times\":[{\"starttime\":\"21:06:49\",\"stoptime\":\"21:07:49\"},{\"starttime\":\"22:06:49\",\"stoptime\":\"22:07:49\"}],\"resource_ids\":[1580212807,6290434822,2481434800]},{\"program_id\":97011480463480900778,\"status\":\"NEW\",\"display_layout\":\"TEXT_ONLY\",\"display_times\":[{\"starttime\":\"21:06:49\",\"stoptime\":\"21:07:49\"},{\"starttime\":\"22:06:49\",\"stoptime\":\"22:07:49\"}],\"resource_ids\":[5480212808]}]}]}";
//
//        String response2 = clientPool.sendRemoteRequest(resourceDetail, dsmRemoteUnit);
//
//        logger.info("ResourceDetail Response from Server: " + response2);
//
//        String response3 = clientPool.sendRemoteRequest(programDetail, dsmRemoteUnit);
//
//        logger.info("ProgramDetail Response from Server: " + response3);
//         */
//    }
//
////    ResourceDetail createResourceDetail() {
////
////        ResourceDetail resourceDetail = new ResourceDetail();
////        ResourceDetail.PlayerDetail data = resourceDetail.new PlayerDetail();
////
////        ResourceDetail.PlayerDetail.Resources resources = data.new Resources();
////        resources.setResourceDetail("restaurant_front.mp4");
////        resources.setResourceId("5480212808");
////        resources.setResourceType("VIDEO");
////        resources.setStatus("OLD");
////
////        List<ResourceDetail.PlayerDetail.Resources> resourcesList = new ArrayList<>();
////
////        data.setDisplayDate("2017-01-13");
////        data.setResources(resourcesList);
////
////        return resourceDetail;
////
////    }
//
//    ProgramDetail createProgramDetail() {
//
//        ProgramDetail progDetail = new ProgramDetail();
//        ProgramDetail.Data data = progDetail.new Data();
//
//        ProgramDetail.Data.Program prog = data.new Program();
//        prog.setDisplayLayout("3SPLIT");
//        prog.setProgramId("19011480463480900778");
//        prog.setStatus("NEW");
//
//        ProgramDetail.Data.Program.DisplayTime displayTime1 = prog.new DisplayTime();
//        displayTime1.setStarttime("21:06:49");
//        displayTime1.setStoptime("21:07:49");
//
//        ProgramDetail.Data.Program.DisplayTime displayTime2 = prog.new DisplayTime();
//        displayTime2.setStarttime("21:06:49");
//        displayTime2.setStoptime("21:07:49");
//
//        List<ProgramDetail.Data.Program.DisplayTime> displayTimes = new ArrayList<>();
//        displayTimes.add(displayTime1);
//        displayTimes.add(displayTime2);
//
//        List<ProgramDetail.Data.Program.Resources> resourcesList = new ArrayList<>();
//        
//        
//        ProgramDetail.Data.Program.Resources resources = prog.new  Resources();
//        resources.setResourceDetail("restaurant_front.mp4");
//        resources.setResourceId("5480212808");
//        resources.setResourceType("VIDEO");
//        resources.setStatus("OLD");
//        
//        resourcesList.add(resources);
//
//        prog.setResources(resourcesList);
//        prog.setDisplayTimesList(displayTimes);
//
//        List<ProgramDetail.Data.Program> programIds = new ArrayList<>();
//        programIds.add(prog);
//
//        data.setDisplayDate("2017-01-13");
//        data.setProgramIds(programIds);
//
//        return progDetail;
//    }
//
//    AdSetupRequest createPlayerDetail() {
//
//        AdSetupRequest playerDetail = new AdSetupRequest();
//
//        playerDetail.setMethodName(APIMethodName.PLAYER_DETAIL.getValue());
//
//        AdSetupRequest.PlayerDetail data = playerDetail.new PlayerDetail();
//        data.setDisplayDate("2017-01-14");
//
//        //program IDs
//        List<Long> programIdList = new ArrayList<>();
//        programIdList.add(763838330L);
//        programIdList.add(543838330L);
//        programIdList.add(913838330L);
//
//        //Terminals
//        AdSetupRequest.PlayerDetail.TerminalDetail terminal = data.new TerminalDetail();
//        terminal.setProgramIdList(programIdList);
//        terminal.setTaskId(839392829);
//        terminal.setTaskName("First Task");
//        terminal.setTerminalId("99877373738333");
//        terminal.setTerminalHeight(1920);
//        terminal.setTerminalWidth(1080);
//
//        List<AdSetupRequest.PlayerDetail.TerminalDetail> terminalList = new ArrayList<>();
//        //adding only one terminal for now
//        terminalList.add(terminal);
//
//        data.setTerminals(terminalList);
//
//        List<AdSetupRequest.PlayerDetail> dataList = new ArrayList<>();
//        //add only a single day's data
//        dataList.add(data);
//
//        playerDetail.setPlayerDetail(dataList);
//
//        return playerDetail;
//    }
//
//    public void executeOLD(JobExecutionContext jec) throws JobExecutionException {
//
//        JobDetail jobDetail = jec.getJobDetail();
//        String jobName = jobDetail.getKey().getName();
//
//        JobDataMap jobsDataMap = jec.getMergedJobDataMap();
//
//        JobsConfig jobsData = (JobsConfig) jobsDataMap.get(jobName);
//        HttpClientPool clientPool = (HttpClientPool) jobsDataMap.get(NamedConstants.CLIENT_POOL);
//
//        //logger.debug("size of jobMap: " + jobMap.size());
//        /*logger.debug("sleeping for 30s at: " + new DateTime().getSecondOfDay());
//         try {
//         logger.debug("mimick job execution.....");
//         Thread.sleep(30000L);
//         } catch (InterruptedException ex) {logger.debug("error trying to sleep: " + ex.getMessage());
//         }
//         logger.debug("done sleeping for 30s at: " + new DateTime().getSecondOfDay());
//         */
//        AdFetchRequest request = new AdFetchRequest();
//        request.setMethodName("ADFETCH_REQUEST");
//        AdFetchRequest.Params params = request.new Params();
//        params.setStatus(ProcessingUnitState.POLL.getValue()); //need Enum
//        request.setParams(params);
//
//        String jsonRequest = GeneralUtils.convertToJson(request, AdFetchRequest.class);
//
//        logger.debug("New AdFetch Request: " + jsonRequest);
//
//        Map<String, RemoteRequest> remoteUnits = jobsData.getRemoteRequestUnits();
//
//        RemoteRequest centralUnit = remoteUnits.get(NamedConstants.CENTRAL_UNIT_REQUEST);
//
//        String response = clientPool.sendRemoteRequest(jsonRequest, centralUnit);
//
//        logger.info("Response from Central Server: " + response);
//
//    }
//
//    @Override
//    public void interrupt() throws UnableToInterruptJobException {
//        logger.warn("Failed to interrupt a Job before deleting it..");
//    }
//
//}
