/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.library.jobsandlisteners.job;

import com.library.httpconnmanager.HttpClientPool;
import com.library.customexception.MyCustomException;
import com.library.datamodel.Constants.AdPaymentStatus;
import com.library.datamodel.Constants.CampaignStatus;
import com.library.datamodel.Constants.EntityName;
import com.library.datamodel.Constants.NamedConstants;
import com.library.datamodel.Constants.TransactionAggregatorStatus;
import com.library.datamodel.model.v1_0.AdPaymentDetails;
import com.library.datamodel.model.v1_0.AdProgram;
import com.library.datamodel.model.v1_0.MamboPayPaymentResponse;
import com.library.datamodel.model.v1_0.MoMoPaymentMamboPay;
import com.library.dbadapter.DatabaseAdapter;
import com.library.sgsharedinterface.ExecutableJob;
import org.quartz.InterruptableJob;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.UnableToInterruptJobException;
import com.library.sglogger.util.LoggerUtil;
import com.library.sgmtnmoneyug.DebitClient;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.openide.util.Exceptions;

/**
 * The class doing the work
 *
 * @author smallgod
 */
public class AdPaymentJob implements Job, InterruptableJob, ExecutableJob {

    private static final LoggerUtil logger = new LoggerUtil(AdPaymentJob.class);

    //this method is for testing purpose only, delete after
    @Override
    public void execute(JobExecutionContext jec) throws JobExecutionException {

        logger.debug("Current thread here is  : " + Thread.currentThread().getName());
        logger.debug("Current thread in state : " + Thread.currentThread().getState().name());

        synchronized (NamedConstants.AD_PAYMENT_MUTEX) {

            try {

                JobDataMap jobsDataMap = jec.getMergedJobDataMap();
                HttpClientPool clientPool = (HttpClientPool) jobsDataMap.get(NamedConstants.CLIENT_POOL);
                DatabaseAdapter databaseAdapter = (DatabaseAdapter) jobsDataMap.get(NamedConstants.DB_ADAPTER);

                Set<String> columnsToFetch = new HashSet<>();
                columnsToFetch.add("ALL");

                Set<AdPaymentDetails> updatedPayments = new HashSet<>();

                try {

                    if (NamedConstants.FETCH_PAYMENTS_LOCK.tryLock(30, TimeUnit.SECONDS)) {

                        logger.debug("Thread, " + Thread.currentThread().getName() + ", Inside pending payments lock, about to fetch!");

                        Map<String, Object> pendingPaymentAds = new HashMap<>();

                        Set<AdPaymentStatus> payStatus = new HashSet<>();
                        payStatus.add(AdPaymentStatus.PAY_NEW);

                        pendingPaymentAds.put("paymentStatus", payStatus);  //whether or not we need to fetch this payment

                        Set<AdPaymentDetails> newPayments = databaseAdapter.fetchBulk(EntityName.AD_PAYMENT, pendingPaymentAds, columnsToFetch);

                        if (null == newPayments || newPayments.isEmpty()) {

                            logger.info("No New payments where fetched from the database");
                            return;

                        } else {

                            for (AdPaymentDetails newPayment : newPayments) {
                                //dont fetch it again
                                newPayment.setPaymentStatus(AdPaymentStatus.PAY_INITIATED);
                                updatedPayments.add(newPayment);

                                //we might need to move the lower for loop here
                                //2. Alert campaignProcessor to move to next step
        
                            }
                            databaseAdapter.SaveOrUpdateBulk(EntityName.AD_PAYMENT, updatedPayments, Boolean.FALSE);
                        }

                    } else {
                        logger.debug("Tried to wait, but the Lock is still held by some other thread, return now!");
                        return;
                    }

                } catch (InterruptedException e) {

                    logger.error("Interrrupted exception: " + e.getMessage());
                    e.printStackTrace();

                } finally {
                    //release lock
                    logger.debug("Releasing lock!");
                    NamedConstants.FETCH_PAYMENTS_LOCK.unlock();
                }

                DebitClient debitAccount = new DebitClient(clientPool);
                //Set<AdPaymentDetails> paymentsToUpdate = new HashSet<>();
                Set<AdProgram> programsToUpdate = new HashSet<>();

                for (AdPaymentDetails payment : updatedPayments) {

                    MoMoPaymentMamboPay request = new MoMoPaymentMamboPay();
                    request.setAccountToDebit(request.new AccountToDebit(NamedConstants.MAMBOPAY_PARAM_MSISDN, payment.getPayerAccount()));
                    request.setAmountToDebit(request.new AmountToDebit(NamedConstants.MAMBOPAY_PARAM_AMOUNT, payment.getAmount()));
                    request.setTransactionId(request.new TransactionId(NamedConstants.MAMBOPAY_PARAM_TRANSID, payment.getInternalPaymentID()));

                    MamboPayPaymentResponse response = debitAccount.debitClientViaMamboPay(request, NamedConstants.MAMBOPAY_PARAM_CALLBACKURL, NamedConstants.ADVERTXPO_CALLBACK_URL, NamedConstants.MAMBOPAY_DEBIT_URL, NamedConstants.MAMBOPAY_HEADER_SUBSCKEY, NamedConstants.SUBSCRIPTION_KEY);

                    if (response != null) {

                        String reference = response.getMamboPayReference();
                        TransactionAggregatorStatus status = TransactionAggregatorStatus.convertToEnum(response.getStatus());
                        String message = response.getStatusDescription();

                        Map<String, Object> resourceProps = new HashMap<>();
                        resourceProps.put("internalPaymentID", new HashSet<>(Arrays.asList(payment.getInternalPaymentID())));

                        Set<AdProgram> adPrograms = databaseAdapter.fetchEntitiesByNamedQuery(EntityName.AD_PROGRAM, AdProgram.FETCH_CAMPAIGNS_BY_PAYMENT_ID, resourceProps);
                        AdProgram adProgram = (AdProgram) adPrograms.toArray()[0];

                        switch (status) {

                            case PROCESSING:
                                //already marked INITIATED;
                                adProgram.setAdCampaignStatus(CampaignStatus.PENDING_PAYMENT);
                                adProgram.setDescription("Payment initiated: " + message);

                                break;

                            case FAILED:
                                payment.setPaymentStatus(AdPaymentStatus.PAY_FAILED);
                                adProgram.setAdCampaignStatus(CampaignStatus.REJECTED);
                                adProgram.setDescription(message);
                                logger.warn(message);
                                break;

                            case DUPLICATE:
                                //pass
                                logger.debug("Duplicate Payment to Aggregator, ignoring aggregator response: " + message);
                                break;

                            case UNKNOWN:
                                //figure out what to do with unknown
                                logger.warn("Aggregator Payment status unknown: " + message);
                                break;

                            default:
                                //unknown
                                logger.warn("Default Payment status from aggregator: " + message);
                                break;
                        }

                        //we could just wait for all payments/progs and update at once outside of Loop
                        payment.setAggregatorPaymentID(reference);
                        payment.setStatusDescription(message);

                        adProgram.setAdPaymentDetails(payment);
                        programsToUpdate.add(adProgram);
                        //cascade update
                        databaseAdapter.SaveOrUpdateBulk(EntityName.AD_PROGRAM, programsToUpdate, Boolean.FALSE);

                    }
                }

            } catch (MyCustomException ex) {
                Exceptions.printStackTrace(ex);
            } catch (Exception ex) {
                logger.error("An Error occurred in AdPamentJob: " + ex.getMessage());
                ex.printStackTrace();
            }

        }

    }

    @Override
    public void interrupt() throws UnableToInterruptJobException {
        logger.warn("Failed to interrupt a Job before deleting it..");
    }

}
