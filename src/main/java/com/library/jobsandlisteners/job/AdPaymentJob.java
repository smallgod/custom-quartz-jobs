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
import com.library.datamodel.Json.DBSaveResponse;
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

                DebitClient debitAccount = new DebitClient(clientPool);

                try {

                    if (NamedConstants.FETCH_PAYMENTS_LOCK.tryLock(30, TimeUnit.SECONDS)) {

                        Boolean triggerNow = Boolean.FALSE;
                        Object triggerNowObj = jobsDataMap.get(NamedConstants.TRIGGER_NOW);
                        if (null != triggerNowObj) {
                            triggerNow = (Boolean) triggerNowObj;
                        }

                        //react to trigger-now requests
                        if (triggerNow) {

                            AdPaymentDetails paymentDetails = (AdPaymentDetails) jobsDataMap.get(NamedConstants.PAYMENTS_DATA);
                            makePayment(debitAccount, paymentDetails, databaseAdapter);
                            logger.debug("Releasing lock!");
                            NamedConstants.FETCH_PAYMENTS_LOCK.unlock();
                            return;

                        }

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
                                makePayment(debitAccount, newPayment, databaseAdapter);
                            }
                            //databaseAdapter.SaveOrUpdateBulk(EntityName.AD_PAYMENT, updatedPayments, Boolean.FALSE);
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

            } catch (MyCustomException ex) {
                Exceptions.printStackTrace(ex);
            } catch (Exception ex) {
                logger.error("An Error occurred in AdPamentJob: " + ex.getMessage());
                ex.printStackTrace();
            }

        }

    }

    /**
     * Make a payment and update the status in the DB
     *
     * @param debitAccount
     * @param payment
     * @param databaseAdapter
     * @return
     * @throws MyCustomException
     */
    boolean makePayment(DebitClient debitAccount, AdPaymentDetails payment, DatabaseAdapter databaseAdapter) throws MyCustomException {

        boolean isUpdated = Boolean.FALSE;
        MoMoPaymentMamboPay request = new MoMoPaymentMamboPay();
        request.setAccountToDebit(request.new AccountToDebit(NamedConstants.MAMBOPAY_PARAM_MSISDN, payment.getPayerAccount()));
        request.setAmountToDebit(request.new AmountToDebit(NamedConstants.MAMBOPAY_PARAM_AMOUNT, payment.getAmount()));
        request.setTransactionId(request.new TransactionId(NamedConstants.MAMBOPAY_PARAM_TRANSID, payment.getInternalPaymentID()));

        MamboPayPaymentResponse response = debitAccount.debitClientViaMamboPay(request, NamedConstants.MAMBOPAY_PARAM_CALLBACKURL, NamedConstants.ADVERTXPO_CALLBACK_URL, NamedConstants.MAMBOPAY_DEBIT_URL, NamedConstants.MAMBOPAY_HEADER_SUBSCKEY, NamedConstants.SUBSCRIPTION_KEY);

        if (response != null) {

            String reference = response.getMamboPayReference();
            TransactionAggregatorStatus status = TransactionAggregatorStatus.convertToEnum(response.getStatus());
            String message = response.getStatusDescription();

            switch (status) {

                case PROCESSING:
                    //already marked INITIATED;
                    payment.setPaymentStatus(AdPaymentStatus.PAY_INITIATED);

                    break;

                case FAILED:
                    payment.setPaymentStatus(AdPaymentStatus.PAY_FAILED);
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

            DBSaveResponse dbResonse = databaseAdapter.saveOrUpdateEntity(payment, Boolean.FALSE);
            if (null != dbResonse) {
                isUpdated = dbResonse.getSuccess();
            }
        }
        return isUpdated;
    }

    @Override
    public void interrupt() throws UnableToInterruptJobException {
        logger.warn("Failed to interrupt a Job before deleting it..");
    }

}
