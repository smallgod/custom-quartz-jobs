package com.library.jobsandlisteners.job;

import com.library.httpconnmanager.HttpClientPool;
import com.library.customexception.MyCustomException;
import com.library.datamodel.Constants.AdPaymentStatus;
import com.library.datamodel.Constants.EntityName;
import com.library.datamodel.Constants.NamedConstants;
import com.library.datamodel.Constants.TransactionAggregatorStatus;
import com.library.datamodel.Json.DBSaveResponse;
import com.library.datamodel.model.v1_0.AdPaymentDetails;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Job class to fetch payments and send them to aggregator/mobile money operator
 * for processing
 *
 *
 * @author smallgod
 */
public class AdPaymentJob implements Job, InterruptableJob, ExecutableJob {

    private static final LoggerUtil logger = new LoggerUtil(AdPaymentJob.class);

    @Override
    public void execute(JobExecutionContext jec) throws JobExecutionException {

        //synchronized (NamedConstants.AD_PAYMENT_MUTEX) {
        JobDataMap jobsDataMap = jec.getMergedJobDataMap();
        HttpClientPool clientPool = (HttpClientPool) jobsDataMap.get(NamedConstants.CLIENT_POOL);
        DatabaseAdapter databaseAdapter = (DatabaseAdapter) jobsDataMap.get(NamedConstants.DB_ADAPTER);
        DebitClient debitAccount = new DebitClient(clientPool);

        try {

            //true if lock acquired, false if time expired with no lock acquired
            boolean hasAcquiredLock = NamedConstants.FETCH_PAYMENTS_LOCK.tryLock(30, TimeUnit.SECONDS);

            if (hasAcquiredLock) {

                Boolean triggerNow = Boolean.FALSE;
                Object triggerNowObj = jobsDataMap.get(NamedConstants.TRIGGER_NOW_PAYPROCESSOR);

                if (null != triggerNowObj) {
                    triggerNow = (Boolean) triggerNowObj;
                }

                //TriggerNow requests from campaignProcesor
                if (triggerNow) {

                    AdPaymentDetails paymentDetails = (AdPaymentDetails) jobsDataMap.get(NamedConstants.PAYMENTS_DETAILS);
                    makePayment(debitAccount, paymentDetails, databaseAdapter);

                } else {

                    Set<AdPaymentStatus> payStatus = new HashSet<>();
                    payStatus.add(AdPaymentStatus.PAY_NEW);

                    Map<String, Object> fetchProps = new HashMap<>();
                    fetchProps.put("paymentStatus", payStatus);

                    Set<AdPaymentDetails> payments = databaseAdapter.fetchBulk(EntityName.AD_PAYMENT, fetchProps, NamedConstants.ALL_COLUMNS);

                    if (!(null == payments || payments.isEmpty())) {

                        for (AdPaymentDetails newPayment : payments) {
                            makePayment(debitAccount, newPayment, databaseAdapter);
                        }
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

            logger.error("An Error occurred in AdPamentJob: " + ex.getMessage());
            ex.printStackTrace();

        } finally {

            logger.debug("Releasing fetch-payments lock!");
            NamedConstants.FETCH_PAYMENTS_LOCK.unlock();
        }

        //}
    }

    /**
     * Make a payment and update the status in the DB
     *
     * @param debitAccount
     * @param payment
     * @param databaseAdapter
     * @return True if the payment is being processed
     * @throws MyCustomException
     */
    private boolean makePayment(DebitClient debitAccount, AdPaymentDetails payment, DatabaseAdapter databaseAdapter) throws MyCustomException {

        boolean isPaymentInitiated = Boolean.FALSE;
        MoMoPaymentMamboPay request = new MoMoPaymentMamboPay();
        request.setAccountToDebit(request.new AccountToDebit(NamedConstants.MAMBOPAY_PARAM_MSISDN, payment.getPayerAccount()));
        request.setAmountToDebit(request.new AmountToDebit(NamedConstants.MAMBOPAY_PARAM_AMOUNT, payment.getAmount()));
        request.setTransactionId(request.new TransactionId(NamedConstants.MAMBOPAY_PARAM_TRANSID, payment.getInternalPaymentID()));

        MamboPayPaymentResponse response = debitAccount.debitClientViaMamboPay(request, NamedConstants.MAMBOPAY_PARAM_CALLBACKURL, NamedConstants.ADVERTXPO_CALLBACK_URL, NamedConstants.MAMBOPAY_DEBIT_URL, NamedConstants.MAMBOPAY_HEADER_SUBSCKEY, NamedConstants.SUBSCRIPTION_KEY);
        String responseMessage;
        if (response != null) {

            String reference = response.getMamboPayReference();
            TransactionAggregatorStatus status = TransactionAggregatorStatus.convertToEnum(response.getStatus());
            responseMessage = response.getStatusDescription();

            switch (status) {

                case PROCESSING:
                    payment.setPaymentStatus(AdPaymentStatus.PAY_INITIATED);
                    payment.setAggregatorPaymentID(reference);
                    isPaymentInitiated = Boolean.TRUE;
                    break;

                case FAILED:
                    payment.setPaymentStatus(AdPaymentStatus.PAY_FAILED);
                    logger.warn(responseMessage);
                    break;

                case DUPLICATE:
                    //pass
                    logger.debug("Duplicate Payment to Aggregator, ignoring aggregator response: " + responseMessage);
                    break;

                case UNKNOWN:
                    //figure out what to do with unknown
                    logger.warn("Aggregator Payment status unknown: " + responseMessage);
                    break;

                default:
                    //unknown
                    logger.warn("Default Payment status from aggregator: " + responseMessage);
                    break;
            }

        } else {
            responseMessage = "No response from MamboPay Server";
            logger.warn(responseMessage);
        }

        payment.setStatusDescription(responseMessage);
        DBSaveResponse dbResonse = databaseAdapter.saveOrUpdateEntity(payment, Boolean.FALSE);

        return isPaymentInitiated;
    }

    @Override
    public void interrupt() throws UnableToInterruptJobException {
        logger.warn("Failed to interrupt a Job before deleting it..");
    }

}
