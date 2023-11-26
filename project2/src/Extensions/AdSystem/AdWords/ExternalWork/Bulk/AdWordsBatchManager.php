<?php

namespace App\Extensions\AdSystem\AdWords\ExternalWork\Bulk;

use App\Entity\{AdwordsAccount, AdwordsBatchJob};
use Airbrake\Notifier;
use App\Extensions\AdSystem\AdWords\ExternalWork\AdWordsErrorDetail;
use App\Extensions\AdSystem\AdWords\ExternalWork\Auth\AdWordsServiceManager;
use App\Extensions\Common\{ContentType, AdSystemEnum};
use App\Extensions\Common\ExternalWork\Bulk\{BatchJob as BatchJobAlias, BatchManager};
use App\Interfaces\EntityInterface\{BatchJobInterface, SystemAccountInterface};
use App\Providers\ProviderDocumentName;
use Google\Ads\GoogleAds\Util\V13\ResourceNames;
use Google\Ads\GoogleAds\V13\Enums\BatchJobStatusEnum\BatchJobStatus;
use Google\Ads\GoogleAds\V13\Resources\BatchJob;
use Google\Ads\GoogleAds\V13\Services\{BatchJobOperation, BatchJobResult, BatchJobServiceClient,
    GoogleAdsRow, MutateOperation};
use Google\ApiCore\{ApiException, OperationResponse, ValidationException};
use Doctrine\ODM\MongoDB\DocumentManager;
use Doctrine\ORM\{Exception\ORMException, OptimisticLockException, EntityManagerInterface};
use Psr\Container\ContainerInterface;
use Psr\Log\LoggerInterface;
use Symfony\Component\Cache\Adapter\AdapterInterface;

/**
 * Class AdWordsBatchManager
 *
 * @package App\Extensions\AdSystem\AdWords\ExternalWork\Bulk
 */
abstract class AdWordsBatchManager extends BatchManager
{
    private const POLL_FREQUENCY_SECONDS = 1;

    private const MAX_TOTAL_POLL_INTERVAL_SECONDS = 60;

    private const LIMIT_MUTATE_OPERATIONS = 1000;

    /**
     * @var ContainerInterface
     */
    protected ContainerInterface $container;

    /**
     * @var string
     */
    private string $env;

    /**
     * @var AdwordsServiceManager
     */
    private AdWordsServiceManager $serviceManager;

    /**
     * @var LoggerInterface
     */
    public LoggerInterface $logger;

    /**
     * @var AdapterInterface
     */
    public AdapterInterface $cache;

    /**
     * AdWordsBatchManager constructor.
     * @param ContainerInterface     $container
     * @param AdWordsServiceManager  $serviceManager
     * @param EntityManagerInterface $em
     * @param DocumentManager        $dm
     * @param LoggerInterface        $adwordsLogger
     * @param AdapterInterface       $cache
     */
    public function __construct(
        ContainerInterface      $container,
        AdWordsServiceManager   $serviceManager,
        EntityManagerInterface  $em,
        DocumentManager         $dm,
        LoggerInterface         $adwordsLogger,
        AdapterInterface        $cache
    ) {
        parent::__construct(AdSystemEnum::ADWORDS, $em, $dm, $serviceManager->getProjectDir());

        $this->container        = $container;
        $this->serviceManager   = $serviceManager;
        $this->logger           = $adwordsLogger;
        $this->cache            = $cache;
        $this->env              = $serviceManager->getEnv();
    }

    /**
     * @return string
     */
    protected function getEnv(): string
    {
        return $this->env;
    }

    /**
     * @return AdapterInterface
     */
    protected function getCache(): AdapterInterface
    {
        return $this->cache;
    }

    /**
     * @return LoggerInterface
     */
    protected function getLogger(): LoggerInterface
    {
        return $this->logger;
    }

    /**
     * @return AdWordsServiceManager
     */
    protected function getGoogleServiceManager(): AdWordsServiceManager
    {
        return $this->serviceManager;
    }

    /**
     * @return Notifier
     */
    protected function getAirbrakeNotifier(): Notifier
    {
        return $this->container->get('ami_airbrake.notifier');
    }

    /**
     * @return BatchJobServiceClient
     */
    protected function getBatchJobService(): BatchJobServiceClient
    {
        return $this->getGoogleServiceManager()->getBatchJobServiceClient();
    }

    /**
     * @param int $customerId Required. The client customer id.
     * @param int $batchJobId Required. The batch job id.
     *
     * @return bool|BatchJob
     * @throws ValidationException
     */
    private function getBulkUploadStatus(int $customerId, int $batchJobId): ?BatchJob
    {
        $query = sprintf( /** @lang text */
            'SELECT batch_job.id, batch_job.status, batch_job.metadata.operation_count, 
            batch_job.metadata.executed_operation_count 
            FROM batch_job 
            WHERE batch_job.id = %s',
            $batchJobId
        );

        try {
            // Issues a search request.
            $googleAdsServiceClient = $this->getGoogleServiceManager()->getGoogleAdsServiceClient();
            $response = $googleAdsServiceClient->search($customerId, $query);

            // Only one location can be set for a company
            if ($response->getPage()->getPageElementCount() > 0) {
                // Iterates over all rows in all pages and prints the requested field values for
                // the campaign criterion in each row.
                foreach ($response->iterateAllElements() as $googleAdsRow) {
                    /** @var GoogleAdsRow $googleAdsRow */
                    return $googleAdsRow->getBatchJob();
                }
            }

        } catch (ApiException $apiException) {
            foreach ($apiException->getMetadata() as $metadatum) {
                foreach ($metadatum['errors'] as $error) {
                    if (!strpos(AdWordsErrorDetail::errorDetail($error['message']), "internal error")) {
                        $this->getAirbrakeNotifier()->notify(new \Exception(
                            '[Google] ApiException was thrown with message - ' . $error['message']
                            . ' when the process was running "getBulkUploadStatus()"' . PHP_EOL
                        ));
                    }
                }
            }
        }

        return false;
    }

    /**
     * @param MutateOperation[]      $operations
     * @param array                  $entitiesIds
     * @param SystemAccountInterface $account
     * @param string                 $action
     *
     * @return void
     * @throws OptimisticLockException | ValidationException | \Exception
     */
    protected function uploadOperations(
        array                   $operations,
        array                   $entitiesIds,
        SystemAccountInterface  $account,
        string                  $action
    ) {
        $batchJobServiceClient = $this->getBatchJobService();

        $this->getLogger()->info("Creating new {$this->getOperandType()}:{$action} batch job in ". AdSystemEnum::ADWORDS);
        $batchJobResourceName = $this->createBatchJob($batchJobServiceClient, $account);
        if (is_null($batchJobResourceName)) {
            return;
        }

        $this->getLogger()->info("Creating new {$this->getOperandType()}:{$action} batch job locally");
        $hotsBatchJob = $this->createHotsBatchJob($account, $batchJobResourceName, $entitiesIds, $action);

        try {
            $this->addAllBatchJobOperations($batchJobServiceClient, $batchJobResourceName, $operations);
            $operationResponse = $batchJobServiceClient->runBatchJob($batchJobResourceName);
            $this->pollBatchJob($operationResponse);

            $this->getLogger()->info(sprintf ("Uploaded %d operations for batch job with ID %d.". PHP_EOL,
                count($operations),
                BatchJobServiceClient::parseName($batchJobResourceName)
            ),
                [$hotsBatchJob->getOperandType(), $hotsBatchJob->getAction()]
            );

        } catch (ApiException $apiException) {
            foreach ($apiException->getMetadata() as $metadatum) {
                foreach ($metadatum['errors'] as $error) {
                    if (!strpos(AdWordsErrorDetail::errorDetail($error['message']), "internal error")) {
                        $this->getAirbrakeNotifier()->notify(new \Exception(
                            '[Google] ApiException was thrown with message - ' . $error['message']
                            . '. When the process was running "uploadOperations()"' . PHP_EOL
                        ));
                    }
                }
            }

            $hotsBatchJob->setStatus(BatchJobAlias::STATUS_ERROR);
            $this->getEntityManager()->flush();
        }
    }

    /**
     * @param BatchJobServiceClient  $batchJobServiceClient
     * @param SystemAccountInterface $account
     *
     * @return string | null
     * @throws \Exception
     */
    protected function createBatchJob(
        BatchJobServiceClient   $batchJobServiceClient,
        SystemAccountInterface  $account
    ):  ?string
    {
        // Creates a batch job operation to create a new batch job.
        $batchJobOperation = new BatchJobOperation();
        $batchJobOperation->setCreate(new BatchJob());

        try {
            // Issues a request to the API and get the batch job's resource name.
            return $batchJobServiceClient->mutateBatchJob($account->getSystemAccountId(), $batchJobOperation)
                ->getResult()
                ->getResourceName();

        } catch (ApiException $apiException) {
            foreach ($apiException->getMetadata() as $metadatum) {
                foreach ($metadatum['errors'] as $error) {
                    $where = ['systemAccount' => $account->getId()];
                    $attributes = ['error' => $error['message']];

                    $dm = $this->getDocumentManager();
                    foreach (ContentType::CONTENT_TYPES as $CONTENT_TYPE) {
                        $documentName = ProviderDocumentName::getQueueByContentType($CONTENT_TYPE);
                        $dm->getRepository($documentName)->updateManyByAttributes($dm, $where, $attributes);
                    }

                    if (!strpos(AdWordsErrorDetail::errorDetail($error['message']), "internal error")) {
                        $this->getAirbrakeNotifier()->notify(new \Exception(
                            '[Google] ApiException was thrown with message - ' . $error['message']
                            . '. When the process was running "createBatchJob()", batch job can\'t create.' . PHP_EOL
                        ));
                    }
                }
            }

            return null;
        }
    }

    /**
     * @param AdwordsAccount         $account
     * @param string                 $batchJobResourceName
     * @param array                  $entitiesIds
     * @param string                 $action
     *
     * @return AdwordsBatchJob
     * @throws ORMException | ValidationException | OptimisticLockException
     */
    protected function createHotsBatchJob(
        SystemAccountInterface  $account,
        string                  $batchJobResourceName,
        array                   $entitiesIds,
        string                  $action
    ):  AdwordsBatchJob
    {
        $hotsBatchJob = new AdwordsBatchJob();
        $hotsBatchJob->setAction($action);
        $hotsBatchJob->setOperandType($this->getOperandType());
        $hotsBatchJob->setStatus(BatchJobAlias::STATUS_PENDING_RESULT);
        $hotsBatchJob->setSystemAccount($account);
        $hotsBatchJob->setSystemJobId((int)BatchJobServiceClient::parseName($batchJobResourceName)['batch_job_id']);
        $hotsBatchJob->setMetaData(json_encode(['ids' => $entitiesIds]));
        $hotsBatchJob->setExecuteAt(new \DateTime("+ 30 second"));

        $em = $this->getEntityManager();
        $em->persist($hotsBatchJob);
        $em->flush();

        return $hotsBatchJob;
    }

    /**
     * Adds all batch job operations to the batch job. As this is the first time for this
     * batch job, pass null as a sequence token. The response will contain the next sequence token
     * that you can use to upload more operations in the future.
     *
     * @param BatchJobServiceClient  $batchJobServiceClient The batch job service client
     * @param string                 $batchJobResourceName  Required. The resource name of the batch job.
     * @param MutateOperation[]      $mutateOperations      Required. The list of mutates being added.
     *
     * @throws ApiException | \Exception
     */
    private function addAllBatchJobOperations(
        BatchJobServiceClient   $batchJobServiceClient,
        string                  $batchJobResourceName,
        array                   $mutateOperations
    ) {

        $mutateOperations = array_chunk($mutateOperations, self::LIMIT_MUTATE_OPERATIONS);

        $response = $batchJobServiceClient->addBatchJobOperations(
            $batchJobResourceName,
            $mutateOperations[0]
        );

        unset($mutateOperations[0]);

        if (!empty($mutateOperations)) {
            foreach ($mutateOperations as $operations) {
                $response = $batchJobServiceClient->addBatchJobOperations(
                    $batchJobResourceName,
                    $operations,
                    ['sequenceToken' => $response->getNextSequenceToken()]
                );
            }
        }
    }

    /**
     * Polls the server until the batch job execution finishes by setting the initial poll
     * delay time and the total time to wait before time-out.
     *
     * @param OperationResponse $operationResponse the operation response used to poll the server
     *
     * @throws ApiException | ValidationException
     */
    private function pollBatchJob(OperationResponse $operationResponse)
    {
        $operationResponse->pollUntilComplete([
            'initialPollDelayMillis' => self::POLL_FREQUENCY_SECONDS * 1000,
            'totalPollTimeoutMillis' => self::MAX_TOTAL_POLL_INTERVAL_SECONDS * 1000
        ]);
    }

    /**
     * @param BatchJobInterface $hotsBatchJob
     */
    public function checkBatchJobResult(BatchJobInterface $hotsBatchJob)
    {
        $em = $this->getEntityManager();

        try {
            if ($hotsBatchJob->getStatus() != BatchJobAlias::STATUS_PENDING_RESULT && $hotsBatchJob->getAttempts() > 7) {
                throw new \Exception("Unexpected status {$hotsBatchJob->getStatus()} in job {$hotsBatchJob->getId()}");
            }

            $batchJobResults = $this->fetchBatchJobResults($hotsBatchJob);

            if (!$batchJobResults) {
                return;
            } else {
                isset($batchJobResults['errors'])  ?: $batchJobResults['errors']  = [];
                isset($batchJobResults['results']) ?: $batchJobResults['results'] = [];
            }

            $hotsEntitiesIds = json_decode($hotsBatchJob->getMetaData(), true)['ids'];

            if (((count($batchJobResults['results']) + count($batchJobResults['errors'])) != count($hotsEntitiesIds))
                && ($hotsBatchJob->getAction() == BatchJobAlias::ACTION_ADD)
                && ($hotsBatchJob->getOperandType() != BatchJobAlias::OPERAND_TYPE_EXTENSION)
            ) {
                $this->getLogger()->error("{$this->getOperandType()}s operations count is wrong in response", [
                    'expected' => count($hotsEntitiesIds),
                    'actual_results' => count($batchJobResults['results']),
                    'actual_errors' => count($batchJobResults['errors']),
                ]);

                /**
                 * Sometimes Google returns different number of operations in batch job results with
                 * messed indexes of operations
                 */
                if (!$this->failedResultProcessingFallback($hotsBatchJob, $batchJobResults)) {
                    $hotsBatchJob->setStatus(BatchJobAlias::STATUS_ERROR);
                    $em->persist($hotsBatchJob);
                    $em->flush();

                    return;
                }
            }

            $this->processResults($batchJobResults, $hotsBatchJob);

            $hotsBatchJob->setStatus(BatchJobAlias::STATUS_COMPLETE);
            $em->persist($hotsBatchJob);
            $em->flush();

            $this->getLogger()->info("Total {$this->getOperandType()}s processing stats", ['success' => count($batchJobResults['results']),
                'errors' => count($batchJobResults['errors']), $hotsBatchJob->getId(), $hotsBatchJob->getSystemJobId()]);
            $this->getLogger()->info("Memory usage: " . (memory_get_peak_usage(true) / 1024 / 1024),
                [$hotsBatchJob->getId(), $hotsBatchJob->getSystemJobId()]);

        } catch (ApiException $apiException) {
            foreach ($apiException->getMetadata() as $metadatum) {
                foreach ($metadatum['errors'] as $error) {
                    if (!strpos(AdWordsErrorDetail::errorDetail($error['message']), "internal error")) {
                        $this->getAirbrakeNotifier()->notify(new \Exception(
                            '[Google] ApiException was thrown with message - ' . $error['message']
                            . ' (' . $error['trigger']['stringValue'] . ')' . ' when the process was running '
                            . '"checkBatchJobResult()", batch job can\'t create.' . PHP_EOL
                        ));
                    }
                }
            }

        } catch (\Exception $fault) {
            $this->getAirbrakeNotifier()->notify(new \Exception(
                '[Google] Exception was thrown with message - ' . $fault->getMessage()
                    . ' when the process was running "checkBatchJobResult", batch job can\'t create.'. PHP_EOL
            ));
        }
    }

    /**
     * @param BatchJobInterface $hotsBatchJob
     *
     * @return array | array[] | null
     * @throws ApiException | ValidationException
     */
    protected function fetchBatchJobResults(BatchJobInterface $hotsBatchJob): ?array
    {
        /** @var EntityManagerInterface $em */
        $em = $this->getEntityManager();

        $hotsBatchJob->setAttempts($hotsBatchJob->getAttempts() + 1);
        $sleepSeconds = 15 * pow(2, $hotsBatchJob->getAttempts());
        $hotsBatchJob->setExecuteAt(new \DateTime("+ $sleepSeconds second"));

        if ($hotsBatchJob->getAttempts() > 7) {
            $this->getLogger()->error("Batch job is too long in progress status", [$hotsBatchJob->getOperandType(),
                $hotsBatchJob->getAction(), $hotsBatchJob->getId(), $hotsBatchJob->getSystemJobId(),
                $hotsBatchJob->getAttempts()]);
        }

        $batchJobResult = $this->getBulkUploadStatus(
            $hotsBatchJob->getSystemAccount()->getSystemAccountId(),
            $hotsBatchJob->getSystemJobId()
        );

        if (!$batchJobResult) {
            return null;
        }

        if ($batchJobResult->getStatus() === BatchJobStatus::PENDING) {
            $this->getLogger()->info("The batch job has been accepted by the batch processor, but the related".
                "batch job actions is not currently running. ", [
                $hotsBatchJob->getOperandType(), $hotsBatchJob->getAction(), $hotsBatchJob->getId(),
                $hotsBatchJob->getSystemJobId()]);

            $hotsBatchJob->setStatus(BatchJobAlias::STATUS_PENDING_CANCELLATION);
            $em->persist($hotsBatchJob);
            $em->flush();

            $this->getLogger()->error("The batch job ID: {$hotsBatchJob->getSystemJobId()}, is not currently running.");

            return null;
        } elseif ($batchJobResult->getStatus() === BatchJobStatus::RUNNING) {
            $this->getLogger()->info("Batch job ID is still in progress", [$hotsBatchJob->getOperandType(),
                $hotsBatchJob->getAction(), $hotsBatchJob->getId(), $hotsBatchJob->getSystemJobId()]);

            if ($hotsBatchJob->getAttempts() > 50) {
                $hotsBatchJob->setStatus(BatchJobAlias::STATUS_ERROR);
                $em->persist($hotsBatchJob);

                $this->getAirbrakeNotifier()->notify(new \Exception(
                    '[Google] Batch job ID "' . $hotsBatchJob->getSystemJobId() . '" is still in progress - '
                    . $hotsBatchJob->getAttempts() . ', Batch job status - ' . BatchJobAlias::STATUS_ERROR . PHP_EOL
                ));
            }

            $em->flush();

            return null;
        }

        $hotsBatchJob->setExecuteAt(new \DateTime("+ 60 second"));

        /** Batch status FAILED */
        if (in_array($batchJobResult->getStatus(), [BatchJobStatus::UNKNOWN, BatchJobStatus::UNSPECIFIED])) {
            $this->getLogger()->error("Canceled batch job " . var_export($batchJobResult, true),
                [$hotsBatchJob->getId(), $hotsBatchJob->getSystemJobId()]);

            $hotsBatchJob->setStatus(BatchJobAlias::STATUS_ERROR);
            $em->persist($hotsBatchJob);
            $em->flush();

            return null;
        }

        if ($batchJobResult->getStatus() === BatchJobStatus::DONE) {
            if ($batchJobResult->getMetadata()->getOperationCount()
                !== $batchJobResult->getMetadata()->getExecutedOperationCount()) {
                $this->getLogger()->info(printf(
                    "The number of mutate operations executed by the batch job. (%d/%d)",
                    $batchJobResult->getMetadata()->getExecutedOperationCount(),
                    $batchJobResult->getMetadata()->getOperationCount()
                ));
            }

            return $this->fetchBatchJobResponse(
                $hotsBatchJob->getSystemAccount()->getSystemAccountId(),
                $hotsBatchJob->getSystemJobId()
            );
        }

        return null;
    }

    /**
     * @param string    $customerId  Required. The client customer id.
     * @param int       $batchJobId  Required. The batch job id.
     *
     * @throws ApiException | ValidationException
     */
    protected function fetchBatchJobResponse(
        string                  $customerId,
        int                     $batchJobId
    ):  array
    {
        $batchJobResourceName = ResourceNames::forBatchJob($customerId, $batchJobId);

        $batchJobServiceClient = $this->getGoogleServiceManager()->getBatchJobServiceClient();

        /** Gets all the results from running batch job and print their information. */
        $listBatchJobResults = $batchJobServiceClient->listBatchJobResults(
            $batchJobResourceName,
            ['pageSize' => $this->getGoogleServiceManager()::PAGE_SIZE]
        );

        $batchJobResults = [
            'results' => [],
            'errors'  => []
        ];
        foreach ($listBatchJobResults->iterateAllElements() as $batchJobResult) {
            /** @var BatchJobResult $batchJobResult */
            if (!$batchJobResult->getMutateOperationResponse()) {
                if (!$batchJobResult->getStatus()) {
                    continue;
                } else {
                    $batchJobResults['errors'][$batchJobResult->getOperationIndex()]
                        =  strstr($batchJobResult->getStatus()->getMessage(), ',', true);
                }
            } else {
                $batchJobResults['results'][$batchJobResult->getOperationIndex()]
                    = $batchJobResult->getMutateOperationResponse();
            }
        }

        return $batchJobResults;
    }
}
