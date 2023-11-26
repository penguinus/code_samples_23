<?php

namespace App\Extensions\AdSystem\Bing\ExternalWork\Bulk;

use Airbrake\Notifier;
use App\Entity\BingBatchJob;
use App\Extensions\AdSystem\Bing\ExternalWork\Auth\BingServiceManager;
use App\Extensions\Common\AdSystemEnum;
use App\Extensions\Common\ExternalWork\Bulk\BatchJob;
use App\Extensions\Common\ExternalWork\Bulk\BatchManager;
use App\Interfaces\EntityInterface\BatchJobInterface;
use App\Interfaces\EntityInterface\SystemAccountInterface;
use Doctrine\ODM\MongoDB\DocumentManager;
use Doctrine\ORM\EntityManagerInterface;
use Doctrine\ORM\OptimisticLockException;
use Doctrine\ORM\ORMException;
use Microsoft\BingAds\V13\Bulk\DownloadFileType;
use Microsoft\BingAds\V13\Bulk\GetBulkDownloadStatusRequest;
use Microsoft\BingAds\V13\Bulk\GetBulkUploadStatusRequest;
use Microsoft\BingAds\V13\Bulk\GetBulkUploadStatusResponse;
use Microsoft\BingAds\V13\Bulk\GetBulkUploadUrlRequest;
use Psr\Container\ContainerInterface;
use Psr\Log\LoggerInterface;
use Symfony\Component\Cache\Adapter\AdapterInterface;

ini_set("soap.wsdl_cache_enabled", "0");
ini_set("soap.wsdl_cache_ttl", "0");

/**
 * Class BingBatchManager
 *
 * @package App\Extensions\AdSystem\Bing\ExternalWork\Bulk
 */
abstract class BingBatchManager extends BatchManager
{
    /**
     * @var ContainerInterface
     */
    protected ContainerInterface $container;

    /**@var string*/
    private string $env;

    /**
     * @var BingServiceManager
     */
    private BingServiceManager $serviceManager;

    /**
     * @var LoggerInterface
     */
    private LoggerInterface $logger;

    /**
     * @var AdapterInterface
     */
    public AdapterInterface $cache;

    /**
     * @var string
     */
    public string $DownloadFileType = DownloadFileType::Csv;

    /**
     * BingBatchManager constructor.
     * @param ContainerInterface        $container
     * @param BingServiceManager        $serviceManager
     * @param EntityManagerInterface    $em
     * @param DocumentManager           $dm
     * @param LoggerInterface           $bingLogger
     * @param AdapterInterface          $cache
     */
    public function __construct(
        ContainerInterface      $container,
        BingServiceManager      $serviceManager,
        EntityManagerInterface  $em,
        DocumentManager         $dm,
        LoggerInterface         $bingLogger,
        AdapterInterface        $cache
    ) {
        parent::__construct(AdSystemEnum::BING, $em, $dm, $serviceManager->getProjectDir());

        $this->container        = $container;
        $this->serviceManager   = $serviceManager;
        $this->logger           = $bingLogger;
        $this->cache            = $cache;
        $this->env              = $serviceManager->getEnv();
    }

    /**
     * Array of fields which use during creating operations
     */
    abstract static function getOperationFields(): array;

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
     * @return BingServiceManager
     */
    protected function getBingServiceManager(): BingServiceManager
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
     * @param string $responseMode
     * @param int $accountId
     * @return mixed
     */
    function GetBulkUploadUrl(string $responseMode, int $accountId)
    {
        $bulkService = $this->getBingServiceManager()->getBulkService($accountId);

        $request = new GetBulkUploadUrlRequest();

        $request->ResponseMode = $responseMode;
        $request->AccountId = $accountId;

        return $bulkService->GetService()->GetBulkUploadUrl($request);
    }

    /**
     * @param $requestId
     * @param $accountId
     * @return mixed
     */
    function GetBulkUploadStatus($requestId, int $accountId)
    {
        $bulkService = $this->getBingServiceManager()->getBulkService($accountId);

        $request = new GetBulkUploadStatusRequest();

        $request->RequestId = $requestId;

        return $bulkService->GetService()->GetBulkUploadStatus($request);
    }

    /**
     * @param $requestId
     * @param $accountId
     * @return mixed
     */
    public function GetBulkDownloadStatus($requestId, int $accountId)
    {
        $bulkService = $this->getBingServiceManager()->getBulkService($accountId);

        $request = new GetBulkDownloadStatusRequest();

        $request->RequestId = $requestId;

        return $bulkService->GetService()->GetBulkDownloadStatus($request);
    }

    /**
     * @param array $operations
     * @param array $entitiesIds
     * @param SystemAccountInterface $account
     * @param string $action
     *
     * @throws ORMException | OptimisticLockException | \Exception
     */
    protected function uploadOperations(array $operations, array $entitiesIds, SystemAccountInterface $account, string $action)
    {
        $this->getLogger()->info("Creating new {$this->getOperandType()}:{$action} batch job in " . AdSystemEnum::BING);

        $batchJob = $this->GetBulkUploadUrl(
            BatchUploadDownloadHelper::getResponseMode($action),
            $account->getSystemAccountId());

        $uploadRequestId = $batchJob->RequestId;
        $uploadUrl = $batchJob->UploadUrl;

        $this->getLogger()->info("Creating new {$this->getOperandType()}:{$action} batch job locally");

        $hotsBatchJob = $this->createHotsBatchJob($account, $uploadRequestId, $entitiesIds, $action);

        $this->getLogger()
            ->info("Uploading operations to " . AdSystemEnum::BING . " for batch job {$hotsBatchJob->getSystemJobId()}");

        try {
            $batchUploadDownloadHelper = new BatchUploadDownloadHelper(
                $this->getBingServiceManager()->getCustomerId(),
                $account->getSystemAccountId(),
                $this->getOperandType(),
                $action);

            // Upload a bulk file to Bing.
            $uploadSuccess = $batchUploadDownloadHelper
                ->makeFile($this->getOperationFields(), $operations)
                ->compressFileToZip()
                ->uploadFile(
                    $uploadUrl,
                    $this->getBingServiceManager()->getAccessToken(),
                    $this->getBingServiceManager()->getDeveloperToken());


            // If the file was not uploaded, do not continue to poll for results.
            if ($uploadSuccess == false) {
                $hotsBatchJob->setStatus(BingBatchJob::STATUS_UPLOADING_ERROR);
                $this->getEntityManager()->flush();
                return;
            }


            $this->getLogger()->info(sprintf("Uploaded %d operations for batch job with ID %d.\n",
                count($operations), $uploadRequestId), [$hotsBatchJob->getOperandType(), $hotsBatchJob->getAction()]);
        } catch (\Exception $e) {
            $hotsBatchJob->setStatus(BatchJob::STATUS_ERROR);
            $this->getEntityManager()->flush();

            $this->getAirbrakeNotifier()->notify(new \Exception(
                '[Bing] Exception was thrown with message - ' . $e->getMessage()
                . '. When the process was running "uploadOperations"'. PHP_EOL
                )
            );
        }
    }

    /**
     * @param SystemAccountInterface $systemAccount
     * @param $batchJobId
     * @param array $entitiesIds
     * @param string $action
     * @return BingBatchJob
     * @throws ORMException
     * @throws OptimisticLockException
     */
    protected function createHotsBatchJob(
        SystemAccountInterface $systemAccount,
        $batchJobId,
        array $entitiesIds,
        string $action
    ): BatchJobInterface {
        $hotsBatchJob = new BingBatchJob();
        $hotsBatchJob->setAction($action);
        $hotsBatchJob->setOperandType($this->getOperandType());
        $hotsBatchJob->setStatus(BatchJob::STATUS_PENDING_RESULT);
        $hotsBatchJob->setSystemAccount($systemAccount);
        $hotsBatchJob->setSystemJobId($batchJobId);
        $hotsBatchJob->setMetaData(json_encode(['ids' => $entitiesIds]));
        $hotsBatchJob->setExecuteAt(new \DateTime("+ 30 second"));
        $em = $this->getEntityManager();
        $em->persist($hotsBatchJob);
        $em->flush();

        return $hotsBatchJob;
    }

    /**
     * @param BatchJobInterface $hotsBatchJob
     * @throws ORMException
     * @throws OptimisticLockException
     */
    public function checkBatchJobResult(BatchJobInterface $hotsBatchJob)
    {
        //echo "Memory Usage: " . (memory_get_usage()/1048576) . " MB \n";
        //echo date("Y-m-d H:i:s")."\n";

        $em = $this->getEntityManager();

        if ($hotsBatchJob->getStatus() != BatchJob::STATUS_PENDING_RESULT) {
            throw new \Exception("Unexpected status {$hotsBatchJob->getStatus()} in job {$hotsBatchJob->getId()}");
        }

        $results = $this->getBatchJobResults($hotsBatchJob);
        //Flush changes in $hotsBatchJob object after call function getBatchJobResults
        $em->flush();

        if (empty($results)) {
            // Batch job processing is not finished yet or failed
            return;
        }

        $this->processResults($results, $hotsBatchJob);

        $hotsBatchJob->setStatus(BatchJob::STATUS_COMPLETE);
        $em->flush();

        /*$this->getLogger()->info("Total {$this->getOperandType()}s processing stats",
            ['success' => count($results), 'errors' => count(0), $hotsBatchJob->getId(), $hotsBatchJob->getSystemJobId()]);*/
        $this->getLogger()->info("Memory usage: " . (memory_get_peak_usage(true) / 1024 / 1024),
            [$hotsBatchJob->getId(), $hotsBatchJob->getSystemJobId()]);

        //echo "Memory Usage: " . (memory_get_usage()/1048576) . " MB \n";
        //echo date("Y-m-d H:i:s")."\n";
    }

    /**
     * @param BatchJobInterface $hotsBatchJob
     * @return array|null
     */
    protected function getBatchJobResults(BatchJobInterface $hotsBatchJob):? array
    {
        ini_set('memory_limit', '2G');

        $hotsBatchJob->setAttempts($hotsBatchJob->getAttempts() + 1);
        $sleepSeconds = 15 * pow(2, $hotsBatchJob->getAttempts());
        //if (!empty($hotsBatchJob->getXmlResponse())) {
        // Bing doesn't have xml response. See adwords realisation for this case.
        //}
        $hotsBatchJob->setExecuteAt(new \DateTime("+ $sleepSeconds second"));

        if ($hotsBatchJob->getAttempts() > 7) {
            $this->getLogger()->error("Batch job is too long in progress status", [$hotsBatchJob->getOperandType(),
                $hotsBatchJob->getAction(), $hotsBatchJob->getId(), $hotsBatchJob->getSystemJobId(),
                $hotsBatchJob->getAttempts()]);
        }

        $systemAccountId = $hotsBatchJob->getSystemAccount()->getSystemAccountId();
        // Get the upload request status.
        /** @var GetBulkUploadStatusResponse $batchJob */
        $batchJob = $this->GetBulkUploadStatus($hotsBatchJob->getSystemJobId(), $systemAccountId);

        $uploadRequestStatus = $batchJob->RequestStatus;
        // Still in Process
        if (in_array($uploadRequestStatus, [BingBatchJob::STATUS_FILE_UPLOADED, BingBatchJob::STATUS_IN_PROCESS])) {
            $this->getLogger()->info("Batch job ID is still in progress", [$hotsBatchJob->getOperandType(),
                $hotsBatchJob->getAction(), $hotsBatchJob->getId(), $hotsBatchJob->getSystemJobId()]);

            return null;
        }

        // Still in Process
        if (in_array($uploadRequestStatus, [BingBatchJob::STATUS_PENDING_FILE_UPLOAD])) {
            $this->getLogger()->info("The upload file has not been received for the corresponding RequestId.", [
                $hotsBatchJob->getOperandType(), $hotsBatchJob->getAction(), $hotsBatchJob->getId(),
                $hotsBatchJob->getSystemJobId()]);
            $hotsBatchJob->setStatus("FileNotUploaded");

            $this->getAirbrakeNotifier()->notify(new \Exception(
                '[Bing] Exception was thrown with message - The upload file has not been received. '
                . 'When the process was running "getBatchJobResults" Batch job Id: '
                . $hotsBatchJob->getSystemJobId() . PHP_EOL
            ));

            return null;
        }

        // Save general Batch uploading ERROR (not about each item)
        if (!empty($batchJob->Errors->OperationError)) {
            foreach ($batchJob->Errors->OperationError as $processingError) {
                $this->getLogger()->error("Batch job local ID: {$hotsBatchJob->getId()} processing error", [
                    $processingError->Code, $processingError->Details,
                    $processingError->ErrorCode, $processingError->Message
                ]);
                $this->getAirbrakeNotifier()->notify(new \Exception(
                    '[Bing] Exception was thrown with message - %s '. $processingError->Message
                    . '. When the process was running \"getBatchJobResults\" Batch job Id: '
                    . $hotsBatchJob->getId() . PHP_EOL
                ));
            }
        }

        $hotsBatchJob->setExecuteAt(new \DateTime("+ 60 second"));

        // Batch status FAILED
        if ($batchJob->RequestStatus === BingBatchJob::STATUS_FAILED) {
            $this->getLogger()->error("Canceled batch job " . var_export($batchJob, true),
                [$hotsBatchJob->getId(), $hotsBatchJob->getSystemJobId()]);
            $hotsBatchJob->setStatus(BingBatchJob::STATUS_FAILED);

            $this->getAirbrakeNotifier()->notify(new \Exception(
                '[Bing] System batch job Id: ' . $hotsBatchJob->getSystemJobId()
                . ', status - '. BingBatchJob::STATUS_FAILED
                . '. When the process was running "getBatchJobResults"'. PHP_EOL
            ));

            return null;
        }

        // Download Url is EMPTY
        $uploadResultFileUrl = $batchJob->ResultFileUrl;
        if (empty($uploadResultFileUrl)) {
            $this->getLogger()->error("downloadUrl is EMPTY", [$hotsBatchJob->getOperandType(), $hotsBatchJob->getAction(),
                $hotsBatchJob->getId(), $hotsBatchJob->getSystemJobId(), var_export($batchJob, true)]);
            $hotsBatchJob->setStatus("downloadUrl is EMPTY");

            return null;
        }

        // Batch status COMPLETED or COMPLETED_WITH_ERRORS
        if (in_array($uploadRequestStatus, [
                BingBatchJob::STATUS_COMPLETED,
                BingBatchJob::STATUS_COMPLETED_WITH_ERRORS,
            ]
        )) {
            try {
                $this->getLogger()->info(sprintf("Downloading results from %s:\n", $uploadResultFileUrl));

                $batchUploadDownloadHelper = new BatchUploadDownloadHelper(
                    $this->getBingServiceManager()->getCustomerId(),
                    $systemAccountId,
                    $hotsBatchJob->getOperandType(),
                    $hotsBatchJob->getAction());

                // Download zip result file and get content as string from csv file which include to zip.
                $csvResultAsString = $batchUploadDownloadHelper
                    ->downloadFile($uploadResultFileUrl)
                    ->getCsvContentFromZip();

                if (empty($csvResultAsString)) {
                    $hotsBatchJob->setStatus("Empty response");

                    return null;
                }

                // Parse response from Bing
                $result = $this->checkResponse($csvResultAsString, $hotsBatchJob->getAction());

                $this->getLogger()->info("Fetched batch job results", [$hotsBatchJob->getOperandType(),
                    $hotsBatchJob->getAction(), $hotsBatchJob->getId(), $hotsBatchJob->getSystemJobId()]);

                return $result;
            } catch (\Exception $e) {
                $this->getAirbrakeNotifier()->notify(new \Exception(
                    '[Bing] Exception was thrown with message -' . $e->getMessage()
                    .'. When the process was running "uploadOperations"'. PHP_EOL
                ));

                return null;
            }
        } else {
            $hotsBatchJob->setStatus($uploadRequestStatus);

            return null;
        }
    }

    /**
     * @param string $csvContentAsString
     * @param string $action
     * @return array
     */
    protected function checkResponse(string $csvContentAsString, string $action): array
    {
        $results = [
            'results' => [],
            'errors' => [],
        ];

        $lines = explode("\n", $csvContentAsString);
        $headers = str_getcsv($lines[0]);

        // Remove headers from result
        $numberOfHeaders = BatchUploadDownloadHelper::getNumberOfHeadersInResultBatch($action);
        array_splice($lines, 0, $numberOfHeaders);

        foreach ($lines as $line) {
            $row = [];
            foreach (str_getcsv($line) as $key => $field)
                $row[$headers[$key]] = trim($field);

            // In some cases, bing returns duplicates with empty margins id and error it's happens internal error bind server
            if (empty($row['Id']) && empty($row['Error'])) {
                continue;
            }

            $row = array_filter($row, 'strlen');

            // ADD - response include result items and errors
            // UPDATE, REMOVE - response include only error
            // See ResponseMode config during uploading batch
            if (!empty($row['Id']) && empty($row['Error']) && $action == BatchJob::ACTION_ADD) {
                $results['results'][$row['Client Id']] = $row['Id'];
            } elseif (!empty($row['Error']) && empty($results['Id'])) {
                $errorInfo = ['Error' => $row['Error'], 'Error Number' => $row['Error Number']];
                $results['errors'][$row['Client Id']] = $errorInfo;
            }
        }

        unset($lines);

        return $results;
    }
}