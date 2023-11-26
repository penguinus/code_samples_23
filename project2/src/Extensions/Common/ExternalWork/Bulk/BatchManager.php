<?php

namespace App\Extensions\Common\ExternalWork\Bulk;

use App\Interfaces\DocumentRepositoryInterface\BatchItemsUploadInterface;
use App\Interfaces\EntityInterface\BatchJobInterface;
use App\Interfaces\EntityInterface\SystemAccountInterface;
use App\Providers\ProviderEntityName;
use Doctrine\ODM\MongoDB\DocumentManager;
use Doctrine\ORM\EntityManager;
use Doctrine\ORM\EntityManagerInterface;
use Psr\Log\LoggerInterface;
use Symfony\Component\Cache\Adapter\AdapterInterface;
use Symfony\Component\Process\Process;

/**
 * Class BatchManager
 *
 * @package App\Extensions\Common\ExternalWork\Bulk
 */
abstract class BatchManager
{
    /**
     * Limit 10000
     */
    public const BATCH_SIZE = 10000;

    /**
     * at least 2 seconds should be between API requests with the same client id
     */
    public const BJS_API_DELAY = 2;

    // 3 parallel jobs maximum
    public const MAX_PARALLEL_JOBS_COUNT = 4;

    /**
     * @var DocumentManager
     */
    private DocumentManager $dm;

    /**
     * @var EntityManager
     */
    private EntityManager $em;

    /**
     * @var string
     */
    protected string $adSystem;

    /**
     * @var string
     */
    private string $rootDir;

    /**
     * BatchManager constructor.
     * @param string $adSystem
     * @param EntityManagerInterface $em
     * @param DocumentManager $dm
     * @param string $rootDir
     */
    public function __construct(string $adSystem, EntityManagerInterface $em, DocumentManager $dm, string $rootDir)
    {
        $this->adSystem = strtolower($adSystem);
        $this->rootDir = $rootDir;

        $dm->setAdSystem($this->adSystem);
        $this->dm = $dm;

        $this->em = $em;
    }

    /**
     * @return string
     */
    protected function getRootDir(): string
    {
        return $this->rootDir;
    }

    /**
     * @return string
     */
    abstract protected function getEnv(): string;

    /**
     * @return AdapterInterface
     */
    abstract protected function getCache(): AdapterInterface;

    /**
     * @return LoggerInterface
     */
    abstract protected function getLogger(): LoggerInterface;

    /**
     * @return string
     */
    abstract protected function getOperandType(): string;

    /**
     * @return BatchItemsUploadInterface
     */
    abstract protected function getQueryRepository(): BatchItemsUploadInterface;

    /**
     * @param array $hots_entities
     * @param $customerId
     * @return array|bool
     */
    abstract protected function buildAddOperations(array $hots_entities, $customerId);

    /**
     * @param array $hots_entities
     * @param $customerId
     * @return array|bool
     */
    abstract protected function buildUpdateOperations(array $hots_entities, $customerId);

    /**
     * @param array $hots_entities
     * @param $customerId
     * @return array
     */
    abstract protected function buildDeleteOperations(array $hots_entities, $customerId): array;

    /**
     * @param array $operations
     * @param array $entitiesIds
     * @param SystemAccountInterface $account
     * @param string $action
     * @return mixed
     */
    abstract protected function uploadOperations(
        array $operations,
        array $entitiesIds,
        SystemAccountInterface $account,
        string $action
    );

    /**
     * @param array $results
     * @param BatchJobInterface $hotsBatchJob
     * @return mixed
     */
    abstract protected function processResults(array $results, BatchJobInterface $hotsBatchJob);

    /**
     * @param BatchJobInterface $hotsBatchJob
     */
    abstract public function checkBatchJobResult(BatchJobInterface $hotsBatchJob);

    /**
     * @return EntityManager
     */
    protected function getEntityManager(): EntityManager
    {
        return $this->em;
    }

    /**
     * @return DocumentManager
     */
    protected function getDocumentManager(): DocumentManager
    {
        return $this->dm;
    }

    /**
     * @return int[]
     */
    public function getAccountsPendingUpload(): array
    {
        $dm = $this->getDocumentManager();
        $em = $this->getEntityManager();

        $entityName = ProviderEntityName::getForBatchJobBySystem($this->adSystem);
        $excludedSystemAccounts = $em->getRepository($entityName)
            ->getPendingSystemAccounts($this->getOperandType(), BatchJob::ACTION_ADD);

        return $this->getQueryRepository()->getAccountsPendingUpload($dm, $excludedSystemAccounts);
    }

    /**
     * @return int[]
     */
    public function getAccountsPendingUpdates(): array
    {
        $dm = $this->getDocumentManager();
        $em = $this->getEntityManager();

        $entityName = ProviderEntityName::getForBatchJobBySystem($this->adSystem);
        $excludedSystemAccounts = $em->getRepository($entityName)
            ->getPendingSystemAccounts($this->getOperandType(), BatchJob::ACTION_UPDATE);

        return $this->getQueryRepository()->getAccountsPendingUpdates($dm, $excludedSystemAccounts);
    }

    /**
     * @return int[]
     */
    public function getAccountsPendingDelete(): array
    {
        $dm = $this->getDocumentManager();
        $em = $this->getEntityManager();

        $entityName = ProviderEntityName::getForBatchJobBySystem($this->adSystem);
        $excludedSystemAccounts = $em->getRepository($entityName)
            ->getPendingSystemAccounts($this->getOperandType(), BatchJob::ACTION_REMOVE);

        return $this->getQueryRepository()->getAccountsPendingDelete($dm, $excludedSystemAccounts);
    }

    /**
     * @param int $account_id
     * @param int $limit
     * @return array
     */
    protected function getEntitiesIdsPendingAdd(int $account_id, int $limit): array
    {
        $dm = $this->getDocumentManager();
        return $this->getQueryRepository()->getIdsPendingUpload($dm, $account_id, $limit);
    }

    /**
     * @param int $account_id
     * @param int $limit
     * @return array
     */
    protected function getEntitiesIdsPendingUpdate(int $account_id, int $limit): array
    {
        $dm = $this->getDocumentManager();
        return $this->getQueryRepository()->getIdsPendingUpdate($dm, $account_id, $limit);
    }

    /**
     * @param int $account_id
     * @param int $limit
     * @return array
     */
    protected function getEntitiesIdsPendingDelete(int $account_id, int $limit): array
    {
        $dm = $this->getDocumentManager();
        return $this->getQueryRepository()->getIdsPendingDelete($dm, $account_id, $limit);
    }

    /**
     * Override in case if special logic is required for processing failed batch jobs
     *
     * @param BatchJobInterface $hotsBatchJob
     * @param array             $batchJobResults
     */
    abstract protected function failedResultProcessingFallback(BatchJobInterface $hotsBatchJob, array $batchJobResults);

    /**
     * @throws \Exception
     */
    public function scheduleUpdates()
    {
        $accounts = $this->getAccountsPendingUpdates();
        $this->scheduleAction(BatchJob::ACTION_UPDATE, $accounts);
    }

    /**
     * @throws \Exception
     */
    public function scheduleDeletes()
    {
        $accounts = $this->getAccountsPendingDelete();
        $this->scheduleAction(BatchJob::ACTION_REMOVE, $accounts);
    }

    /**
     * @param bool $dry_run
     * @throws \Exception
     */
    public function scheduleAdds(bool $dry_run = false)
    {
        $accounts = $this->getAccountsPendingUpload();
        $this->scheduleAction(BatchJob::ACTION_ADD, $accounts, $dry_run);
    }

    /**
     * @param string $action
     * @param array $accounts
     * @param bool $dry_run
     * @throws \Exception
     */
    protected function scheduleAction(string $action, array $accounts, bool $dry_run = false)
    {
        $this->getLogger()->info("Fetched accounts with {$this->getOperandType()}s pending $action");
        foreach ($accounts as $account_id) {
            $this->getLogger()->info("Start processing google account id: ".$account_id);
            $this->generateAndUploadOperations($account_id, $action, $dry_run);
        }
    }

    /**
     * @param int $account_id
     * @param string $action
     * @return \Generator
     * @throws \Exception
     */
    protected function batchesGenerator(int $account_id, string $action): \Generator
    {
        switch ($action) {
            case BatchJob::ACTION_ADD:
                $entities = $this->getEntitiesIdsPendingAdd($account_id, static::BATCH_SIZE * static::MAX_PARALLEL_JOBS_COUNT);
                break;
            case BatchJob::ACTION_UPDATE:
                $entities = $this->getEntitiesIdsPendingUpdate($account_id, static::BATCH_SIZE * static::MAX_PARALLEL_JOBS_COUNT);
                break;
            case BatchJob::ACTION_REMOVE:
                $entities = $this->getEntitiesIdsPendingDelete($account_id, static::BATCH_SIZE * static::MAX_PARALLEL_JOBS_COUNT);
                break;
            default:
                throw new \Exception("Unsupported batchesGenerator action $action");
        }

        foreach (array_chunk($entities, static::BATCH_SIZE) as $batch_entities) {
            if (empty($batch_entities)) {
                throw new \Exception("No {$this->getOperandType()}s were found for batch job to $action");
            }
            yield $batch_entities;
        }
    }

    /**
     * @param array $hots_entities
     * @param $customerId
     * @param string $action
     * @return array|bool
     * @throws \Exception
     */
    protected function buildOperations(array $hots_entities, $customerId, string $action)
    {
        switch ($action) {
            case BatchJob::ACTION_ADD:
                $result = $this->buildAddOperations($hots_entities, $customerId);
                break;
            case BatchJob::ACTION_UPDATE:
                $result = $this->buildUpdateOperations($hots_entities, $customerId);
                break;
            case BatchJob::ACTION_REMOVE:
                $result = $this->buildDeleteOperations($hots_entities, $customerId);
                break;
            default:
                throw new \Exception("Unsupported buildOperations action $action");
        }

        list($operations, $entitiesIds) = $result;

        if (empty($operations)) {
            return false;
        } elseif (count($operations) != count($entitiesIds) && $this->getOperandType() != BatchJob::OPERAND_TYPE_EXTENSION) {
            throw new \Exception("Number of operations is different from number of entities ids");
        } else {
            return $result;
        }
    }

    /**
     * @param int $account_id
     * @param string $action
     * @param bool $dry_run
     * @throws \Exception
     */
    public function generateAndUploadOperations(int $account_id, string $action, bool $dry_run = false)
    {
        ini_set('memory_limit', '3G');

        $entityName = ProviderEntityName::getForAccountsBySystem($this->adSystem);

        /** @var SystemAccountInterface $account*/
        $account = $this->getEntityManager()->getRepository($entityName)
            ->findOneBy(['id' => $account_id, 'available' => 1]);

        if (empty($account) || $account->getSystemAccountId() == null) {
            $this->getLogger()->error("Can't finally operations because account Id : $account_id not found or system id is empty.");
            return;
        }

        $batch_number = 0;
        foreach ($this->batchesGenerator($account_id, $action) as $hots_entities) {
            if ($batch_number > 0) {
                sleep(self::BJS_API_DELAY);
            }

            $this->getLogger()->info("Start building operations for {$this->getOperandType()}:{$action} batch job $batch_number");
            list($operations, $entitiesIds) = $this->buildOperations($hots_entities, $account->getSystemAccountId(), $action);

            if ($operations != false) {
                $this->getLogger()->info(
                    "Total operations generated for {$this->getOperandType()}:{$action} batch job $batch_number: ".
                    count($operations)
                );
                if (!$dry_run) {
                    $this->uploadOperations($operations, $entitiesIds, $account, $action);
                } else {
                    $this->getLogger()->info("Skip operations uploading because running in dry run mode");
                }
                $batch_number++;
                $this->getLogger()->info("Memory usage: ".(memory_get_peak_usage(true) / 1024 / 1024));
            }
        }
    }

    /**
     * @param string $action
     * @param int $limit
     */
    public function processPendingJobs(string $action, int $limit = 5)
    {
        $entityName = ProviderEntityName::getForBatchJobBySystem($this->adSystem);

        /** @var BatchJobInterface[] $hotsBatchJobs */
        $hotsBatchJobs = $this->getEntityManager()->getRepository($entityName)
            ->getReadyPendingJobs($this->getOperandType(), $action, $limit);

        if (!empty($hotsBatchJobs)) {
            $processes = [];
            foreach ($hotsBatchJobs as $hotsBatchJob) {
                $command = "php {$this->getRootDir()}/../bin/console keywordsconnect:checkBatchJobResultWorker {$this->adSystem} {$hotsBatchJob->getId()} --env=" . $this->getEnv();
                $process = new Process($command);
                $process->setTimeout(240);
                $processes[$hotsBatchJob->getId()] = $process;
            }

            while (!empty($processes)) {
                foreach ($processes as $i => $process) {
                    $process->start();
                    $process->wait();

                    // checks the process is ended and don't output error
                    if ($process->isTerminated() && !$process->isSuccessful()) {
                        echo $process->getErrorOutput();
                    } else {
                        echo $process->getOutput();
                    }

                    unset($processes[$i]);
                }
            }
        }
    }

    /**
     * @param BatchJobInterface $hotsBatchJob
     * @return array
     */
    protected function jsonDecodeMetaData(BatchJobInterface $hotsBatchJob): array
    {
        return json_decode($hotsBatchJob->getMetaData(), true)['ids'];
    }

    /**
     * @param BatchJobInterface $hotsBatchJob
     * @param array             $update_data
     *
     * @throws \Exception
     */
    protected function processingCollectionByAction(BatchJobInterface $hotsBatchJob, array $update_data) {
        switch ($hotsBatchJob->getAction()) {
            case BatchJob::ACTION_ADD:
                $this->processCollectionsAfterAdd($update_data);

                break;
            case BatchJob::ACTION_UPDATE:
                $this->processCollectionsAfterUpdate($update_data);

                break;
            case BatchJob::ACTION_REMOVE:
                $this->processCollectionsAfterRemove($update_data);

                break;
            default :
                throw new \Exception("Unknown action {$hotsBatchJob->getAction()} in processResults");
        }
    }
}
