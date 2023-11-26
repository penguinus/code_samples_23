<?php

namespace App\Extensions\AdSystem\Bing\ExternalWork\Bulk;

use App\Document\Ad;
use App\Document\Adgroup;
use App\Document\AdgroupsQueue;
use App\Document\AdsQueue;
use App\Document\ErrorsQueue;
use App\Document\Keyword;
use App\Document\KeywordsQueue;
use App\Entity\BingBatchJob;
use App\Extensions\AdSystem\Bing\ExternalWork\BingErrorDetail;
use App\Extensions\Common\ContentType;
use App\Interfaces\DocumentRepositoryInterface\BatchItemsUploadInterface;
use App\Interfaces\EntityInterface\BatchJobInterface;
use App\Providers\ProviderCampaignName;
use App\Providers\ProviderDocumentName;
use Doctrine\ODM\MongoDB\DocumentManager;
use MongoDB\BSON\ObjectId;

class BingAdGroupManager extends BingBatchManager
{
    CONST BATCH_SIZE = 5000;

    const OPERATION_FIELDS = array(
        "type" => "Type",
        "status" => "Status",
        "id" => "Id",
        "parentId" => "Parent Id",
        "clientId" => "Client Id",
        "adGroup" => "Ad Group",
        "cpcBid" => "Cpc Bid",
        "startDate" => "Start Date",
        "networkDist" => "Network Distribution",
        "language" => "Language",
        "name" => "Name",
    );

    /**
     * @return string
     */
    public function getOperandType(): string
    {
        return BingBatchJob::OPERAND_TYPE_ADGROUP;
    }

    /**
     * @return array
     */
    static function getOperationFields(): array
    {
        return self::OPERATION_FIELDS;
    }

    /**
     * Init empty array with keys by fields
     *
     * @return array
     */
    static function initEmptyArrayByFields(): array
    {
        return array_fill_keys(array_keys(self::getOperationFields()), '');
    }

    /**
     * @return BatchItemsUploadInterface
     */
    public function getQueryRepository(): BatchItemsUploadInterface
    {
        return $this->getDocumentManager()->getRepository(AdgroupsQueue::class);
    }

    /**
     * @param array $hots_entities
     * @param  $customerId
     * @return array
     */
    protected function buildAddOperations(array $hots_entities, $customerId): array
    {
        $operations = [];
        $entitiesIds = [];

        $adGroup = $this->initEmptyArrayByFields();
        foreach ($hots_entities as $hots_adgroup) {
            // Targetting restriction settings - these setting only affect serving
            // Adwords has such settings but bing doesn't have analogous settings. See Adwords group settings in code.
            $adGroup['type'] = 'Ad Group';
            $adGroup['status'] = $hots_adgroup['status'] ? 'Active' : 'Paused';
            $adGroup['parentId'] = $hots_adgroup['systemCampaignId'];
            $adGroup['clientId'] = (string)$hots_adgroup['_id'];
            $adGroup['adGroup'] = $hots_adgroup['adgroup'];
            $adGroup['cpcBid'] = $hots_adgroup['defaultCpc'];
            // Set start date. For adwords setting on campaign level.
            $adGroup['startDate'] = date('m/d/y', strtotime('+1 day'));
            // Set network settings. For adwords setting on campaign level. Now default value set.
            $adGroup['networkDist'] = "OwnedAndOperatedAndSyndicatedSearch";
            // Set Languages. For adwords setting on campaign level.
            $adGroup['language'] = 'English';

            $operations[] = $adGroup;
            $entitiesIds[] = (string)$hots_adgroup['_id'];
        }

        return [$operations, $entitiesIds];
    }

    /**
     * @param array $hots_entities
     * @param $customerId
     * @return array
     */
    protected function buildUpdateOperations(array $hots_entities, $customerId): array
    {
        $operations = [];
        $entitiesIds = [];

        $adGroup = $this->initEmptyArrayByFields();
        foreach ($hots_entities as $hots_adgroup) {
            $adGroup['type'] = 'Ad Group';
            if(isset($hots_adgroup['status'])) {
                $adGroup['status'] = $hots_adgroup['status'] ? 'Active' : 'Paused';
            }
            $adGroup['id'] = $hots_adgroup['systemAdgroupId'];
            $adGroup['parentId'] = $hots_adgroup['systemCampaignId'];
            $adGroup['clientId'] = (string)$hots_adgroup['_id'];
            $adGroup['adGroup'] = $hots_adgroup['adgroup'];
            $adGroup['cpcBid'] = $hots_adgroup['defaultCpc'];

            $operations[] = $adGroup;
            $entitiesIds[] = (string)$hots_adgroup['_id'];
        }
        return [$operations, $entitiesIds];
    }


    /**
     * @param array $hots_entities
     * @param $customerId
     * @return array
     */
    protected function buildDeleteOperations(array $hots_entities, $customerId): array
    {
        $operations = [];
        $entitiesIds = [];

        $adGroup = $this->initEmptyArrayByFields();
        foreach ($hots_entities as $hots_adgroup) {
            $adGroup['type'] = 'Ad Group';
            $adGroup['status'] = 'Deleted';
            $adGroup['id'] = $hots_adgroup['systemAdgroupId'];
            $adGroup['parentId'] = $hots_adgroup['systemCampaignId'];
            $adGroup['clientId'] = (string)$hots_adgroup['_id'];

            $operations[] = $adGroup;
            $entitiesIds[] = (string)$hots_adgroup['_id'];
        }
        return [$operations, $entitiesIds];
    }

    /**
     * @param BatchJobInterface $hotsBatchJob
     * @param array|null        $batchJobResults
     */
    protected function failedResultProcessingFallback(BatchJobInterface $hotsBatchJob, array $batchJobResults)
    {
        // See Adwords realisation
    }

    /**
     * @param array $resultSuccesses
     * @throws
     */
    public function processCollectionsAfterAdd(array $resultSuccesses)
    {
        if(empty($resultSuccesses))
            return;

        ini_set("memory_limit", "7G");

        /**@var DocumentManager $dm*/
        $dm = $this->getDocumentManager();

        $_ids = array_map(function ($id){ return new ObjectId($id); }, array_keys($resultSuccesses));

        $selectFields = ['id', 'brandTemplateId', 'campaignId', 'teId', 'kcCampaignBackendId',
            'systemCampaignId', 'defaultCpc', 'status'];
        $adgroups = $dm->getRepository(AdgroupsQueue::class)->getByIds($dm , $_ids, $selectFields, true);

        $campaignIds = array();
        $teAdgroupIds = array();
        $adgroupsByTemplate = array();
        $adgroupData = array();

        /** @var AdgroupsQueue $adgroup */
        foreach ($adgroups as $adgroup)
        {
            $campaignIds[] = new ObjectId((string)$adgroup['campaignId']);
            $teAdgroupIds[] = $adgroup['teId'];

            $adgroupsByTemplate[$adgroup['brandTemplateId']][] = $adgroup;
            $adgroupData[(string)$adgroup['campaignId']][$adgroup['teId']] =
                (integer)$resultSuccesses[(string)$adgroup['_id']];
        }
        unset($adgroups);

        $campaignIds = array_unique($campaignIds);
        // Set system adgroup id to adgroups queue
        $this->setSystemIdToAdgroupsQueue($adgroupData, $campaignIds, $teAdgroupIds);

        // Set system adgroup id to keywords queue
        $this->setSystemAdgroupIdToQueue( KeywordsQueue::class, $adgroupData, $campaignIds, $teAdgroupIds);

        // Set system adgroup id to ads queue
        $this->setSystemAdgroupIdToQueue( AdsQueue::class, $adgroupData, $campaignIds, $teAdgroupIds);
        unset($campaignIds, $teAdgroupIds, $adgroupData);

        // Add ad groups to main Adgroup collections by templates
        $this->addAdGroupsToMainCollections($adgroupsByTemplate, $resultSuccesses);
        unset($adgroupsByTemplate, $resultSuccesses);

        // Remove ad groups from Queue
        $dm->getRepository(AdgroupsQueue::class)->removeByIds($dm , $_ids);
    }

    protected function setSystemIdToAdgroupsQueue(array $adgroupData, array $campaignIds, array $teIds)
    {
        $dm = $this->getDocumentManager();

        /**@var AdgroupsQueue[] $adgroups*/
        $adgroups = $dm->createQueryBuilder(AdgroupsQueue::class)
            ->field('campaignId')->in($campaignIds)
            ->field('teId')->in($teIds)
            ->field('add')->exists(false)
            ->getQuery()->execute();

        foreach ($adgroups as $adgroup){
            if(isset($adgroupData[$adgroup->getCampaignId()][$adgroup->getTeId()]))
                $adgroup->setSystemAdgroupId($adgroupData[$adgroup->getCampaignId()][$adgroup->getTeId()]);
        }
        $dm->flush();

        foreach ($adgroups as $adgroup){
            $dm->detach($adgroup);
            unset($adgroup);
        }
        $dm->flush();
        gc_collect_cycles();
    }

    /**
     * @param array $adgroupsByTemplate
     * @param array $resultSuccesses
     */
    protected function addAdGroupsToMainCollections(array $adgroupsByTemplate, array $resultSuccesses)
    {
        $dm = $this->getDocumentManager();

        $counter = 1;
        foreach ($adgroupsByTemplate as $templateId => $adgroups){
            $dm->setBrandTemplateId($templateId);

            /**@var AdgroupsQueue[] $adgroups*/
            foreach ($adgroups as $adgroup){
                $hotsAdgroup = new \App\Document\Adgroup;

                //$hotsAdgroup->setUniqueBid($adgroup['defaultCpc']);
                //$hotsAdgroup->setUniqueStatus($adgroup['status']);
                $hotsAdgroup->setTeId($adgroup['teId']);
                $hotsAdgroup->setKcCampaignBackendId($adgroup['kcCampaignBackendId']);
                $hotsAdgroup->setSystemCampaignId($adgroup['systemCampaignId']);
                $hotsAdgroup->setSystemAdgroupId($resultSuccesses[(string)$adgroup['_id']]);

                $dm->persist($hotsAdgroup);

                if (($counter % 50) === 0) {
                    $dm->flush();
                    $dm->clear();
                }

                $counter++;
            }
            $dm->flush();
        }
    }

    /**
     * @param string $documentName
     * @param array $adgroupData
     * @param array $campaignIds
     * @param array $teAdgroupIds
     * @throws
     */
    protected function setSystemAdgroupIdToQueue(string $documentName, array $adgroupData, array $campaignIds, array $teAdgroupIds)
    {
        $dm = $this->getDocumentManager();

        /** @var KeywordsQueue|AdsQueue $items */
        $items = $dm->createQueryBuilder($documentName)
            ->field('campaignId')->in($campaignIds)
            ->field('teAdgroupId')->in($teAdgroupIds)
            ->immortal() // no limit of time for cursor lifetime
            ->getQuery()
            ->execute();

        foreach ($items as $item){
            if(isset($adgroupData[$item->getCampaignId()][$item->getTeAdgroupId()]))
                $item->setSystemAdgroupId($adgroupData[$item->getCampaignId()][$item->getTeAdgroupId()]);
        }
        $dm->flush();

        // Clear memory
        foreach ($items as $item) { $dm->detach($item); unset($item); }
        gc_collect_cycles();
    }

    /**
     * @param array $ids
     * @throws \MongoException
     */
    protected function processCollectionsAfterUpdate(array $ids)
    {
        if(empty($ids))
            return;

        ini_set("memory_limit", "7G");

        /**@var DocumentManager $dm*/
        $dm = $this->getDocumentManager();

//        $uniqueAdgroupsFields = array();

        $_ids = array_map(function ($id){ return new ObjectId($id); }, $ids);

        /*$selectFields = ['defaultCpc', 'brandTemplateId', 'status', 'systemAdgroupId'];
        $adgroups = $dm->getRepository(AdgroupsQueue::class)->getByIds($dm , $_ids, $selectFields, true);

        foreach ($adgroups as $adgroup) {
            $updatingFields = array('defaultCpc' => $adgroup['defaultCpc'], 'status' => $adgroup['status']);
            $uniqueAdgroupsFields[$adgroup['brandTemplateId']][$adgroup['systemAdgroupId']] = $updatingFields;
        }
        unset($adgroups);

        // update unique ad group fields in main collection (Adgroup)
        $this->updateUniqueAdGroupFields($uniqueAdgroupsFields);
        unset($uniqueAdgroupsFields);*/

        // Remove ad groups from Queue
        $dm->getRepository(AdgroupsQueue::class)->removeByIds($dm , $_ids);
    }

//    /**
//     * @param $uniqueAdgroupsFields
//     */
//    protected function updateUniqueAdGroupFields($uniqueAdgroupsFields)
//    {
//        /**@var DocumentManager $dm*/
//        $dm = $this->getDocumentManager();
//
//        foreach ($uniqueAdgroupsFields as $templateId => $uniqueAdgroupsField) {
//            $dm->setBrandTemplateId($templateId);
//
//            $campaignAdgroups = $dm->getRepository(Adgroup::class)
//                ->getBySystemAdgroupIds($dm , array_keys($uniqueAdgroupsField));
//
//            /** @var Adgroup $campaignAdgroup */
//            foreach ($campaignAdgroups as $campaignAdgroup){
//                $campaignAdgroup->setUniqueBid($uniqueAdgroupsField[$campaignAdgroup->getSystemAdgroupId()]['defaultCpc']);
//                $campaignAdgroup->setUniqueStatus($uniqueAdgroupsField[$campaignAdgroup->getSystemAdgroupId()]['status']);
//            }
//            $dm->flush();
//        }
//    }

    /**
     * @param array $ids
     * @throws \MongoException
     */
    protected function processCollectionsAfterRemove(array $ids)
    {
        if(empty($ids))
            return;

        ini_set("memory_limit", "7G");

        /**@var DocumentManager $dm*/
        $dm = $this->getDocumentManager();

        $_ids = array_map(function ($id){ return new ObjectId($id); }, $ids);

        $selectFields = ['brandTemplateId', 'systemCampaignId', 'systemAdgroupId', 'teId'];
        /** @var AdgroupsQueue[] $adgroups */
        $adgroups = $dm->getRepository(AdgroupsQueue::class)->getByIds($dm , $_ids, $selectFields, true);

        $teIds = [];
        $systemAdgroupIds = [];
        $systemIdsByTemplate = [];
        $systemCampaignIds = [];
        foreach ($adgroups as $adgroup ){
            $systemIdsByTemplate[$adgroup['brandTemplateId']][] = $adgroup['systemAdgroupId'];
            $systemCampaignIds[] = $adgroup['systemCampaignId'];
            $systemAdgroupIds[] = $adgroup['systemAdgroupId'];
            $teIds[] = $adgroup['teId'];
        }
        unset($adgroups);
        $systemCampaignIds = array_unique($systemCampaignIds);

        foreach ($systemIdsByTemplate as $templateId => $systemIds)
        {
            $dm->setBrandTemplateId($templateId);

            $attributes = ['systemAdgroupId' => $systemIds, 'systemCampaignId' => $systemCampaignIds];

            $dm->getRepository(Keyword::class)->removeByAttributesIn($dm, $attributes);
            $dm->getRepository(Ad::class)->removeByAttributesIn($dm, $attributes);
            $dm->getRepository(Adgroup::class)->removeByAttributesIn($dm , $attributes);
        }

        $attributes = ['systemCampaignId' => $systemCampaignIds, 'systemAdgroupId' => $systemAdgroupIds];

        $dm->getRepository(KeywordsQueue::class)->removeByAttributesIn($dm, $attributes);
        $dm->getRepository(AdsQueue::class)->removeByAttributesIn($dm, $attributes);
        // Remove ad groups from Queue
        $dm->getRepository(AdgroupsQueue::class)->removeByIds($dm , $_ids);

        $attributes = ['type' => strtolower(ContentType::ADGROUP), 'teId' => $teIds];
        // Clean up error queue
        $dm->getRepository(ErrorsQueue::class)->removeByMixedAttributes($dm, $attributes);
    }

    /**
     * @param array $errors
     * @throws \MongoException
     */
    protected function registerErrors(array $errors)
    {
        if(empty($errors))
            return;

        $dm = $this->getDocumentManager();

        $_ids = array_map(function ($id){ return new ObjectId($id); }, array_keys($errors));

        /**@var AdgroupsQueue[] $adgroups*/
        $adgroups = $dm->getRepository(AdgroupsQueue::class)->getByIds($dm , $_ids);

        $deletedIds = [];
        foreach ($adgroups as $index => $adgroup) {
            $error = $errors[$adgroup->getId()];
            if (!in_array($error['Error Number'], ['1217', '1201'])) {
                $adgroup->setError($error['Error']);
                $adgroup->setErrorCode($error['Error Number']);
            } else {
                $deletedIds[] = $adgroup->getTeId();
                unset($adgroups[$index]);
            }
        }

        if (!empty($deletedIds)) {
            foreach (ContentType::CONTENT_TYPES as $contentType) {
                if ($contentType == ContentType::EXTENSION) {
                    continue;
                } elseif ($contentType == ContentType::ADGROUP) {
                    $attribute = ['teId' => $deletedIds];
                } else {
                    $attribute = ['teAdgroupId' => $deletedIds];
                }

                $collectionDocumentName = ProviderDocumentName::getMainCollectionByContentType($contentType);
                $dm->getRepository($collectionDocumentName)
                    ->removeByAttributesIn($dm, $attribute);

                $queueDocumentName = ProviderDocumentName::getQueueByContentType($contentType);
                $dm->getRepository($queueDocumentName)
                    ->removeByAttributes($dm, $attribute);
            }
        }

        $dm->flush();

        $counter = 1;
        foreach ($adgroups as $adgroup) {
            $campaignName = ProviderCampaignName::getCampaignName($dm, $this->getCache(), $adgroup->getCampaignId());
            $errorDetail = BingErrorDetail::errorDetail($adgroup->getErrorCode());
            $errorDetail = !empty($errorDetail) ? $errorDetail : $adgroup->getError();

            $adgroupError = new ErrorsQueue;

            $adgroupError->setType(strtolower(ContentType::ADGROUP));
            $adgroupError->setErrorElementId(new ObjectId($adgroup->getId()));
            $adgroupError->setRawError($adgroup->getError());
            $adgroupError->setBackendId($adgroup->getKcCampaignBackendId());
            $adgroupError->setError($errorDetail);
            $adgroupError->setCampaignName($campaignName);
            $adgroupError->setCampaignId(new ObjectId($adgroup->getCampaignId()));
            $adgroupError->setAdgroup($adgroup->getAdgroup());
            $adgroupError->setTeId($adgroup->getTeId());

            $dm->persist($adgroupError);

            $dm->detach($adgroup);
            unset($adgroup);

            if (($counter % 50) === 0) {
                $dm->flush();
                $dm->clear();
            }

            $counter++;
        }
        $dm->flush();
    }

    /**
     * @param $results $csvContentAsString
     * @param BatchJobInterface $hotsBatchJob
     * @throws
    */
    protected function processResults(array $results, BatchJobInterface $hotsBatchJob)
    {
        $queryEntitiesIds = $this->jsonDecodeMetaData($hotsBatchJob);

        if ($hotsBatchJob->getAction() == BingBatchJob::ACTION_ADD) {
            // Success add operations processing
            $this->processCollectionsAfterAdd($results['results']);
            // Register errors in queues
            $this->registerErrors($results['errors']);

        } elseif($hotsBatchJob->getAction() == BingBatchJob::ACTION_UPDATE) {
            // Success update operations processing
            $queryEntitiesIds = array_diff($queryEntitiesIds, array_keys($results['errors']));
            $this->processCollectionsAfterUpdate($queryEntitiesIds);
            // Register errors in queues
            $this->registerErrors($results['errors']);

        } elseif($hotsBatchJob->getAction() == BingBatchJob::ACTION_REMOVE) {
            // Success remove operations processing
            $queryEntitiesIds = array_diff($queryEntitiesIds, array_keys($results['errors']));
            $this->processCollectionsAfterRemove($queryEntitiesIds);
            // Register errors in queues
            $this->registerErrors($results['errors']);

        } else {
            throw new \Exception("Unknown action {$hotsBatchJob->getAction()} in processResults");
        }

        $this->getLogger()->info("Completed processing batch jobs results",
            [$hotsBatchJob->getOperandType(), $hotsBatchJob->getAction(),
                $hotsBatchJob->getId(), $hotsBatchJob->getSystemJobId()]);
    }
}
