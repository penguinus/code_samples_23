<?php

namespace App\Extensions\AdSystem\Bing\ExternalWork\Bulk;

use App\Document\ErrorsQueue;
use App\Document\KcCampaign;
use App\Document\Keyword;
use App\Document\KeywordsQueue;
use App\Entity\BingBatchJob;
use App\Entity\BrandTemplate;
use App\Extensions\AdSystem\Bing\ExternalWork\BingErrorDetail;
use App\Extensions\Common\AdSystemEnum;
use App\Extensions\Common\ContentType;
use App\Interfaces\DocumentRepositoryInterface\BatchItemsUploadInterface;
use App\Interfaces\EntityInterface\BatchJobInterface;
use App\Providers\ProviderCampaignName;
use App\Providers\ProviderDocumentName;
use MongoDB\BSON\ObjectId;

/**
 * Class BingKeywordManager
 * @package App\Extensions\AdSystem\Bing\ExternalWork\Bulk
 */
class BingKeywordManager extends BingBatchManager
{
    /**
     *
     */
    CONST BATCH_SIZE = 20000;
    /**
     *
     */
    const MAX_PARALLEL_JOBS_COUNT = 5;

    /**
     *
     */
    const OPERATION_FIELDS = array(
        "type" => "Type",
        "status" => "Status",
        "id" => "Id",
        "parentId" => "Parent Id",
        "clientId" => "Client Id",
        "keyword" => "Keyword",
        "matchType" => "Match Type",
        "finalUrl" => "Final Url",
        "bid" => "Bid",
        "name" => "Name"
    );

    /**
     * @return string
     */
    public function getOperandType(): string
    {
        return BingBatchJob::OPERAND_TYPE_KEYWORD;
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
        return $this->getDocumentManager()->getRepository(KeywordsQueue::class);
    }

    /**
     * @param $hots_entities
     * @param $customerId
     * @return array|bool
     */
    protected function buildAddOperations(array $hots_entities, $customerId)
    {
        ini_set("memory_limit", "3G");
        gc_enable();

        $dm = $this->getDocumentManager();
        $em = $this->getEntityManager();

        $backendIds = array_unique(array_column($hots_entities, 'kcCampaignBackendId'));
        $campaigns = $dm->getRepository(KcCampaign::class)
            ->getCampaignLocationInfoByBackendIds($dm, $backendIds);

        $brandTemplateIds = array_unique(array_column($hots_entities, 'brandTemplateId'));
        $channelIdByTemplateIds = $em->getRepository(BrandTemplate::class)
            ->getChannelIdsByIds($brandTemplateIds, AdSystemEnum::BING);

        $operations = [];
        $entitiesIds = [];

        $keyword = $this->initEmptyArrayByFields();
        foreach ($hots_entities as $hots_keyword) {

            if(!isset($hots_keyword['teAdgroupId']) && (!isset($hots_keyword['negative']) || !$hots_keyword['negative'])) {
                print "ERROR! On Campaign level can be only NEGATIVE keywords.";

                continue;
            }

            if (isset($campaigns[(string)$hots_keyword['campaignId']])) {
                $campaign = $campaigns[(string)$hots_keyword['campaignId']];
            } else {
                $this->getQueryRepository()->removeByCampaignId($dm, $hots_keyword['campaignId']);
                return false;
            }

            $keyword['clientId'] = (string)$hots_keyword['_id'];

            // Negative or non negative
            if (isset($hots_keyword['negative']) && $hots_keyword['negative']) {
                if (!isset($hots_keyword['teAdgroupId'])) {
                    $keyword['type'] = "Campaign Negative Keyword";
                    $keyword['parentId'] = $hots_keyword['systemCampaignId'];
                } else {
                    $keyword['type'] = "Ad Group Negative Keyword";
                    $keyword['parentId'] = $hots_keyword['systemAdgroupId'];
                }
            } else {
                $keyword['type'] = "Keyword";
                $keyword['parentId'] = $hots_keyword['systemAdgroupId'];
            }

            $keywordText = $em->getRepository('App:BrandKeyword')
                ->processKeywordReplacements($hots_keyword['keyword'], $campaign['city'], $campaign['state']);

            $keyword['keyword'] = $keywordText;

            $kwdMatchType = $this->getQueryRepository()->getSystemMatchType($dm, $hots_keyword);
            if (!$kwdMatchType)
                continue;

            $keyword['matchType'] = $hots_keyword['matchType'];

            if (!isset($hots_keyword['negative']) || (isset($hots_keyword['negative']) && !$hots_keyword['negative']) ){

                $keyword['status'] = $hots_keyword['status'] == 1 ? 'Active' : 'Paused';

                if (isset($hots_keyword['destinationUrl'])) {

                    $finalUrl = $em->getRepository('App:BrandKeyword')
                        ->makeDestinationUrl($hots_keyword['destinationUrl'],
                            $hots_keyword['kcCampaignBackendId'],
                            $channelIdByTemplateIds[$hots_keyword['brandTemplateId']],
                            $hots_keyword['systemAdgroupId'],
                            $campaign['cityId']);

                    $keyword['finalUrl'] = $finalUrl;
                }
                // Set bids (optional).
                if (isset($hots_keyword['maxCpc']))
                    if ($hots_keyword['maxCpc'] != "" && $hots_keyword['maxCpc'] != "0" && $hots_keyword['maxCpc'] != 0) {
                        $keyword['bid'] = $hots_keyword['maxCpc'];
                    }
            }

            $operations[] = $keyword;
            $entitiesIds[] = (string)$hots_keyword['_id'];
        }
        unset($campaigns, $hots_entities);

        return [$operations, $entitiesIds];
    }

    /**
     * @param array $hots_entities
     * @param $customerId
     * @return array|bool
     */
    protected function buildUpdateOperations(array $hots_entities, $customerId)
    {
        ini_set("memory_limit", "3G");
        gc_enable();

        $dm = $this->getDocumentManager();
        $em = $this->getEntityManager();

        $backendIds = array_unique(array_column($hots_entities, 'kcCampaignBackendId'));
        $campaigns = $dm->getRepository(KcCampaign::class)
            ->getCampaignLocationInfoByBackendIds($dm, $backendIds);

        $brandTemplateIds = array_unique(array_column($hots_entities, 'brandTemplateId'));
        $channelIdByTemplateIds = $em->getRepository(BrandTemplate::class)
            ->getChannelIdsByIds($brandTemplateIds, AdSystemEnum::BING);

        $operations = [];
        $entitiesIds = [];

        $keyword = $this->initEmptyArrayByFields();
        foreach ($hots_entities as $hots_keyword) {
            if (isset($campaigns[(string)$hots_keyword['campaignId']])) {
                $campaign = $campaigns[(string)$hots_keyword['campaignId']];
            } else {
                $this->getQueryRepository()->removeByCampaignId($dm, $hots_keyword['campaignId']);
                return false;
            }

            $keyword['clientId'] = (string)$hots_keyword['_id'];

            // Negative or non negative
            if (isset($hots_keyword['negative']) && $hots_keyword['negative']) {
                if (!isset($hots_keyword['teAdgroupId'])) {
                    $keyword['type'] = "Campaign Negative Keyword";
                    $keyword['parentId'] = $hots_keyword['systemCampaignId'];
                } else {
                    $keyword['type'] = "Ad Group Negative Keyword";
                    $keyword['parentId'] = $hots_keyword['systemAdgroupId'];
                }
            } else {
                $keyword['type'] = "Keyword";
                $keyword['parentId'] = $hots_keyword['systemAdgroupId'];
            }

            $keyword['id'] = $hots_keyword['systemKeywordId'];

            $keywordText = $em->getRepository('App:BrandKeyword')
                ->processKeywordReplacements($hots_keyword['keyword'], $campaign['city'], $campaign['state']);

            $keyword['keyword'] = $keywordText;

            $kwdMatchType = $this->getQueryRepository()->getSystemMatchType($dm, $hots_keyword);
            if (!$kwdMatchType)
                continue;

            $keyword['matchType'] = $hots_keyword['matchType'];

            if (!isset($hots_keyword['negative']) || !$hots_keyword['negative']) {
                $keyword['status'] = $hots_keyword['status'] == 1 ? 'Active' : 'Paused';

                if (isset($hots_keyword['destinationUrl'])) {

                    $finalUrl = $em->getRepository('App:BrandKeyword')
                        ->makeDestinationUrl($hots_keyword['destinationUrl'],
                            $hots_keyword['kcCampaignBackendId'],
                            $channelIdByTemplateIds[$hots_keyword['brandTemplateId']],
                            $hots_keyword['systemAdgroupId'],
                            $campaign['cityId']);

                    $keyword['finalUrl'] = $finalUrl;
                }
                // Set bids (optional).
                if (isset($hots_keyword['maxCpc']))
                    if ($hots_keyword['maxCpc'] != "" && $hots_keyword['maxCpc'] != "0" && $hots_keyword['maxCpc'] != 0) {
                        $keyword['bid'] = $hots_keyword['maxCpc'];
                    }
            }

            $operations[] = $keyword;
            $entitiesIds[] = (string)$hots_keyword['_id'];
        }
        unset($campaigns, $hots_entities);

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

        $keyword = $this->initEmptyArrayByFields();
        foreach ($hots_entities as $hots_keyword) {

            $keyword['clientId'] = (string)$hots_keyword['_id'];

            // Negative or non negative
            if (isset($hots_keyword['negative']) && $hots_keyword['negative']) {
                if (!isset($hots_keyword['teAdgroupId'])) {
                    $keyword['type'] = "Campaign Negative Keyword";
                    $keyword['parentId'] = $hots_keyword['systemCampaignId'];
                } else {
                    if(!isset($hots_keyword['systemAdgroupId']))
                        continue;

                    $keyword['type'] = "Ad Group Negative Keyword";
                    $keyword['parentId'] = $hots_keyword['systemAdgroupId'];
                }
            } else {
                if(!isset($hots_keyword['systemAdgroupId']))
                    continue;

                $keyword['type'] = "Keyword";
                $keyword['parentId'] = $hots_keyword['systemAdgroupId'];
            }

            $keyword['id'] = $hots_keyword['systemKeywordId'];
            $keyword['status'] = 'Deleted';

            $operations[] = $keyword;
            $entitiesIds[] = (string)$hots_keyword['_id'];
        }

        return [$operations, $entitiesIds];
    }

    /**
     * @param BatchJobInterface $hotsBatchJob
     * @param array|null        $batchJobResults
     */
    protected function failedResultProcessingFallback(BatchJobInterface $hotsBatchJob, array $batchJobResults = null)
    {
        // See Adwords realisation
    }

    /**
     * @param array $resultSuccesses
     * @throws \MongoException
     * @throws \Doctrine\ODM\MongoDB\MongoDBException
     */
    public function processCollectionsAfterAdd(array $resultSuccesses)
    {
        if (empty($resultSuccesses))
            return;

        ini_set("memory_limit", "7G");

        $dm = $this->getDocumentManager();

        $keywordsByTemplate = [];
        $keywordData = [];
        $campaignIds = [];
        $teIds = [];

        $_ids = array_map(function ($id) {
            return new ObjectId($id);
        }, array_keys($resultSuccesses));

        $selectFields = ['id', 'brandTemplateId', 'campaignId', 'teId', 'teAdgroupId',
            'kcCampaignBackendId', 'systemCampaignId', 'systemAdgroupId'];
        $keywords = $dm->getRepository(KeywordsQueue::class)->getByIds($dm, $_ids, $selectFields, true);

        foreach ($keywords as $keyword) {
            $campaignIds[] = new ObjectId((string)$keyword['campaignId']);
            $teIds[] = $keyword['teId'];

            $keywordsByTemplate[$keyword['brandTemplateId']][] = $keyword;
            $keywordData[(string)$keyword['campaignId']][$keyword['teId']] = $resultSuccesses[(string)$keyword['_id']];
        }
        unset($keywords);

        $campaignIds = array_unique($campaignIds);
        $teIds = array_unique($teIds);

        $this->setSystemIdToKeywordsQueue($keywordData, $campaignIds, $teIds);
        unset($keywordData, $campaignIds, $teIds);

        $this->addItemsToMainCollections($keywordsByTemplate, $resultSuccesses);
        unset($keywordsByTemplate, $resultSuccesses);

        $dm->getRepository(KeywordsQueue::class)->removeByIds($dm, $_ids);
    }

    /**
     * @param array $keywordData
     * @param array $campaignIds
     * @param array $teIds
     * @throws \Doctrine\ODM\MongoDB\MongoDBException
     */
    protected function setSystemIdToKeywordsQueue(array $keywordData, array $campaignIds, array $teIds)
    {
        $dm = $this->getDocumentManager();

        /**@var KeywordsQueue[] $keywords */
        $keywords = $dm->createQueryBuilder(KeywordsQueue::class)
            ->field('campaignId')->in($campaignIds)
            ->field('teId')->in($teIds)
            ->field('add')->exists(false)
            ->immortal()// no limit of time for cursor lifetime
            ->getQuery()
            ->execute();

        foreach ($keywords as $keyword) {
            if (isset($keywordData[$keyword->getCampaignId()][$keyword->getTeId()]))
                $keyword->setSystemKeywordId($keywordData[$keyword->getCampaignId()][$keyword->getTeId()]);
        }
        $dm->flush();

        foreach ($keywords as $keyword) {
            $dm->detach($keyword);
            unset($keyword);
        }
        $dm->flush();
        gc_collect_cycles();
    }

    /**
     * @param array $keywordsByTemplate
     * @param array $resultSuccesses
     */
    protected function addItemsToMainCollections(array $keywordsByTemplate, array $resultSuccesses)
    {
        $dm = $this->getDocumentManager();

        $counter = 1;
        foreach ($keywordsByTemplate as $templateId => $keywords) {
            $dm->setBrandTemplateId($templateId);

            foreach ($keywords as $keyword) {
                $hotsKeyword = new \App\Document\Keyword();
                $hotsKeyword->setTeAdgroupId(
                    isset($keyword['teAdgroupId']) ? isset($keyword['teAdgroupId']) : null);
                $hotsKeyword->setTeId($keyword['teId']);
                $hotsKeyword->setKcCampaignBackendId($keyword['kcCampaignBackendId']);
                $hotsKeyword->setSystemCampaignId(
                    isset($keyword['systemCampaignId']) ? $keyword['systemCampaignId'] : null);
                $hotsKeyword->setSystemAdgroupId(
                    isset($keyword['systemAdgroupId']) ? $keyword['systemAdgroupId'] : null);
                $hotsKeyword->setSystemKeywordId($resultSuccesses[(string)$keyword['_id']]);

                $dm->persist($hotsKeyword);

                if (($counter % 100) === 0) {
                    $dm->flush();
                    $dm->clear();
                }

                $counter++;
            }
            $dm->flush();
        }
    }

    /**
     * @param array $ids
     */
    protected function processCollectionsAfterUpdate(array $ids)
    {
        if (empty($ids))
            return;

        $dm = $this->getDocumentManager();

        $_ids = array_map(function ($id) {
            return new ObjectId($id);
        }, $ids);
        // Remove keyword from Queue
        $dm->getRepository(KeywordsQueue::class)->removeByIds($dm, $_ids);
    }

    /**
     * @param array $ids
     */
    protected function processCollectionsAfterRemove(array $ids)
    {
        if (empty($ids))
            return;

        $dm = $this->getDocumentManager();

        $_ids = array_map(function ($id) {
            return new ObjectId($id);
        }, $ids);

        $selectFields = ['brandTemplateId', 'systemKeywordId'];
        /**@var KeywordsQueue[] $keywords */
        $keywords = $dm->getRepository(KeywordsQueue::class)->getByIds($dm, $_ids, $selectFields, true);

        $systemIdsByTemplate = [];

        foreach ($keywords as $keyword) {
            $systemIdsByTemplate[$keyword['brandTemplateId']] [] = $keyword['systemKeywordId'];
        }

        foreach ($systemIdsByTemplate as $templateId => $systemIds) {
            $dm->setBrandTemplateId($templateId);
            $dm->getRepository(Keyword::class)->removeBySystemIds($dm, $systemIds);
        }

        // Remove keyword from Queue
        $dm->getRepository(KeywordsQueue::class)->removeByIds($dm, $_ids);
    }

    /**
     * @param array $errors
     * @throws \MongoException
     */
    protected function registerErrors(array $errors)
    {
        if (empty($errors))
            return;

        $dm = $this->getDocumentManager();

        $_ids = array_map(function ($id) {
            return new ObjectId($id);
        }, array_keys($errors));

        /**@var KeywordsQueue[] $keywords */
        $keywords = $dm->getRepository(KeywordsQueue::class)->getByIds($dm, $_ids);

        $deletedIds = [];
        foreach ($keywords as $index => $keyword) {
            $error = $errors[$keyword->getId()];
            if (!in_array($error['Error Number'], ['1501', '4330', '1005'])) {
                $keyword->setError($error['Error']);
                $keyword->setErrorCode($error['Error Number']);
            } else {
                $deletedIds[] = $keyword->getTeId();
                unset($keywords[$index]);
            }
        }

        if (!empty($deletedIds)) {
            $collectionDocumentName = ProviderDocumentName::getMainCollectionByContentType(ContentType::AD);
            $dm->getRepository($collectionDocumentName)
                ->removeByAttributesIn($dm, ['teId' => $deletedIds]);

            $queueDocumentName = ProviderDocumentName::getQueueByContentType(ContentType::AD);
            $dm->getRepository($queueDocumentName)
                ->removeByAttributes($dm, ['teId' => $deletedIds]);
        }

        $dm->flush();

        $counter = 1;
        foreach ($keywords as $keyword) {
            $errorDetail = BingErrorDetail::errorDetail($keyword->getErrorCode());
            $errorDetail = !empty($errorDetail) ? $errorDetail : $keyword->getError();

            $keywordError = new ErrorsQueue;
            $campaignName = ProviderCampaignName::getCampaignName($dm, $this->getCache(), $keyword->getCampaignId());

            $keywordError->setType(strtolower(ContentType::KEYWORD));
            $keywordError->setErrorElementId(new ObjectId($keyword->getId()));
            $keywordError->setRawError($keyword->getError());
            $keywordError->setCampaignName($campaignName);
            $keywordError->setBackendId($keyword->getKcCampaignBackendId());
            $keywordError->setError($errorDetail);
            $keywordError->setCampaignId(new ObjectId($keyword->getCampaignId()));
            $keywordError->setAdgroup($keyword->getAdgroup());
            $keywordError->setKeyword($keyword->getKeyword());
            $keywordError->setTeId($keyword->getTeId());
            $keywordError->setTeAdgroupId($keyword->getTeAdgroupId());

            $dm->persist($keywordError);

            $dm->detach($keyword);
            unset($keyword);

            if (($counter % 50) === 0) {
                $dm->flush();
                $dm->clear();
            }

            $counter++;
        }
        $dm->flush();
    }

    /**
     * @param array $results
     * @param BingBatchJob $hotsBatchJob
     * @throws \Exception
     */
    protected function processResults($results, BatchJobInterface $hotsBatchJob)
    {
        $queryEntitiesIds = $this->jsonDecodeMetaData($hotsBatchJob);

        if ($hotsBatchJob->getAction() == BingBatchJob::ACTION_ADD) {
            // Success add operations processing
            $this->processCollectionsAfterAdd($results['results']);
            // Register errors in queues
            $this->registerErrors($results['errors']);

        } elseif ($hotsBatchJob->getAction() == BingBatchJob::ACTION_UPDATE) {
            // Success update operations processing
            $queryEntitiesIds = array_diff($queryEntitiesIds, array_keys($results['errors']));
            $this->processCollectionsAfterUpdate($queryEntitiesIds);
            // Register errors in queues
            $this->registerErrors($results['errors']);

        } elseif ($hotsBatchJob->getAction() == BingBatchJob::ACTION_REMOVE) {
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