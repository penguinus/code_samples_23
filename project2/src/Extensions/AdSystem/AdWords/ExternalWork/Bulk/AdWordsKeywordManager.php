<?php

namespace App\Extensions\AdSystem\AdWords\ExternalWork\Bulk;

use App\Document\{ErrorsQueue, KcCampaign, Keyword, KeywordsQueue};
use App\Entity\BrandKeyword;
use App\Extensions\AdSystem\AdWords\ExternalWork\AdWordsErrorDetail;
use App\Extensions\Common\{AdSystemEnum, ContentType};
use App\Extensions\Common\ExternalWork\Bulk\BatchJob;
use App\Interfaces\DocumentRepositoryInterface\BatchItemsUploadInterface;
use App\Interfaces\EntityInterface\BatchJobInterface;
use App\Providers\ProviderCampaignName;
use Doctrine\ODM\MongoDB\MongoDBException;
use Google\Ads\GoogleAds\Util\FieldMasks;
use Google\Ads\GoogleAds\Util\V13\ResourceNames;
use Google\ApiCore\ApiException;
use Google\Ads\GoogleAds\V13\Common\{KeywordInfo, PolicyViolationKey};
use Google\Ads\GoogleAds\V13\Enums\AdGroupCriterionStatusEnum\AdGroupCriterionStatus;
use Google\Ads\GoogleAds\V13\Resources\{AdGroupCriterion, CampaignCriterion};
use Google\Ads\GoogleAds\V13\Services\{AdGroupCriterionServiceClient, CampaignCriterionServiceClient, GoogleAdsRow,
    MutateOperation, AdGroupCriterionOperation, CampaignCriterionOperation, MutateOperationResponse};
use Google\ApiCore\ValidationException;
use MongoDB\BSON\ObjectId;
use Psr\Cache\InvalidArgumentException;

/**
 *
 */
class AdWordsKeywordManager extends AdWordsBatchManager
{
    public const MAX_PARALLEL_JOBS_COUNT = 8;

    /**
     * @return string
     */
    public function getOperandType(): string
    {
        return BatchJob::OPERAND_TYPE_KEYWORD;
    }

    /**
     * @return BatchItemsUploadInterface
     */
    public function getQueryRepository(): BatchItemsUploadInterface
    {
        return $this->getDocumentManager()->getRepository(KeywordsQueue::class);
    }

    /**
     * @param array $hots_entities
     * @param       $customerId
     *
     * @return array[]|false
     * @throws MongoDBException
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

        $criterionOperations = [];
        $entitiesIds = [];
        foreach ($hots_entities as $hots_keyword) {
            if (!isset($hots_keyword['teAdgroupId']) && (!isset($hots_keyword['negative']) || !$hots_keyword['negative'])) {
                print "ERROR! On Campaign level can be only NEGATIVE keywords.";

                continue;
            }

            if (isset($campaigns[(string)$hots_keyword['campaignId']])) {
                $campaign = $campaigns[(string)$hots_keyword['campaignId']];
            } else {
                $this->getQueryRepository()->removeByCampaignId($dm , $hots_keyword['campaignId']);

                return false;
            }

            $kwdMatchType = $this->getQueryRepository()->getSystemMatchType($dm, $hots_keyword);
            if (!$kwdMatchType) {
                continue;
            }

            $keywordText = $em->getRepository(BrandKeyword::class)
                ->processKeywordReplacements($hots_keyword['keyword'], $campaign['city'], $campaign['state']);

            $keywordInfo = new KeywordInfo();
            $keywordInfo->setText($keywordText);
            $keywordInfo->setMatchType($kwdMatchType);

            if (isset($hots_keyword['negative']) && $hots_keyword['negative'] && !isset($hots_keyword['teAdgroupId'])) {
                // Constructs a campaign criterion using the keyword text info above.
                $campaignCriterion = new CampaignCriterion();
                $campaignCriterion->setCampaign(ResourceNames::forCampaign($customerId, $hots_keyword['systemCampaignId']));
                $campaignCriterion->setKeyword($keywordInfo);
                $campaignCriterion->setNegative(true);

                $criterionOperationType = new CampaignCriterionOperation();
                $criterionOperationType->setCreate($campaignCriterion);
            } else {
                // Constructs an ad group criterion using the keyword text info above.
                $adGroupCriterion = new AdGroupCriterion();
                $adGroupCriterion->setAdGroup(ResourceNames::forAdGroup($customerId, $hots_keyword['systemAdgroupId']));
                $adGroupCriterion->setStatus(
                    isset($hots_keyword['status']) && $hots_keyword['status'] ?
                        AdGroupCriterionStatus::ENABLED : AdGroupCriterionStatus::PAUSED);
                $adGroupCriterion->setKeyword($keywordInfo);

                // Set bids (optional).
                if (isset($hots_keyword['maxCpc']) && !empty($hots_keyword['maxCpc']) && $hots_keyword['maxCpc'] != 0) {
                    $adGroupCriterion->setCpcBidMicros($hots_keyword['maxCpc'] * 1000000);
                }

                if (isset($hots_keyword['negative']) && $hots_keyword['negative']) {
                    $adGroupCriterion->setNegative(true);
                }

                $criterionOperationType = new AdGroupCriterionOperation();
                $criterionOperationType->setCreate($adGroupCriterion);
            }

            if (isset($hots_keyword['policyErrors'])) {
                $policyViolationKeys = [];
                foreach ($hots_keyword['policyErrors'] as $error) {
                    $policyViolationKey = new PolicyViolationKey();
                    $policyViolationKey->setPolicyName($error['policyName']);
                    $policyViolationKey->setViolatingText($error['violatingText']);

                    $policyViolationKeys[] = $policyViolationKey;
                }

                $criterionOperationType->setExemptPolicyViolationKeys($policyViolationKeys);

                $dm->createQueryBuilder(ErrorsQueue::class)->remove()
                    ->field('extensionElementId')->equals((string)$hots_keyword['_id'])
                    ->getQuery()
                    ->execute();
            }

            // Issues a mutate operation to add the keyword criteria.
            $mutateOperation = new MutateOperation();

            if (isset($hots_keyword['negative']) && $hots_keyword['negative'] && !isset($hots_keyword['teAdgroupId'])) {
                $criterionOperations[] = $mutateOperation->setCampaignCriterionOperation($criterionOperationType);
            } else {
                $criterionOperations[] = $mutateOperation->setAdGroupCriterionOperation($criterionOperationType);
            }

            $entitiesIds[] = (string)$hots_keyword['_id'];
        }

        unset($campaigns, $hots_entities);

        return [$criterionOperations, $entitiesIds];
    }

    /**
     * @param array $hots_entities
     * @param       $customerId
     *
     * @return array|bool
     */
    protected function buildUpdateOperations(array $hots_entities, $customerId)
    {
        ini_set("memory_limit", "3G");
        gc_enable();

        $dm = $this->getDocumentManager();

        $backendIds = array_unique(array_column($hots_entities, 'kcCampaignBackendId'));
        $campaigns = $dm->getRepository(KcCampaign::class)
            ->getCampaignLocationInfoByBackendIds($dm, $backendIds);

        $criterionOperations = [];
        $entitiesIds = [];
        foreach ($hots_entities as $hots_keyword) {
            if (!isset($campaigns[(string)$hots_keyword['campaignId']])) {
                $this->getQueryRepository()->removeByCampaignId($dm , $hots_keyword['campaignId']);

                return false;
            }

            if (isset($hots_keyword['negative']) && $hots_keyword['negative'] && !isset($hots_keyword['teAdgroupId'])) {
                // Creates a campaign criterion with the proper resource name and any other changes.
                $resourceName = ResourceNames::forCampaignCriterion(
                    $customerId,
                    $hots_keyword['systemCampaignId'],
                    $hots_keyword['systemKeywordId']
                );

                // Constructs a campaign criterion using the keyword text info above.
                $campaignCriterion = new CampaignCriterion();
                $campaignCriterion->setResourceName($resourceName);

                // Constructs an operation that will update the ad group criterion, using the FieldMasks
                // utility to derive the update mask. This mask tells the Google Ads API which attributes of
                // the campaign criterion you want to change.
                $criterionOperation = new CampaignCriterionOperation();
                $criterionOperation->setUpdate($campaignCriterion);
                $criterionOperation->setUpdateMask(FieldMasks::allSetFieldsOf($campaignCriterion));
            } else {
                // Creates an ad group criterion with the proper resource name and any other changes.
                $resourceName = ResourceNames::forAdGroupCriterion(
                    $customerId,
                    $hots_keyword['systemAdgroupId'],
                    $hots_keyword['systemKeywordId']
                );

                // Constructs an ad group criterion using the keyword text info above.
                $adGroupCriterion = new AdGroupCriterion();
                $adGroupCriterion->setResourceName($resourceName);
                $adGroupCriterion->setStatus(
                    $hots_keyword['status'] ? AdGroupCriterionStatus::ENABLED : AdGroupCriterionStatus::PAUSED);

                // Set bids (optional).
                if (isset($hots_keyword['maxCpc']) && !empty($hots_keyword['maxCpc']) && $hots_keyword['maxCpc'] != 0) {
                    $adGroupCriterion->setCpcBidMicros($hots_keyword['maxCpc'] * 1000000);
                }

                // Constructs an operation that will update the ad group criterion, using the FieldMasks
                // utility to derive the update mask. This mask tells the Google Ads API which attributes of
                // the ad group criterion you want to change.
                $criterionOperation = new AdGroupCriterionOperation();
                $criterionOperation->setUpdate($adGroupCriterion);
                $criterionOperation->setUpdateMask(FieldMasks::allSetFieldsOf($adGroupCriterion));
            }

            // Issues a mutate operation to add the keyword criteria.
            $mutateOperation = new MutateOperation();

            if (isset($hots_keyword['negative']) && $hots_keyword['negative'] && !isset($hots_keyword['teAdgroupId'])) {
                $criterionOperations[] = $mutateOperation->setCampaignCriterionOperation($criterionOperation);
            } else {
                $criterionOperations[] = $mutateOperation->setAdGroupCriterionOperation($criterionOperation);
            }

            $entitiesIds[] = (string)$hots_keyword['_id'];
        }

        unset($campaigns, $hots_entities);
        
        return [$criterionOperations, $entitiesIds];
    }

    /**
     * @param array $hots_entities
     * @param       $customerId
     *
     * @return array
     */
    protected function buildDeleteOperations(array $hots_entities, $customerId): array
    {
        $criterionOperations = [];
        $entitiesIds = [];

        foreach ($hots_entities as $hots_keyword) {
            // Constructs an operation that will remove the keyword with the specified resource name.
            if (isset($hots_keyword['negative']) && $hots_keyword['negative'] && !isset($hots_keyword['teAdgroupId'])) {
                // Creates a campaign criterion with the proper resource name and any other changes.
                $resourceName = ResourceNames::forCampaignCriterion(
                    $customerId,
                    $hots_keyword['systemCampaignId'],
                    $hots_keyword['systemKeywordId']
                );

                // Constructs an operation that will remove the keyword with the specified resource name.
                $criterionOperationType = new CampaignCriterionOperation();
                $criterionOperationType->setRemove($resourceName);

                // Issues a mutate request to remove the campaign criterion.
                $mutateOperation = new MutateOperation();
                $criterionOperations[] = $mutateOperation->setCampaignCriterionOperation($criterionOperationType);
            } else {
                // Creates an ad group criterion with the proper resource name and any other changes.
                $resourceName = ResourceNames::forAdGroupCriterion(
                    $customerId,
                    $hots_keyword['systemAdgroupId'],
                    $hots_keyword['systemKeywordId']
                );

                // Constructs an operation that will remove the keyword with the specified resource name.
                $criterionOperationType = new AdGroupCriterionOperation();
                $criterionOperationType->setRemove($resourceName);

                // Issues a mutate request to remove the ad group criterion.
                $mutateOperation = new MutateOperation();
                $criterionOperations[] = $mutateOperation->setAdGroupCriterionOperation($criterionOperationType);
            }

            $entitiesIds[] = (string)$hots_keyword['_id'];
        }

        return [$criterionOperations, $entitiesIds];
    }

    /**
     * @param BatchJobInterface $hotsBatchJob
     * @param array $batchJobResults
     *
     * @return bool
     * @throws ApiException|ValidationException|MongoDBException
     */
    protected function failedResultProcessingFallback(BatchJobInterface $hotsBatchJob, array $batchJobResults): bool
    {
        $googleAdsServiceClient = $this->getGoogleServiceManager()->getGoogleAdsServiceClient();

        $keywordQueueIds = array_map(function ($id) {
            return new ObjectId($id);
        }, $this->jsonDecodeMetaData($hotsBatchJob));

        $update_data = [];
        $missedCriterionIds = [];
        foreach ($batchJobResults['results'] as $operationIndex => $operationResponse) {
            /** @var MutateOperationResponse $operationResponse */
            if ($operationResponse->hasAdGroupCriterionResult()) {
                if ($operationResponse->getAdGroupCriterionResult()->getResourceName() != null) {
                    $update_data[$keywordQueueIds[$operationIndex]] = AdGroupCriterionServiceClient::parseName(
                        $operationResponse->getAdGroupAdResult()->getResourceName()
                    )['criterion_id'];
                } else {
                    $missedCriterionIds[] = $keywordQueueIds[$operationIndex];
                }
            }

            if ($operationResponse->hasCampaignCriterionResult()) {
                if ($operationResponse->getCampaignCriterionResult()->getResourceName() != null) {
                    $update_data[$keywordQueueIds[$operationIndex]] = CampaignCriterionServiceClient::parseName(
                        $operationResponse->getAdGroupAdResult()->getResourceName()
                    )['criterion_id'];
                } else {
                    $missedCriterionIds[] = $keywordQueueIds[$operationIndex];
                }
            }
        }

        /** @var KeywordsQueue[] $keywordsQueue */
        $keywordsQueue = $this->getDocumentManager()->createQueryBuilder(KeywordsQueue::class)
            ->field('id')->in($missedCriterionIds ?: $keywordQueueIds)
            ->field('systemCampaignId')->exists(true)
            ->getQuery()
            ->execute();

        $criterionBySystemCampaignIds = [];
        $systemCampaignIds = [];
        foreach ($keywordsQueue as $keyword) {
            $systemCampaignIds[] = $keyword->getSystemCampaignId();
            $criterionBySystemCampaignIds[$keyword->getSystemCampaignId()][] = $keyword;
        }

        if (!empty($systemCampaignIds)) {
            // Creates a query that retrieves keywords.
            $queryForAdGroupCriterion = sprintf(/** @lang text */
                "SELECT ad_group.id, ad_group_criterion.criterion_id 
                FROM ad_group_criterion 
                WHERE ad_group_criterion.type = KEYWORD 
                AND ad_group_criterion.status != REMOVED 
                AND campaign.id IN (%s)",
                implode(', ', $systemCampaignIds)
            );

            // Issues a search request by specifying page size.
            $adGroupCriterionResponse = $googleAdsServiceClient->search(
                $hotsBatchJob->getSystemAccount()->getSystemAccountId(),
                $queryForAdGroupCriterion,
                ['pageSize' => $this->getGoogleServiceManager()::PAGE_SIZE]
            );

            if (!empty($adGroupCriterionResponse->iterateAllElements())) {
                // Iterates over all rows in all pages and prints the requested field values for
                // the ad group in each row.
                /** @var GoogleAdsRow $googleAdsRow */
                foreach ($adGroupCriterionResponse->iterateAllElements() as $googleAdsRow) {
                    /** @var KeywordsQueue[] $keywordsQueue */
                    $keywordsQueue = $criterionBySystemCampaignIds[$googleAdsRow->getCampaign()->getId()];

                    foreach ($keywordsQueue as $keyword) {
                        if ($keyword->getSystemAdgroupId() == $googleAdsRow->getAdGroup()->getId()) {
                            $update_data[(string)$keyword->getId()] = $googleAdsRow->getAdGroupCriterion()->getCriterionId();
                        }
                    }
                }
            }

            $queryForCampaignCriterion = sprintf(/** @lang text */
                "SELECT campaign.id, campaign_criterion.negative, campaign_criterion.criterion_id 
            FROM campaign_criterion 
            WHERE campaign_criterion.type = KEYWORD 
            AND campaign_criterion.status != REMOVED 
            AND campaign.id IN (%s)",
                implode(', ', $systemCampaignIds)
            );

            // Issues a search request by specifying page size.
            $campaignCriterionResponse = $googleAdsServiceClient->search(
                $hotsBatchJob->getSystemAccount()->getSystemAccountId(),
                $queryForCampaignCriterion,
                ['pageSize' => $this->getGoogleServiceManager()::PAGE_SIZE]
            );

            if (!empty($campaignCriterionResponse->iterateAllElements())) {
                // Iterates over all rows in all pages and prints the requested field values for
                // the ad group in each row.
                /** @var GoogleAdsRow $googleAdsRow */
                foreach ($campaignCriterionResponse->iterateAllElements() as $googleAdsRow) {
                    if (!$googleAdsRow->getCampaignCriterion()->hasNegative()) {
                        continue;
                    }

                    /** @var KeywordsQueue[] $keywordsQueue */
                    $keywordsQueue = $criterionBySystemCampaignIds[$googleAdsRow->getCampaign()->getId()];
                    foreach ($keywordsQueue as $keyword) {
                        if ($keyword->getSystemCampaignId() == $googleAdsRow->getCampaign()->getId() && $keyword->getNegative()) {
                            $update_data[(string)$keyword->getId()] = $googleAdsRow->getCampaignCriterion()->getCriterionId();
                        }
                    }
                }
            }
        }

        $this->processCollectionsAfterAdd($update_data);

        $this->getLogger()->info(
            "Restored " . count($update_data) . " keywords after batch job failure",
            [$hotsBatchJob->getId(), $hotsBatchJob->getSystemJobId()]
        );

        return count($keywordQueueIds) == (count($update_data) + count($batchJobResults['errors']));
    }

    /**
     * @param array $update_data
     *
     * @throws MongoDBException
     */
    public function processCollectionsAfterAdd(array $update_data)
    {
        ini_set("memory_limit", "30G");

        $dm = $this->getDocumentManager();

        $ids = [];
        foreach ($update_data as $id => $system_keyword_id) {
            $ids[] = new ObjectId($id);
        }

        /** @var KeywordsQueue[] $keywords */
        $keywords = $dm->createQueryBuilder(KeywordsQueue::class)
            ->field('id')->in($ids)
            ->getQuery()
            ->execute();

        $keywordsByTemplate = [];
        $campaignIds = [];
        foreach ($keywords as $keyword) {
            $campaignIds[] = new ObjectId($keyword->getCampaignId());
            $keywordsByTemplate[$keyword->getBrandTemplateId()][] = $keyword;
            $keywordData[$keyword->getCampaignId()][$keyword->getTeId()] = $update_data[$keyword->getId()];
        }
        unset($keywords);

        $campaignIds = array_unique($campaignIds);

        /** @var KeywordsQueue[] $keywords */
        $keywords = $dm->createQueryBuilder(KeywordsQueue::class)
            ->field('campaignId')->in($campaignIds)
            ->field('add')->exists(false)
            ->getQuery()
            ->execute();

        foreach ($keywords as $keyword) {
            if (isset($keywordData[$keyword->getCampaignId()][$keyword->getTeId()])) {
                $keyword->setSystemKeywordId($keywordData[$keyword->getCampaignId()][$keyword->getTeId()]);
            }
        }

        $dm->flush();

        foreach ($keywords as $keyword){
            $dm->detach($keyword);

            unset($keyword);
        }

        $dm->flush();

        gc_collect_cycles();

        $counter = 1;
        foreach ($keywordsByTemplate as $templateId => $keywords) {
            $dm->setBrandTemplateId($templateId);

            foreach ($keywords as $keyword){
                $hotsKeyword = new Keyword();
                $hotsKeyword->setTeAdgroupId($keyword->getTeAdgroupId());
                $hotsKeyword->setTeId($keyword->getTeId());
                $hotsKeyword->setKcCampaignBackendId($keyword->getKcCampaignBackendId());
                $hotsKeyword->setSystemCampaignId($keyword->getSystemCampaignId());
                $hotsKeyword->setSystemAdgroupId($keyword->getSystemAdgroupId());
                $hotsKeyword->setSystemKeywordId($update_data[$keyword->getId()]);

                $dm->persist($hotsKeyword);

                if (($counter % 100) === 0) {
                    $dm->flush();
                    $dm->clear();
                }

                $counter++;
            }

            $dm->flush();
        }

        $dm->createQueryBuilder(KeywordsQueue::class)
            ->remove()
            ->field('id')->in($ids)
            ->getQuery()
            ->execute();
    }

    /**
     * @param array $update_data
     */
    protected function processCollectionsAfterUpdate(array $update_data)
    {
        $dm = $this->getDocumentManager();
        $dm->setAdSystem(AdSystemEnum::ADWORDS);

        $ids = [];
        foreach ($update_data as $id => $system_keyword_id) {
            $ids[] = new ObjectId($id);
        }

        // Remove keyword from Queue
        $dm->getRepository(KeywordsQueue::class)->removeByIds($dm, $ids);
    }

    /**
     * @param array $ids
     */
    protected function processCollectionsAfterRemove(array $ids)
    {
        $dm = $this->getDocumentManager();

        $selectFields = ['brandTemplateId', 'systemKeywordId'];
        /**@var KeywordsQueue[] $keywords*/
        $keywords = $dm->getRepository(KeywordsQueue::class)->getByIds($dm , $ids, $selectFields, true);

        $systemIdsByTemplate = [];

        foreach ($keywords as $keyword){
            $systemIdsByTemplate[$keyword['brandTemplateId']] [] = $keyword['systemKeywordId'];
        }

        foreach ($systemIdsByTemplate as $templateId => $systemIds){
            $dm->setBrandTemplateId($templateId);
            $dm->getRepository(Keyword::class)->removeBySystemIds($dm , $systemIds);
        }

        // Remove keyword from Queue
        $dm->getRepository(KeywordsQueue::class)->removeByIds($dm , $ids);
    }

    /**
     * @param array $update_data
     *
     * @throws MongoDBException|InvalidArgumentException
     */
    protected function registerErrors(array $update_data)
    {
        $dm = $this->getDocumentManager();

        $ids = [];
        foreach ($update_data as $id => $system_keyword_id) {
            $ids[] = new ObjectId($id);
        }

        /** @var KeywordsQueue[] $keywords */
        $keywords = $dm->createQueryBuilder(KeywordsQueue::class)
            ->field('id')->in($ids)
            ->getQuery()
            ->execute();

        foreach ($keywords as $keyword) {
            if ($error_message = $update_data[$keyword->getId()]) {
                if (is_array($error_message)) {
                    $pes = [];
                    foreach ($error_message['policy_errors'] as $pe) {
                        $pes[] = $pe;
                    }

                    $qb = $dm->createQueryBuilder(KeywordsQueue::class);
                    $qb->updateOne()
                        ->field('id')->equals(new ObjectId($keyword->getId()))
                        ->field('policyErrors')->push($qb->expr()->each($pes))
                        ->getQuery()
                        ->execute();

                } else {
                    if (stripos($error_message, "isExemptable")) {
                        $isExemptable = substr($error_message, stripos($error_message, "isExemptable") + 18);
                        $isExemptable = substr($isExemptable, 0, stripos($isExemptable, ","));
                    } else {
                        $isExemptable = "true";
                    }

                    if (($isExemptable == "true") && (strpos($error_message, "PolicyName") !== false)) {
                        $pes = [];
                        $tempError = substr($error_message, stripos($error_message, "policyName") + 16);
                        $pes ['policyName'] = substr($tempError, 0, stripos($tempError, "'"));
                        $tempError = substr($error_message, stripos($error_message, "violatingText") + 19);
                        $pes ['violatingText'] = substr($tempError, 0, stripos($tempError, "'"));
                        $pes = [$pes];

                        $qb = $dm->createQueryBuilder(KeywordsQueue::class);
                        $qb->updateOne()
                            ->field('id')->equals(new ObjectId($keyword->getId()))
                            ->field('policyErrors')->push($qb->expr()->each($pes))
                            ->getQuery()
                            ->execute();
                    } else {
                        $keyword = $dm->createQueryBuilder(KeywordsQueue::class)
                            ->field('id')->equals(new ObjectId($keyword->getId()))
                            ->getQuery()
                            ->getSingleResult();

                        if ($keyword->getUpdate() && (strpos(AdWordsErrorDetail::errorDetail($error_message), "identical and redundant"))) {
                            continue;
                        }

                        $dm->createQueryBuilder(KeywordsQueue::class)->updateOne()
                            ->field('id')->equals(new ObjectId($keyword->getId()))
                            ->field('error')->set($error_message)
                            ->getQuery()
                            ->execute();

                        $campaignName = ProviderCampaignName::getCampaignName($dm, $this->getCache(), $keyword->getCampaignId());

                        $exemptionKeyword = new ErrorsQueue();
                        $exemptionKeyword->setType(strtolower(ContentType::KEYWORD));
                        $exemptionKeyword->setErrorElementId(new ObjectId($keyword->getId()));
                        $exemptionKeyword->setRawError($error_message);
                        $exemptionKeyword->setBackendId($keyword->getKcCampaignBackendId());
                        $exemptionKeyword->setCampaignName($campaignName);
                        $exemptionKeyword->setError(AdWordsErrorDetail::errorDetail($error_message));
                        $exemptionKeyword->setCampaignId(new ObjectId($keyword->getCampaignId()));
                        $exemptionKeyword->setAdgroup($keyword->getAdgroup());
                        $exemptionKeyword->setKeyword($keyword->getKeyword());
                        $exemptionKeyword->setTeId($keyword->getTeId());
                        $exemptionKeyword->setTeAdgroupId($keyword->getTeAdgroupId());

                        $dm->persist($exemptionKeyword);
                    }
                }
            }
        }
    }

    /**
     * @param array             $results
     * @param BatchJobInterface $hotsBatchJob
     *
     * @return mixed|void
     * @throws MongoDBException|ValidationException|InvalidArgumentException|\Exception
ё     */
    protected function processResults(array $results, BatchJobInterface $hotsBatchJob)
    {
        $queryEntitiesIds = $this->jsonDecodeMetaData($hotsBatchJob);

        $update_data = [];
        if (in_array($hotsBatchJob->getAction(), [BatchJob::ACTION_ADD, BatchJob::ACTION_UPDATE])) {
            foreach ($results['results'] as $operationIndex => $operationResponse) {
                /** @var MutateOperationResponse $operationResponse */
                if ($operationResponse->hasAdGroupCriterionResult()) {
                    if ($operationResponse->getAdGroupCriterionResult()->getResourceName() != null) {
                        $update_data[$queryEntitiesIds[$operationIndex]] = AdGroupCriterionServiceClient::parseName(
                            $operationResponse->getAdGroupCriterionResult()->getResourceName()
                        )['criterion_id'];
                    }
                }

                if ($operationResponse->hasCampaignCriterionResult()) {
                    if ($operationResponse->getCampaignCriterionResult()->getResourceName() != null) {
                        $update_data[$queryEntitiesIds[$operationIndex]] = CampaignCriterionServiceClient::parseName(
                            $operationResponse->getCampaignCriterionResult()->getResourceName()
                        )['criterion_id'];
                    }
                }
            }
        } elseif ($hotsBatchJob->getAction() === BatchJob::ACTION_REMOVE) {
            foreach ($queryEntitiesIds as $ids) {
                $update_data[] = new ObjectId($ids);
            }
        }

        $this->processingCollectionByAction($hotsBatchJob, $update_data);

        $errors = [];
        foreach ($results['errors'] as $operationIndex => $operationResponse) {
            if ($operationResponse != NULL) {
                $errors[$queryEntitiesIds[$operationIndex]] = $operationResponse;
            }
        }

        $this->registerErrors($errors);

        $this->getLogger()->info("Completed processing batch jobs results",
            [$hotsBatchJob->getOperandType(), $hotsBatchJob->getAction(),
                $hotsBatchJob->getId(), $hotsBatchJob->getSystemJobId()]);
    }
}
