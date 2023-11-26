<?php

namespace App\Extensions\AdSystem\AdWords\ExternalWork\Bulk;

use App\Document\{AdgroupsQueue as AdgroupInQueue, Adgroup as AdgroupInCollection, Ad as AdInCollection, AdsQueue,
    Keyword as KeywordInCollection, ErrorsQueue, KeywordsQueue};
use App\Entity\AdwordsBatchJob;
use App\Extensions\AdSystem\AdWords\ExternalWork\AdWordsErrorDetail;
use App\Extensions\Common\ContentType;
use App\Extensions\Common\ExternalWork\Bulk\BatchJob;
use App\Interfaces\DocumentRepositoryInterface\BatchItemsUploadInterface;
use App\Interfaces\EntityInterface\BatchJobInterface;
use App\Providers\ProviderCampaignName;
use Google\Ads\GoogleAds\V13\Common\{TargetingSetting, TargetRestriction};
use Google\Ads\GoogleAds\V13\Enums\AdGroupCriterionStatusEnum\AdGroupCriterionStatus;
use Google\Ads\GoogleAds\V13\Enums\AdGroupTypeEnum\AdGroupType;
use Google\Ads\GoogleAds\V13\Enums\TargetingDimensionEnum\TargetingDimension;
use Google\Ads\GoogleAds\V13\Resources\AdGroup;
use Google\Ads\GoogleAds\V13\Services\{AdGroupOperation, AdGroupServiceClient, GoogleAdsRow, MutateOperation,
    MutateOperationResponse};
use Google\Ads\GoogleAds\Util\FieldMasks;
use Google\Ads\GoogleAds\Util\V13\ResourceNames;
use Google\ApiCore\{ApiException, ValidationException};
use Doctrine\ODM\MongoDB\MongoDBException;
use MongoDB\BSON\ObjectId;
use Psr\Cache\InvalidArgumentException;

/**
 * Class AdWordsAdGroupManager
 *
 * @package App\Extensions\AdSystem\AdWords\ExternalWork\Bulk
 */
class AdWordsAdGroupManager extends AdWordsBatchManager
{
    /**
     *
     */
    public const BATCH_SIZE = 5000;

    /**
     * @return string
     */
    protected function getOperandType(): string
    {
        return BatchJob::OPERAND_TYPE_ADGROUP;
    }

    /**
     * @return BatchItemsUploadInterface
     */
    public function getQueryRepository(): BatchItemsUploadInterface
    {
        return $this->getDocumentManager()->getRepository(AdgroupInQueue::class);
    }

    /**
     * @param array     $hots_entities
     * @param           $customerId
     * @return array
     */
    protected function buildAddOperations(array $hots_entities, $customerId): array
    {
        $operations = [];
        $entitiesIds = [];

        $adGroupTemporaryId = -1;
        foreach ($hots_entities as $hots_adgroup) {
            $adGroupTemporaryId--;

            $adGroup = new AdGroup();
            $adGroup->setResourceName(ResourceNames::forAdGroup($customerId, $adGroupTemporaryId));
            $adGroup->setName($hots_adgroup['adgroup']);
            $adGroup->setCampaign(ResourceNames::forCampaign($customerId, $hots_adgroup['systemCampaignId']));
            $adGroup->setCpcBidMicros((int)$hots_adgroup['defaultCpc'] * 1000000);
            $adGroup->setStatus($hots_adgroup['status'] ? AdGroupCriterionStatus::ENABLED : AdGroupCriterionStatus::PAUSED);
            $adGroup->setType(AdGroupType::SEARCH_STANDARD);

            $targetPlacementRestriction = new TargetRestriction();
            $targetPlacementRestriction->setTargetingDimension(TargetingDimension::PLACEMENT);
            $targetPlacementRestriction->setBidOnly(false);

            $targetingSetting = new TargetingSetting();
            $targetingSetting->setTargetRestrictions([$targetPlacementRestriction]);
            $adGroup->setTargetingSetting($targetingSetting);

            // Creates an ad group ad operation and add it to the operations list.

            $adGroupAdOperation = new AdGroupOperation();
            $adGroupAdOperation->setCreate($adGroup);

            $mutateOperation = new MutateOperation();
            $operations[] = $mutateOperation->setAdGroupOperation($adGroupAdOperation);
            $entitiesIds[] = (string)$hots_adgroup['_id'];
        }

        return [$operations, $entitiesIds];
    }

    /**
     * @param array     $hots_entities
     * @param           $customerId
     * @return array
     */
    protected function buildUpdateOperations(array $hots_entities, $customerId): array
    {
        $adGroupOperations = [];
        $entitiesIds = [];
        foreach ($hots_entities as $hots_adgroup) {
            $adGroup = new AdGroup();
            $adGroup->setResourceName(ResourceNames::forAdGroup($customerId, $hots_adgroup['systemAdgroupId']));
            $adGroup->setName($hots_adgroup['adgroup']);
            $adGroup->setCampaign(ResourceNames::forCampaign($customerId, $hots_adgroup['systemCampaignId']));
            $adGroup->setCpcBidMicros((int)$hots_adgroup['defaultCpc'] * 1000000);
            $adGroup->setStatus($hots_adgroup['status'] ? AdGroupCriterionStatus::ENABLED : AdGroupCriterionStatus::PAUSED);

            // Creates an ad group ad operation and add it to the operations list.
            $adGroupOperation = new AdGroupOperation();
            $adGroupOperation->setUpdate($adGroup);
            $adGroupOperation->setUpdateMask(FieldMasks::allSetFieldsOf($adGroup));

            // Issues a mutate request to in label an ad group.
            $mutateOperation = new MutateOperation();
            $adGroupOperations[] = $mutateOperation->setAdGroupOperation($adGroupOperation);
            $entitiesIds[] = (string)$hots_adgroup['_id'];
        }

        return [$adGroupOperations, $entitiesIds];
    }


    /**
     * @param array     $hots_entities
     * @param           $customerId
     * @return array
     */
    protected function buildDeleteOperations(array $hots_entities, $customerId): array
    {
        $adGroupOperations = [];
        $entitiesIds = [];
        foreach ($hots_entities as $hots_adgroup) {
            // Creates ad group resource name.
            $resourceName = ResourceNames::forAdGroup($customerId, $hots_adgroup['systemAdgroupId']);

            // Constructs an operation that will remove the ad group with the specified resource name.
            $adGroupOperation = new AdGroupOperation();
            $adGroupOperation->setRemove($resourceName);

            // Issues a mutate request to remove the ad group.
            $mutateOperation = new MutateOperation();
            $adGroupOperations[] = $mutateOperation->setAdGroupOperation($adGroupOperation);
            $entitiesIds[] = (string)$hots_adgroup['_id'];
        }

        return [$adGroupOperations, $entitiesIds];
    }

    /**
     * @param BatchJobInterface $hotsBatchJob
     * @param array             $batchJobResults
     *
     * @return bool
     * @throws ApiException | ValidationException | MongoDBException
     */
    protected function failedResultProcessingFallback(BatchJobInterface $hotsBatchJob, array $batchJobResults): bool
    {
        $googleAdsServiceClient = $this->getGoogleServiceManager()->getGoogleAdsServiceClient();
        $dm = $this->getDocumentManager();

        $adGroupQueueIds = array_map(function ($id) {
            return new ObjectId($id);
        }, $this->jsonDecodeMetaData($hotsBatchJob));

        $update_data = [];
        $missedAdGroupIds = [];
        foreach ($batchJobResults['results'] as $operationIndex => $operationResponse) {
            /** @var MutateOperationResponse $operationResponse */
            if ($operationResponse->getAdGroupResult()->getResourceName() != null) {
                $update_data[$adGroupQueueIds[$operationIndex]] = AdGroupServiceClient::parseName(
                    $operationResponse->getAdGroupResult()->getResourceName()
                )['ad_group_id'];
            } else {
                $missedAdGroupIds[] = $adGroupQueueIds[$operationIndex];
            }
        }

        /** @var AdgroupInQueue[] $adgroupQueue */
        $adgroupQueue = $dm->createQueryBuilder(AdgroupInQueue::class)
            ->field('id')->in($missedAdGroupIds ?: $adGroupQueueIds)
            ->field('systemCampaignId')->exists(true)
            ->getQuery()
            ->execute();

        $adGroupBySystemCampaignIds = [];
        $systemCampaignIds = [];
        foreach ($adgroupQueue as $adgroup) {
            $systemCampaignIds[] = $adgroup->getSystemCampaignId();
            $adGroupBySystemCampaignIds[$adgroup->getSystemCampaignId()][] = $adgroup;
        }

        if (!empty($systemCampaignIds)) {
            $query = sprintf(/** @lang text */
                "SELECT campaign.id, ad_group.id, ad_group.name 
            FROM ad_group 
            WHERE campaign.id IN (%s) ",
                implode(', ', $systemCampaignIds)
            );

            // Issues a search request by specifying page size.
            $response = $googleAdsServiceClient->search(
                $hotsBatchJob->getSystemAccount()->getSystemAccountId(),
                $query,
                ['pageSize' => $this->getGoogleServiceManager()::PAGE_SIZE]
            );

            if (empty($response->iterateAllElements())) {
                return false;
            }
            // Iterates over all rows in all pages and prints the requested field values for
            // the ad group in each row.
            /** @var GoogleAdsRow $googleAdsRow */
            foreach ($response->iterateAllElements() as $googleAdsRow) {
                /** @var AdgroupInQueue[] $adgroups */
                $adgroups = $adGroupBySystemCampaignIds[$googleAdsRow->getCampaign()->getId()];

                foreach ($adgroups as $adgroup) {
                    if ($adgroup->getAdgroup() == $googleAdsRow->getAdGroup()->getName()) {
                        $update_data[(string)$adgroup->getId()] = $googleAdsRow->getAdGroup()->getId();
                    }
                }
            }
        }

        $this->processCollectionsAfterAdd($update_data);

        $this->getLogger()->info(
            "Restored " . count($update_data) . " adgroups after batch job failure",
            [$hotsBatchJob->getId(), $hotsBatchJob->getSystemJobId()]
        );

        return count($adGroupQueueIds) == (count($update_data) + count($batchJobResults['errors']));
    }

    /**
     * @param array     $update_data
     * @throws MongoDBException
     */
    public function processCollectionsAfterAdd(array $update_data)
    {
        $dm = $this->getDocumentManager();
        ini_set("memory_limit", "30G");

        $ids = [];
        foreach ($update_data as $id => $system_adgroup_id) {
            $ids[] = new ObjectId($id);
        }

        /** @var AdgroupInQueue[] $adgroups */
        $adgroups = $dm->createQueryBuilder(AdgroupInQueue::class)
            ->field('id')->in($ids)
            ->getQuery()
            ->execute();

        $campaignIds = [];
        $adgroupsByTemplate = [];
        $adgroupData = [];
        foreach ($adgroups as $adgroup) {
            $campaignIds[] = new ObjectId($adgroup->getCampaignId());

            $dm->createQueryBuilder(AdgroupInQueue::class)->updateOne()
                ->field('teId')->equals($adgroup->getTeId())
                ->field('campaignId')->equals(new ObjectId($adgroup->getCampaignId()))
                ->field('add')->exists(false)
                ->field('systemAdgroupId')->set($update_data[(string)$adgroup->getId()])
                ->getQuery()
                ->execute();

            $adgroupsByTemplate[$adgroup->getBrandTemplateId()][] = $adgroup;
            $adgroupData[$adgroup->getCampaignId()][$adgroup->getTeId()] = $update_data[$adgroup->getId()];
        }

        $campaignIds = array_unique($campaignIds);

        /** @var AdsQueue[] $ads */
        $ads = $dm->createQueryBuilder(AdsQueue::class)
            ->field('campaignId')->in($campaignIds)
            ->immortal()// no limit of time for cursor lifetime
            ->getQuery()
            ->execute();
//        ->timeout(-1); // no limit of time for executing query

        foreach ($ads as $ad) {
            if (isset($adgroupData[$ad->getCampaignId()][$ad->getTeAdgroupId()]))
                $ad->setSystemAdgroupId($adgroupData[$ad->getCampaignId()][$ad->getTeAdgroupId()]);
        }
        $dm->flush();
        foreach ($ads as $ad) {
            $dm->detach($ad);
            unset($ad);
        }
        $dm->flush();
        gc_collect_cycles();

        /** @var KeywordsQueue[] $keywords */
        $keywords = $dm->createQueryBuilder(KeywordsQueue::class)
            ->field('campaignId')->in($campaignIds)
            ->immortal()// no limit of time for cursor lifetime
            ->getQuery()
            ->execute();
//            ->timeout(-1); // no limit of time for executing query

        foreach ($keywords as $keyword) {
            if (isset($adgroupData[$keyword->getCampaignId()][$keyword->getTeAdgroupId()])){
                $keyword->setSystemAdgroupId($adgroupData[$keyword->getCampaignId()][$keyword->getTeAdgroupId()]);
            }
        }
        $dm->flush();
        foreach ($keywords as $keyword) {
            $dm->detach($keyword);
            unset($keyword);
        }
        gc_collect_cycles();

        $counter = 1;
        foreach ($adgroupsByTemplate as $templateId => $adgroups) {
            $dm->setBrandTemplateId($templateId);
            foreach ($adgroups as $adgroup) {
                $hotsAdgroup = new AdgroupInCollection();
                //
                //CampaignAdgroup fields
                //
                //$hotsAdgroup->setUniqueBid($adgroup->getDefaultCpc());
                //$hotsAdgroup->setUniqueStatus($adgroup->getStatus());
                $hotsAdgroup->setTeId($adgroup->getTeId());
                $hotsAdgroup->setKcCampaignBackendId($adgroup->getKcCampaignBackendId());
                $hotsAdgroup->setSystemCampaignId($adgroup->getSystemCampaignId());
                $hotsAdgroup->setSystemAdgroupId($update_data[$adgroup->getId()]);

                $dm->persist($hotsAdgroup);

                if (($counter % 100) === 0) {
                    $dm->flush();
                    $dm->clear();
                }

                $counter++;
            }
            $dm->flush();
        }

        // Remove ad groups from Queue
        $dm->getRepository(AdgroupInQueue::class)->removeByIds($dm, $ids);
    }

    /**
     * @param array $update_data
     */
    protected function processCollectionsAfterUpdate(array $update_data)
    {
        $dm = $this->getDocumentManager();
        $ids = [];
        /* $systemAdgroupIds = [];
         $uniqueBidByTemplate = [];*/

        foreach ($update_data as $id => $system_adgroup_id) {
            $ids[] = new ObjectId($id);
        }

        // Remove ad groups from Queue
        $dm->getRepository(AdgroupInQueue::class)->removeByIds($dm, $ids);
    }

    /**
     * @param array     $ids
     *
     * @throws MongoDBException
     */
    protected function processCollectionsAfterRemove(array $ids)
    {
        $dm = $this->getDocumentManager();
        ini_set("memory_limit", "30G");
        $systemIdsByTemplate = [];

        /** @var AdgroupInQueue[] $adgroups */
        $adgroups = $dm->createQueryBuilder(AdgroupInQueue::class)
            ->field('id')->in($ids)
            ->field('delete')->exists(true)
            ->getQuery()
            ->execute();

        foreach ($adgroups as $adgroup) {
            if (!isset($systemIdsByTemplate[$adgroup->getBrandTemplateId()])) {
                $systemIdsByTemplate[$adgroup->getBrandTemplateId()] = [];
            }

            $systemIdsByTemplate[$adgroup->getBrandTemplateId()] [] = $adgroup->getSystemAdgroupId();
        }

        foreach ($systemIdsByTemplate as $templateId => $systemIds) {
            $dm->setBrandTemplateId($templateId);

            $dm->getRepository(KeywordInCollection::class)
                ->removeByAttributesIn($dm, ['systemAdgroupId' => $systemIds]);
            $dm->getRepository(AdInCollection::class)
                ->removeByAttributesIn($dm, ['systemAdgroupId' => $systemIds]);
            $dm->getRepository(AdgroupInCollection::class)
                ->removeByAttributesIn($dm, ['systemAdgroupId' => $systemIds]);
        }

        // Remove ad groups from Queue
        $dm->getRepository(AdgroupInQueue::class)->removeByIds($dm, $ids);
    }

    /**
     * @param array $update_data
     *
     * @throws MongoDBException | InvalidArgumentException
     */
    protected function registerErrors(array $update_data)
    {
        $dm = $this->getDocumentManager();

        $ids = [];
        foreach ($update_data as $id => $system_ad_id) {
            $ids[] = new ObjectId($id);
        }

        $adgroups = $dm->createQueryBuilder(AdgroupInQueue::class)
            ->field('id')->in($ids)
            ->getQuery()
            ->execute();

        foreach ($adgroups as $adgroup) {
            if ($error_message = $update_data[$adgroup->getId()]) {
                if (is_array($error_message)) {

                    $pes = [];
                    foreach ($error_message['policy_errors'] as $pe) {
                        $pes[] = $pe;
                    }

                    $qb = $dm->createQueryBuilder(AdgroupInQueue::class);

                    $qb->updateOne()
                        ->field('id')->equals(new ObjectId($adgroup->getId()))
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
                        $pes['policyName'] = substr($tempError, 0, stripos($tempError, "'"));
                        $tempError = substr($error_message, stripos($error_message, "violatingText") + 19);
                        $pes['violatingText'] = substr($tempError, 0, stripos($tempError, "'"));
                        $pes = [$pes];

                        $qb = $dm->createQueryBuilder(AdgroupInQueue::class);
                        $qb->updateOne()
                            ->field('id')->equals(new ObjectId($adgroup->getId()))
                            ->field('policyErrors')->push($qb->expr()->each($pes))
                            ->getQuery()
                            ->execute();
                    } else {
                        /** @var AdgroupInQueue $adgroup */
                        $adgroup = $dm->createQueryBuilder(AdgroupInQueue::class)
                            ->field('id')->equals(new ObjectId($adgroup->getId()))
                            ->getQuery()
                            ->getSingleResult();

                        if ($adgroup->getUpdate() && (strpos(AdWordsErrorDetail::errorDetail($error_message), "identical and redundant"))) {
                            continue;
                        }

                        $dm->createQueryBuilder(AdgroupInQueue::class)->updateOne()
                            ->field('id')->equals(new ObjectId($adgroup->getId()))
                            ->field('error')->set($error_message)
                            ->getQuery()
                            ->execute();

                        $campaignName = ProviderCampaignName::getCampaignName($dm, $this->getCache(), $adgroup->getCampaignId());

                        $exemptionAdgroup = new ErrorsQueue();
                        $exemptionAdgroup->setType(strtolower(ContentType::ADGROUP));
                        $exemptionAdgroup->setErrorElementId(new ObjectId($adgroup->getId()));
                        $exemptionAdgroup->setRawError($error_message);
                        $exemptionAdgroup->setBackendId($adgroup->getKcCampaignBackendId());
                        $exemptionAdgroup->setError(AdWordsErrorDetail::errorDetail($error_message));
                        $exemptionAdgroup->setCampaignId(new ObjectId($adgroup->getCampaignId()));
                        $exemptionAdgroup->setCampaignName($campaignName);
                        $exemptionAdgroup->setAdgroup($adgroup->getAdgroup());
                        $exemptionAdgroup->setTeId($adgroup->getTeId());

                        $dm->persist($exemptionAdgroup);
                    }
                }
            }
        }
    }

    /**
     * @param array             $results
     * @param AdwordsBatchJob   $hotsBatchJob
     *
     * @return void
     * @throws MongoDBException | ValidationException | InvalidArgumentException | \Exception
     */
    protected function processResults(array $results, BatchJobInterface $hotsBatchJob)
    {
        $queryEntitiesIds = $this->jsonDecodeMetaData($hotsBatchJob);

        $update_data = [];
        if (in_array($hotsBatchJob->getAction(), [BatchJob::ACTION_ADD, BatchJob::ACTION_UPDATE])) {
            foreach ($results['results'] as $operationIndex => $operationResponse) {
                /** @var MutateOperationResponse $operationResponse */
                if ($operationResponse->getAdGroupResult()->getResourceName() != null) {
                    $update_data[$queryEntitiesIds[$operationIndex]] = AdGroupServiceClient::parseName(
                        $operationResponse->getAdGroupResult()->getResourceName()
                    )['ad_group_id'];
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