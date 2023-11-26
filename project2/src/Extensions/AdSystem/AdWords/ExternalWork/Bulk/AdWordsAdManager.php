<?php

namespace App\Extensions\AdSystem\AdWords\ExternalWork\Bulk;

use App\Document\{AdsQueue, ErrorsQueue, Ad as AdInCollection, KcCampaign as MongoKcCampaign};
use App\Enums\Ads\AdEnum;
use App\Entity\{BrandTemplate, Criteria, KcCampaign as MySqlKcCampaign};
use App\EntityRepository\BrandTemplateRepository;
use App\Extensions\AdSystem\AdWords\ExternalWork\{AdWordsAd, AdWordsErrorDetail};
use App\Extensions\AdSystem\AdWords\ExternalWork\Auth\AdWordsServiceManager;
use App\Extensions\Common\{AdSystemEnum, ContentType};
use App\Extensions\Common\ExternalWork\Bulk\BatchJob;
use App\Interfaces\DocumentRepositoryInterface\BatchItemsUploadInterface;
use App\Interfaces\EntityInterface\BatchJobInterface;
use App\Providers\ProviderCampaignName;
use Doctrine\ODM\MongoDB\{DocumentManager, MongoDBException};
use Doctrine\ORM\{EntityManagerInterface, NonUniqueResultException, NoResultException};
use Google\Ads\GoogleAds\Util\FieldMasks;
use Google\Ads\GoogleAds\Util\V13\ResourceNames;
use Google\Ads\GoogleAds\V13\Enums\AdGroupAdStatusEnum\AdGroupAdStatus;
use Google\Ads\GoogleAds\V13\Common\{PolicyValidationParameter, PolicyViolationKey};
use Google\Ads\GoogleAds\V13\Resources\AdGroupAd;
use Google\Ads\GoogleAds\V13\Services\{AdGroupAdOperation, AdGroupAdServiceClient, GoogleAdsRow, MutateOperation,
    MutateOperationResponse};
use Google\ApiCore\{ApiException, ValidationException};
use MongoDB\BSON\ObjectId;
use Psr\Cache\InvalidArgumentException;
use Psr\Container\ContainerInterface;
use Psr\Log\LoggerInterface;
use Symfony\Component\Cache\Adapter\AdapterInterface;

/**
 * Class AdWordsAdManager
 *
 * @package App\Extensions\AdSystem\AdWords\ExternalWork\Bulk
 */
class AdWordsAdManager extends AdWordsBatchManager
{
    /**
     * @var AdWordsAd
     */
    protected AdWordsAd $adWordsAd;

    /**
     * AdWordsBatchManager constructor.
     *
     * @param ContainerInterface        $container
     * @param AdWordsServiceManager     $serviceManager
     * @param EntityManagerInterface    $em
     * @param DocumentManager           $dm
     * @param LoggerInterface           $adwordsLogger
     * @param AdapterInterface          $cache
     * @param AdWordsAd                 $adWordsAd
     */
    public function __construct(
        ContainerInterface      $container,
        AdWordsServiceManager   $serviceManager,
        EntityManagerInterface  $em,
        DocumentManager         $dm,
        LoggerInterface         $adwordsLogger,
        AdapterInterface        $cache,
        AdWordsAd               $adWordsAd
    ) {
        parent::__construct($container, $serviceManager, $em, $dm, $adwordsLogger, $cache);
        $this->adWordsAd = $adWordsAd;
    }

    /**
     * @return AdWordsAd
     */
    protected function getAdWordsAd(): AdWordsAd
    {
        return $this->adWordsAd;
    }

    /**
     * @return string
     */
    public function getOperandType(): string
    {
        return BatchJob::OPERAND_TYPE_AD;
    }

    /**
     * @return BatchItemsUploadInterface
     */
    public function getQueryRepository(): BatchItemsUploadInterface
    {
        return $this->getDocumentManager()->getRepository(AdsQueue::class);
    }

    /**
     * @param array $hots_entities
     * @param       $customerId
     *
     * @return array[] | false
     * @throws MongoDBException | NoResultException | NonUniqueResultException | \Exception
     */
    protected function buildAddOperations(array $hots_entities, $customerId)
    {
        $em = $this->getEntityManager();
        $dm = $this->getDocumentManager();

        $backendIds = array_unique(array_column($hots_entities, 'kcCampaignBackendId'));
        $campaigns = $dm->getRepository(MongoKcCampaign::class)
            ->getCampaignLocationInfoByBackendIds($dm, $backendIds);

        /** @var MySqlKcCampaign $offerPlaceholders */
        $offerPlaceholders = $em->getRepository(MySqlKcCampaign::class)
            ->getOfferPlaceholderByBackendIds($backendIds);

        $brandTemplateIds = array_unique(array_column($hots_entities, 'brandTemplateId'));
        $channelIdByTemplateIds = $em->getRepository(BrandTemplate::class)
            ->getChannelIdsByIds($brandTemplateIds, AdSystemEnum::ADWORDS);

        /** @var BrandTemplateRepository $placeholdersList */
        $brandPlaceholders = $em->getRepository(BrandTemplate::class)
            ->getListPlaceholdersByIds($brandTemplateIds);

        /** @var BrandTemplateRepository $trackingUrlByTemplateIds */
        $trackingUrlByTemplateIds = $em->getRepository(BrandTemplate::class)
            ->getTrackingUrlByIds($brandTemplateIds, AdSystemEnum::ADWORDS);

        $cityId = null;
        $abbreviation = null;

        $adGroupAdOperations = [];
        $entitiesIds = [];
        foreach ($hots_entities as $hots_ad) {
            if (!key_exists($hots_ad['brandTemplateId'], $brandPlaceholders)) {
                continue;
            }

            if (!array_key_exists($hots_ad['adType'], AdEnum::TYPES)) {
                continue;
            }

            if (isset($campaigns[(string)$hots_ad['campaignId']])) {
                $campaign = $campaigns[(string)$hots_ad['campaignId']];
            } else {
                $this->getQueryRepository()->removeByCampaignId($dm , $hots_ad['campaignId']);

                return false;
            }

            if ($campaign['cityId'] != $cityId) {
                $abbreviation = $em->getRepository(Criteria::class)
                    ->getByLocation($campaign['cityId'], AdSystemEnum::ADWORDS);

                $cityId = $campaign['cityId'];
            }

            // city name cannot exceed 15 characters, and if the abbreviation is empty, this ad must be skipped.
            if (is_null($abbreviation) && strlen($campaign['city']) > 15) {
                continue;
            }

            $placeholders['city'] = $campaign['city'];
            $placeholders['state'] = $campaign['state'];
            $placeholders['brand'] = $brandPlaceholders[$hots_ad['brandTemplateId']];
            $placeholders['offer'] = $offerPlaceholders[$hots_ad['kcCampaignBackendId']];
            $placeholders['adgroup'] = !empty($hots_ad['adgroupPlaceholder']) ? $hots_ad['adgroupPlaceholder'] : null;

            $ad = $this->getAdWordsAd()->makeByType($hots_ad, $cityId, $channelIdByTemplateIds[$hots_ad['brandTemplateId']],
                $trackingUrlByTemplateIds[$hots_ad['brandTemplateId']], $placeholders, $abbreviation);

            // Create ad group ad.
            $adGroupAd = new AdGroupAd();
            $adGroupAd->setAdGroup(ResourceNames::forAdGroup($customerId, (float)($hots_ad['systemAdgroupId'])));
            $adGroupAd->setAd($ad);
            $adGroupAd->setStatus($hots_ad['status'] == 1 ? AdGroupAdStatus::ENABLED : AdGroupAdStatus::PAUSED);

            // Creates an ad group ad operation.
            $adGroupAdOperation = new AdGroupAdOperation();
            $adGroupAdOperation->setCreate($adGroupAd);

            if (isset($hots_ad['policyErrors'])) {
                $errors = $hots_ad['policyErrors'];

                $policyViolationKeys = [];
                foreach ($errors as $error) {
                    $policyViolationKey = new PolicyViolationKey();
                    $policyViolationKey->setPolicyName($error['policyName']);
                    $policyViolationKey->setViolatingText($error['violatingText']);

                    $policyViolationKeys[] = $policyViolationKey;
                }

                $policyViolationParameter = new PolicyValidationParameter();
                $policyViolationParameter->setExemptPolicyViolationKeys($policyViolationKeys);
                $adGroupAdOperation->setPolicyValidationParameter($policyViolationParameter);

                $dm->createQueryBuilder(ErrorsQueue::class)->remove()
                    ->field('extensionElementId')->equals((string)$hots_ad['_id'])
                    ->getQuery()
                    ->execute();
            }

            $mutateOperation = new MutateOperation();
            $adGroupAdOperations[] = $mutateOperation->setAdGroupAdOperation($adGroupAdOperation);
            $entitiesIds[] = (string)$hots_ad['_id'];
        }

        unset($campaigns, $hots_entities);

        return [$adGroupAdOperations, $entitiesIds];
    }

    /**
     * @param array $hots_entities
     * @param       $customerId
     *
     * @return array
     */
    protected function buildUpdateOperations(array $hots_entities, $customerId): array
    {
        $adGroupAdOperations = [];
        $entitiesIds = [];
        foreach ($hots_entities as $hots_ad) {
            $adGroupAd = new AdGroupAd();
            $adGroupAd->setResourceName(ResourceNames::forAdGroupAd($customerId, $hots_ad['systemAdgroupId'], $hots_ad['systemAdId']));
            $adGroupAd->setStatus($hots_ad['status'] == 1 ? AdGroupAdStatus::ENABLED : AdGroupAdStatus::PAUSED);

            // Create operations.
            $adGroupAdOperation = new AdGroupAdOperation();
            $adGroupAdOperation->setUpdate($adGroupAd);
            $adGroupAdOperation->setUpdateMask(FieldMasks::allSetFieldsOf($adGroupAd));

            // Issues a mutate request to an ad in ad group label.
            $mutateOperation = new MutateOperation();
            $adGroupAdOperations[] = $mutateOperation->setAdGroupAdOperation($adGroupAdOperation);
            $entitiesIds[] = (string)$hots_ad['_id'];
        }

        return [$adGroupAdOperations, $entitiesIds];
    }

    /**
     * @param array $hots_entities
     * @param       $customerId
     *
     * @return array
     */
    protected function buildDeleteOperations(array $hots_entities, $customerId): array
    {
        $entitiesIds = [];
        $adGroupAdOperations = [];
        foreach ($hots_entities as $hots_ad) {
            // Creates ad group ad resource name.
            $resourceName = ResourceNames::forAdGroupAd($customerId, $hots_ad['systemAdgroupId'], $hots_ad['systemAdId']);

            // Constructs an operation that will remove the ad with the specified resource name.
            $adGroupAdOperation = new AdGroupAdOperation();
            $adGroupAdOperation->setRemove($resourceName);

            // Issues a mutate request to remove the ad group ad.
            $mutateOperation = new MutateOperation();
            $adGroupAdOperations[] = $mutateOperation->setAdGroupAdOperation($adGroupAdOperation);
            $entitiesIds[] = (string)$hots_ad['_id'];
        }

        return [$adGroupAdOperations, $entitiesIds];
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

        $adQueueIds = array_map(function ($id) {
            return new ObjectId($id);
        }, $this->jsonDecodeMetaData($hotsBatchJob));

        $update_data = [];
        $missedAdGroupIds = [];
        foreach ($batchJobResults['results'] as $operationIndex => $operationResponse) {
            /** @var MutateOperationResponse $operationResponse */
            if ($operationResponse->getAdGroupResult()->getResourceName() != null) {
                $update_data[$adQueueIds[$operationIndex]] = AdGroupAdServiceClient::parseName(
                    $operationResponse->getAdGroupAdResult()->getResourceName()
                )['ad_id'];
            } else {
                $missedAdGroupIds[] = $adQueueIds[$operationIndex];
            }
        }

        /** @var AdsQueue[] $adsQueue */
        $adsQueue = $this->getDocumentManager()->createQueryBuilder(AdsQueue::class)
            ->field('id')->in($missedAdGroupIds ?: $adQueueIds)
            ->field('systemCampaignId')->exists(true)
            ->getQuery()
            ->execute();

        $adBySystemCampaignIds = [];
        $systemCampaignIds = [];
        foreach ($adsQueue as $ad) {
            $systemCampaignIds[] = $ad->getSystemCampaignId();
            $adBySystemCampaignIds[$ad->getSystemCampaignId()][] = $ad;
        }

        if (!empty($systemCampaignIds)) {
            $query = sprintf(/** @lang text */
                "SELECT campaign.id, ad_group.id, ad_group_ad.ad.id 
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
                $systemAdgroupId = AdGroupAdServiceClient::parseName(
                    $googleAdsRow->getAdGroupAd()->getResourceName())['ad_group_id'];

                /** @var AdsQueue[] $ads */
                $ads = $adBySystemCampaignIds[$googleAdsRow->getCampaign()->getId()];
                foreach ($ads as $ad) {
                    if ($ad->getSystemAdgroupId() == $systemAdgroupId) {
                        $update_data[(string)$ad->getId()] = $googleAdsRow->getAdGroupAd()->getAd()->getId();
                    }
                }
            }
        }

        $this->processCollectionsAfterAdd($update_data);

        $this->getLogger()->info(
            "Restored " . count($update_data) . " ads after batch job failure",
            [$hotsBatchJob->getId(), $hotsBatchJob->getSystemJobId()]
        );

        return count($adQueueIds) == (count($update_data) + count($batchJobResults['errors']));
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
        foreach ($update_data as $id => $system_ad_id) {
            $ids[] = new ObjectId($id);
        }

        /** @var AdsQueue[] $ads */
        $ads = $dm->createQueryBuilder(AdsQueue::class)
            ->field('id')->in($ids)
            ->getQuery()
            ->execute();

        $campaignIds = [];
        $adsByTemplate = [];
        $adData = [];
        foreach ($ads as $ad) {
            $campaignIds[] = new ObjectId($ad->getCampaignId());
            $adsByTemplate[$ad->getBrandTemplateId()][] = $ad;
            $adData[$ad->getCampaignId()][$ad->getTeId()] = $update_data[$ad->getId()];
        }

        $campaignIds = array_unique($campaignIds);

        /** @var AdsQueue[] $ads */
        $ads = $dm->createQueryBuilder(AdsQueue::class)
            ->field('campaignId')->in($campaignIds)
            ->field('add')->exists(false)
            ->getQuery()
            ->execute();

        foreach ($ads as $ad) {
            if (isset($adData[$ad->getCampaignId()][$ad->getTeId()])) {
                $ad->setSystemAdId($adData[$ad->getCampaignId()][$ad->getTeId()]);
            }
        }
        $dm->flush();

        foreach ($ads as $ad) {
            $dm->detach($ad);
            unset($ad);
        }
        $dm->flush();
        gc_collect_cycles();

        $counter = 1;
        foreach ($adsByTemplate as $templateId => $ads) {
            $dm->setBrandTemplateId($templateId);

            foreach ($ads as $ad) {
                $hotsAd = new AdInCollection();
                $hotsAd->setTeAdgroupId($ad->getTeAdgroupId());
                $hotsAd->setTeId($ad->getTeId());
                $hotsAd->setKcCampaignBackendId($ad->getKcCampaignBackendId());
                $hotsAd->setSystemCampaignId($ad->getSystemCampaignId());
                $hotsAd->setSystemAdgroupId($ad->getSystemAdgroupId());
                $hotsAd->setSystemAdId($update_data[$ad->getId()]);

                $dm->persist($hotsAd);

                if (($counter % 100) === 0) {
                    $dm->flush();
                    $dm->clear();
                }

                $counter++;
            }

            $dm->flush();
        }

        $dm->createQueryBuilder(AdsQueue::class)
            ->remove()
            ->field('id')->in($ids)
            ->getQuery()
            ->execute();
    }

    /**
     * @param array $update_data
     *
     * @throws MongoDBException
     */
    protected function processCollectionsAfterUpdate(array $update_data)
    {
        ini_set("memory_limit", "3G");
        set_time_limit(-1);

        $ids = [];
        foreach ($update_data as $id => $system_ad_id) {
            $ids[] = new ObjectId($id);
        }

        $this->getDocumentManager()->createQueryBuilder(AdsQueue::class)
            ->remove()
            ->field('id')->in($ids)
            ->getQuery()
            ->execute();
    }

    /**
     * @param array $ids
     *
     * @throws MongoDBException
     */
    protected function processCollectionsAfterRemove(array $ids)
    {
        ini_set("memory_limit", "30G");

        $dm = $this->getDocumentManager();

        /** @var AdsQueue[] $ads */
        $ads = $dm->createQueryBuilder(AdsQueue::class)
            ->field('id')->in($ids)
            ->field('delete')->exists(true)
            ->getQuery()
            ->execute();

        $systemIdsByTemplate = [];
        foreach ($ads as $ad) {
            $systemIdsByTemplate[$ad->getBrandTemplateId()][$ad->getSystemCampaignId()][] = $ad->getSystemAdId();
        }

        foreach ($systemIdsByTemplate as $templateId => $systemIdsByCampaigns) {
            $dm->setBrandTemplateId($templateId);

            foreach ($systemIdsByCampaigns as $systemCampaignId => $systemIds) {
                $dm->createQueryBuilder(AdInCollection::class)
                    ->remove()
                    ->field('systemCampaignId')->equals($systemCampaignId)
                    ->field('systemAdId')->in($systemIds)
                    ->getQuery()
                    ->execute();
            }
        }

        $dm->createQueryBuilder(AdsQueue::class)
            ->remove()
            ->field('id')->in($ids)
            ->getQuery()
            ->execute();
    }

    /**
     * @param array $update_data
     *
     * @throws MongoDBException | InvalidArgumentException
     */
    protected function registerErrors(array $update_data)
    {
        /** @var DocumentManager $dm */
        $dm = $this->getDocumentManager();

        $ids = [];
        foreach ($update_data as $id => $system_ad_id) {
            $ids[] = new ObjectId($id);
        }

        /** @var AdsQueue[] $ads */
        $ads = $dm->createQueryBuilder(AdsQueue::class)
            ->field('id')->in($ids)
            ->getQuery()
            ->execute();

        $counter = 1;
        foreach ($ads as $ad) {
            if ($error_message = $update_data[$ad->getId()]) {
                if (is_array($error_message)) {
                    $pes = [];
                    foreach ($error_message['policy_errors'] as $pe) {
                        $pes[] = $pe;
                    }

                    $qb = $dm->createQueryBuilder(AdsQueue::class);
                    $qb->updateOne()
                        ->field('id')->equals(new ObjectId($ad->getId()))
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

                        $qb = $dm->createQueryBuilder(AdsQueue::class);
                        $qb->updateOne()
                            ->field('id')->equals(new ObjectId($ad->getId()))
                            ->field('policyErrors')->push($qb->expr()->each($pes))
                            ->getQuery()
                            ->execute();
                    } else {
                        /** @var AdsQueue $ad */
                        $ad = $dm->createQueryBuilder(AdsQueue::class)
                            ->field('id')->equals(new ObjectId($ad->getId()))
                            ->getQuery()
                            ->getSingleResult();

                        if ($ad->getUpdate() && (strpos(AdWordsErrorDetail::errorDetail($error_message), "identical and redundant"))) {
                            continue;
                        }

                        $dm->createQueryBuilder(AdsQueue::class)->updateOne()
                            ->field('id')->equals(new ObjectId($ad->getId()))
                            ->field('error')->set($error_message)
                            ->getQuery()
                            ->execute();

                        $campaignName = ProviderCampaignName::getCampaignName($dm, $this->getCache(), $ad->getCampaignId());

                        $exemptionAd = new ErrorsQueue();
                        $exemptionAd->setType(strtolower(ContentType::AD));
                        $exemptionAd->setErrorElementId(new ObjectId($ad->getId()));
                        $exemptionAd->setRawError($error_message);
                        $exemptionAd->setBackendId($ad->getKcCampaignBackendId());
                        $exemptionAd->setError(AdWordsErrorDetail::errorDetail($error_message));
                        $exemptionAd->setCampaignId(new ObjectId($ad->getCampaignId()));
                        $exemptionAd->setCampaignName($campaignName);
                        $exemptionAd->setAdgroup($ad->getAdgroup());
                        $exemptionAd->setHeadline($ad->getHeadline());
                        $exemptionAd->setTeId($ad->getTeId());
                        $exemptionAd->setTeAdgroupId($ad->getTeAdgroupId());

                        $dm->persist($exemptionAd);

                        if (($counter % 100) === 0) {
                            $dm->flush();
                            $dm->clear();
                        }

                        $counter++;
                    }
                }
            }
        }

        $dm->flush();
        $dm->clear();
    }

    /**
     * @param array             $results
     * @param BatchJobInterface $hotsBatchJob
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
                if ($operationResponse->getAdGroupAdResult()->getResourceName() != null) {
                    $update_data[$queryEntitiesIds[$operationIndex]] = AdGroupAdServiceClient::parseName(
                        $operationResponse->getAdGroupAdResult()->getResourceName()
                    )['ad_id'];
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