<?php

namespace App\Extensions\AdSystem\AdWords\ExternalWork\Bulk;

use App\Document\{ErrorsQueue, Extension as ExtensionInCollection, ExtensionsQueue, KcCampaign as MongoKcCampaign};
use App\Entity\{BrandExtension as EntityExtension, BrandTemplate};
use App\Extensions\AdSystem\AdWords\ExternalWork\{AdWordsExtension, AdWordsErrorDetail};
use App\Extensions\AdSystem\AdWords\ExternalWork\Auth\AdWordsServiceManager;
use App\Extensions\Common\{AdSystemEnum, ContentType};
use App\Extensions\Common\ExternalWork\Bulk\BatchJob;
use App\Interfaces\DocumentRepositoryInterface\BatchItemsUploadInterface;
use App\Interfaces\EntityInterface\BatchJobInterface;
use App\Providers\ProviderCampaignName;
use Doctrine\ODM\MongoDB\{DocumentManager, MongoDBException};
use Doctrine\ORM\EntityManagerInterface;
use Google\Ads\GoogleAds\Util\V13\ResourceNames;
use Google\Ads\GoogleAds\V13\Enums\AssetFieldTypeEnum\AssetFieldType;
use Google\Ads\GoogleAds\V13\Resources\CampaignAsset;
use Google\Ads\GoogleAds\V13\Services\{
    AssetOperation,
    CampaignAssetOperation,
    CampaignAssetServiceClient,
    CampaignServiceClient,
    GoogleAdsRow,
    MutateOperation,
    MutateOperationResponse};
use Google\ApiCore\{ApiException, ValidationException};
use MongoDB\BSON\ObjectId;
use Psr\Cache\InvalidArgumentException;
use Psr\Container\ContainerInterface;
use Psr\Log\LoggerInterface;
use Symfony\Component\Cache\Adapter\AdapterInterface;

/**
 * Class AdWordsAdManager.php
 *
 * @package App\Extensions\AdSystem\AdWords\ExternalWork\Bulk
 */
class AdWordsExtensionManager extends AdWordsBatchManager
{
    private int $temporaryId = -1;

    /**
     * @var AdWordsExtension
     */
    protected AdWordsExtension $googleExtension;

    /**
     * @param $category
     * @return false|mixed
     */
    public function getFilterByCategory($category)
    {
        $array = [
            EntityExtension::SITELINK    => AssetFieldType::SITELINK,
            EntityExtension::PHONE       => AssetFieldType::CALL,
            EntityExtension::CALLOUT     => AssetFieldType::CALLOUT,
            EntityExtension::STRUCTURED  => AssetFieldType::STRUCTURED_SNIPPET,
            EntityExtension::PROMOTIONAL => AssetFieldType::PROMOTION,
        ];

        return key_exists($category, $array) ? $array[$category] : false;
    }

    /**
     * AdWordsBatchManager constructor.
     *
     * @param ContainerInterface        $container
     * @param AdWordsServiceManager     $serviceManager
     * @param EntityManagerInterface    $em
     * @param DocumentManager           $dm
     * @param LoggerInterface           $adwordsLogger
     * @param AdapterInterface          $cache
     * @param AdWordsExtension          $googleExtension
     */
    public function __construct(
        ContainerInterface      $container,
        AdWordsServiceManager   $serviceManager,
        EntityManagerInterface  $em,
        DocumentManager         $dm,
        LoggerInterface         $adwordsLogger,
        AdapterInterface        $cache,
        AdWordsExtension        $googleExtension
    ) {
        parent::__construct($container, $serviceManager, $em, $dm, $adwordsLogger, $cache);
        $this->googleExtension = $googleExtension;
    }

    /**
     * @return AdWordsExtension
     */
    public function getGoogleExtension(): AdWordsExtension
    {
        return $this->googleExtension;
    }

    /**
     * Returns the next temporary ID and decrease it by one.
     *
     * @return int the next temporary ID
     */
    private function getNextTemporaryId(): int
    {
        return $this->temporaryId--;
    }

    /**
     * @return string
     */
    protected function getOperandType(): string
    {
        return BatchJob::OPERAND_TYPE_EXTENSION;
    }

    /**
     * @return BatchItemsUploadInterface
     */
    protected function getQueryRepository(): BatchItemsUploadInterface
    {
        return $this->getDocumentManager()->getRepository(ExtensionsQueue::class);
    }

    /**
     * @param array $hots_entities
     * @param       $customerId
     *
     * @return array[]|false
     * @throws MongoDBException|\Exception
     */
    protected function buildAddOperations(array $hots_entities, $customerId)
    {
        $em = $this->getEntityManager();
        $dm = $this->getDocumentManager();

        $backendIds = array_unique(array_column($hots_entities, 'kcCampaignBackendId'));
        $campaigns = $dm->getRepository(MongoKcCampaign::class)
            ->getCampaignLocationInfoByBackendIds($dm, $backendIds);

        $brandTemplateIds = array_unique(array_column($hots_entities, 'brandTemplateId'));

        $channelIdByTemplateIds = $em->getRepository(BrandTemplate::class)
            ->getChannelIdsByIds($brandTemplateIds, AdSystemEnum::ADWORDS);

        $operations = [];
        $entitiesIds = [];
        foreach ($hots_entities as $hots_asset) {
            if (!isset($campaigns[(string)$hots_asset['campaignId']])) {
                $this->getQueryRepository()->removeByCampaignId($dm , $hots_asset['campaignId']);

                return false;
            }

            if (!isset($this->getGoogleExtension()->getMethodsListForExtensions()[$hots_asset['category']])) {
                continue;
            }

            $asset = $this->getGoogleExtension()->makeByType(
                $hots_asset,
                $channelIdByTemplateIds[$hots_asset['brandTemplateId']]
            );

            $asset->setResourceName(ResourceNames::forAsset($customerId, $this->getNextTemporaryId()));

            // Creates an asset operation.
            $assetOperation = new AssetOperation();
            $assetOperation->setCreate($asset);
            // mutate operations to be added to a batch job for asset
            $mutateAssetOperation = new MutateOperation();
            $operations[] = $mutateAssetOperation->setAssetOperation($assetOperation);

            // A link between a Campaign and an Asset
            $campaignAsset = new CampaignAsset();
            $campaignAsset->setAsset($mutateAssetOperation->getAssetOperation()->getCreate()->getResourceName());
            $campaignAsset->setCampaign(ResourceNames::forCampaign($customerId, $hots_asset['systemCampaignId']));
            $campaignAsset->setFieldType($this->getFilterByCategory($hots_asset['category']));

            // Creates a CampaignAssetOperation for each asset resource name by linking it to a newly
            $campaignAssetOperation = new CampaignAssetOperation();
            $campaignAssetOperation->setCreate($campaignAsset);
            // mutate operations to be added to a batch job for asset by linking it campaign
            $mutateCampaignAssetOperation = new MutateOperation();
            $operations[] = $mutateCampaignAssetOperation->setCampaignAssetOperation($campaignAssetOperation);
            $entitiesIds[] = (string)$hots_asset['_id'];
        }

        unset($campaigns, $hots_entities);

        return [$operations, $entitiesIds];
    }

    /**
     * @param array $hots_entities
     * @param       $customerId
     *
     * @return array
     */
    protected function buildUpdateOperations(array $hots_entities, $customerId): array
    {
        $operations = [];
        $entitiesIds = [];
//        foreach ($hots_entities as $hots_asset) {
//            $adGroupAd = new AdGroupAd();
//            $adGroupAd->setResourceName(ResourceNames::forAdGroupAd($customerId, $hots_asset['systemAdgroupId'], $hots_asset['systemAdId']));
//            $adGroupAd->setStatus($hots_asset['status'] == 1 ? AdGroupAdStatus::ENABLED : AdGroupAdStatus::PAUSED);
//
//            // Create operations.
//            $adGroupAdOperation = new AdGroupAdOperation();
//            $adGroupAdOperation->setUpdate($adGroupAd);
//            $adGroupAdOperation->setUpdateMask(FieldMasks::allSetFieldsOf($adGroupAd));
//
//            // Issues a mutate request to an ad in ad group label.
//            $mutateOperation = new MutateOperation();
//            $operations[] = $mutateOperation->setAdGroupAdOperation($adGroupAdOperation);
//            $entitiesIds[] = (string)$hots_asset['_id'];
//        }

        return [$operations, $entitiesIds];
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
        $operations = [];
        foreach ($hots_entities as $hots_asset) {
            // Creates resource name of the asset.
            $resourceName = ResourceNames::forCampaignAsset(
                $customerId,
                $hots_asset['systemCampaignId'],
                $hots_asset['systemExtensionId'],
                AssetFieldType::name($this->getFilterByCategory($hots_asset['category']))
            );

            // Constructs an operation that will remove the asset with the specified resource name.
            $campaignAssetOperation = new CampaignAssetOperation();
            $campaignAssetOperation->setRemove($resourceName);

            // Issues a mutate request to remove the ad group ad.
            $mutateOperation = new MutateOperation();
            $operations[] = $mutateOperation->setCampaignAssetOperation($campaignAssetOperation);
            $entitiesIds[] = (string)$hots_asset['_id'];
        }

        return [$operations, $entitiesIds];
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

        $extQueueIds = array_map(function ($id) {
            return new ObjectId($id);
        }, $this->jsonDecodeMetaData($hotsBatchJob));

        $update_data = [];
        $missedExtIds = [];
        foreach ($batchJobResults['results'] as $operationIndex => $operationResponse) {
            /** @var MutateOperationResponse $operationResponse */
            if ($operationResponse->hasCampaignAssetResult() && $operationIndex % 2 !== 0) {
                $update_data[$extQueueIds[(int)floor($operationIndex / 2)]] = CampaignAssetServiceClient::parseName(
                    $operationResponse->getCampaignAssetResult()->getResourceName()
                )['asset_id'];
            } else {
                $missedExtIds[] = $extQueueIds[(int)floor($operationIndex / 2)];
            }
        }

        /** @var ExtensionsQueue[] $extQueue */
        $extQueue = $this->getDocumentManager()->createQueryBuilder(ExtensionsQueue::class)
            ->field('id')->in($missedExtIds ?: $extQueueIds)
            ->field('systemCampaignId')->exists(true)
            ->getQuery()
            ->execute();

        $extBySystemCampaignIds = [];
        $systemCampaignIds = [];
        foreach ($extQueue as $ext) {
            $systemCampaignIds[] = $ext->getSystemCampaignId();
            $extBySystemCampaignIds[$ext->getSystemCampaignId()][] = $ext;
        }

        $systemCampaignIds = array_unique($systemCampaignIds);
        if (!empty($systemCampaignIds)) {
            $query = "SELECT campaign_asset.resource_name, campaign_asset.asset, campaign_asset.campaign, campaign_asset.field_type "
                . "FROM campaign_asset "
                . "WHERE campaign_asset.status != 'REMOVED'";

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
                if (in_array(CampaignServiceClient::parseName($googleAdsRow->getCampaignAsset()->getCampaign())['campaign_id'], $systemCampaignIds)) {
                    $systemExtensionId = CampaignAssetServiceClient::parseName(
                        $googleAdsRow->getCampaignAsset()->getResourceName()
                    )['asset_id'];

                    /** @var ExtensionsQueue[] $exts */
                    $exts = $extBySystemCampaignIds[$googleAdsRow->getCampaign()->getId()];
                    foreach ($exts as $ext) {
                        if ($googleAdsRow->getCampaignAsset()->getFieldType() == $this->getFilterByCategory($ext->getCategory())) {
                            $methodName = 'get' . ucfirst(str_replace('_s', 'S', strtolower(
                                    AssetFieldType::name($googleAdsRow->getCampaignAsset()->getFieldType())))) . 'Asset';

                            $asset = $this->getGoogleExtension()->$methodName(
                                $hotsBatchJob->getSystemAccount()->getSystemAccountId(),
                                $googleAdsRow->getCampaignAsset()->getAsset()
                            );

                            switch ($googleAdsRow->getCampaignAsset()->getFieldType()) {
                                case AssetFieldType::SITELINK:
                                    if ($ext->getName() == $asset[$googleAdsRow->getCampaignAsset()->getAsset()]['linkText'] &&
                                        $ext->getDesc1() == $asset[$googleAdsRow->getCampaignAsset()->getAsset()]['description1'] &&
                                        $ext->getDesc2() == $asset[$googleAdsRow->getCampaignAsset()->getAsset()]['description2']
                                    )
                                        $update_data[(string)$ext->getId()] = $systemExtensionId;

                                    break;
                                case AssetFieldType::STRUCTURED_SNIPPET || AssetFieldType::CALLOUT:
                                    if ($googleAdsRow->getCampaignAsset()->getFieldType() == AssetFieldType::STRUCTURED_SNIPPET) {
                                        $assetText = implode(', ', $asset[$googleAdsRow->getCampaignAsset()->getAsset()]);
                                    } else {
                                        $assetText = $asset[$googleAdsRow->getCampaignAsset()->getAsset()];
                                    }

                                    if ($ext->getCallout() == $assetText)
                                        $update_data[(string)$ext->getId()] = $systemExtensionId;

                                    break;
                                case AssetFieldType::PROMOTION:
                                    if ($ext->getName() == $asset[$googleAdsRow->getCampaignAsset()->getAsset()])
                                        $update_data[(string)$ext->getId()] = $systemExtensionId;

                                    break;
                                case AssetFieldType::CALL:
                                    if ($ext->getPhone() == $asset[$googleAdsRow->getCampaignAsset()->getAsset()])
                                        $update_data[(string)$ext->getId()] = $systemExtensionId;

                                    break;
                                default:
                                    continue 2;
                            }
                        }
                    }
                }
            }
        }

        $this->processCollectionsAfterAdd($update_data);

        $this->getLogger()->info(
            "Restored " . count($update_data) . " ads after batch job failure",
            [$hotsBatchJob->getId(), $hotsBatchJob->getSystemJobId()]
        );

        return count($extQueueIds) == (count($update_data) + count($batchJobResults['errors']));
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
        foreach ($update_data as $id => $system_id) {
            $ids[] = new ObjectId($id);
        }

        /** @var ExtensionsQueue[] $extensions */
        $extensions = $dm->createQueryBuilder(ExtensionsQueue::class)
            ->field('id')->in($ids)
            ->getQuery()
            ->execute();

        $campaignIds = [];
        $extensionsByTemplate = [];
        $extensionData = [];
        foreach ($extensions as $extension) {
            $campaignIds[] = new ObjectId($extension->getCampaignId());
            $extensionsByTemplate[$extension->getBrandTemplateId()][] = $extension;
            $extensionData[$extension->getCampaignId()][$extension->getTeId()] = $update_data[$extension->getId()];
        }

        $campaignIds = array_unique($campaignIds);

        /** @var ExtensionsQueue[] $extensions */
        $extensions = $dm->createQueryBuilder(ExtensionsQueue::class)
            ->field('campaignId')->in($campaignIds)
            ->field('add')->exists(false)
            ->getQuery()
            ->execute();

        foreach ($extensions as $extension) {
            if (isset($extensionData[$extension->getCampaignId()][$extension->getTeId()])) {
                $extension->setSystemExtensionId($extensionData[$extension->getCampaignId()][$extension->getTeId()]);
            }
        }

        $dm->flush();

        foreach ($extensions as $extension) {
            $dm->detach($extension);

            unset($extension);
        }

        $dm->flush();

        gc_collect_cycles();

        $counter = 1;
        foreach ($extensionsByTemplate as $templateId => $extensions) {
            $dm->setBrandTemplateId($templateId);

            foreach ($extensions as $extension) {
                $campaignExtension = new ExtensionInCollection();
                $campaignExtension->setCategory($extension->getCategory());
                $campaignExtension->setTeId($extension->getTeId());
                $campaignExtension->setSystemExtensionId($update_data[$extension->getId()]);
                $campaignExtension->setSystemAccount($extension->getSystemAccount());
                $campaignExtension->setSystemCampaignId($extension->getSystemCampaignId());
                $campaignExtension->setKcCampaignBackendId($extension->getKcCampaignBackendId());

                $dm->persist($campaignExtension);

                if (($counter % 100) === 0) {
                    $dm->flush();
                    $dm->clear();
                }

                $counter++;
            }

            $dm->flush();
        }

        $dm->createQueryBuilder(ExtensionsQueue::class)
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
//        ini_set("memory_limit", "3G");
//        set_time_limit(-1);
//
//        $ids = [];
//        foreach ($update_data as $id => $system_id) {
//            $ids[] = new ObjectId($id);
//        }
//
//        $this->getDocumentManager()->createQueryBuilder(ExtensionsQueue::class)
//            ->remove()
//            ->field('id')->in($ids)
//            ->getQuery()
//            ->execute();
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

        /** @var ExtensionsQueue[] $extensions */
        $extensions = $dm->createQueryBuilder(ExtensionsQueue::class)
            ->field('id')->in($ids)
            ->field('delete')->exists(true)
            ->getQuery()
            ->execute();

        $systemIdsByTemplate = [];
        foreach ($extensions as $extension) {
            $systemIdsByTemplate[$extension->getBrandTemplateId()][$extension->getSystemCampaignId()][]
                = $extension->getSystemExtensionId();
        }

        foreach ($systemIdsByTemplate as $templateId => $systemIdsByCampaigns) {
            $dm->setBrandTemplateId($templateId);

            foreach ($systemIdsByCampaigns as $systemCampaignId => $systemIds) {
                $dm->createQueryBuilder(ExtensionInCollection::class)
                    ->remove()
                    ->field('systemCampaignId')->equals($systemCampaignId)
                    ->field('systemExtensionId')->in($systemIds)
                    ->getQuery()
                    ->execute();
            }
        }

        $dm->createQueryBuilder(ExtensionsQueue::class)
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
        $dm = $this->getDocumentManager();

        $ids = [];
        foreach ($update_data as $id => $system_id) {
            $ids[] = new ObjectId($id);
        }

        $exts = $dm->createQueryBuilder(ExtensionsQueue::class)
            ->field('id')->in($ids)
            ->getQuery()
            ->execute();

        $counter = 1;
        foreach ($exts as $ext) {
            if ($error_message = $update_data[$ext->getId()]) {
                if (is_array($error_message)) {
                    $pes = [];
                    foreach ($error_message['policy_errors'] as $pe) {
                        $pes[] = $pe;
                    }

                    $qb = $dm->createQueryBuilder(ExtensionsQueue::class);
                    $qb->updateOne()
                        ->field('id')->equals(new ObjectId($ext->getId()))
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

                        $qb = $dm->createQueryBuilder(ExtensionsQueue::class);
                        $qb->updateOne()
                            ->field('id')->equals(new ObjectId($ext->getId()))
                            ->field('policyErrors')->push($qb->expr()->each($pes))
                            ->getQuery()
                            ->execute();
                    } else {
                        /** @var ExtensionsQueue $ext */
                        $ext = $dm->createQueryBuilder(ExtensionsQueue::class)
                            ->field('id')->equals(new ObjectId($ext->getId()))
                            ->getQuery()
                            ->getSingleResult();

                        if ($ext->getUpdate() && (strpos(AdWordsErrorDetail::errorDetail($error_message), "identical and redundant"))) {
                            continue;
                        }

                        $dm->createQueryBuilder(ExtensionsQueue::class)->updateOne()
                            ->field('id')->equals(new ObjectId($ext->getId()))
                            ->field('error')->set($error_message)
                            ->getQuery()
                            ->execute();

                        $campaignName = ProviderCampaignName::getCampaignName($dm, $this->getCache(), $ext->getCampaignId());

                        $exemptionExt = new ErrorsQueue();
                        $exemptionExt->setType(strtolower(ContentType::EXTENSION));
                        $exemptionExt->setErrorElementId(new ObjectId($ext->getId()));
                        $exemptionExt->setRawError($error_message);
                        $exemptionExt->setBackendId($ext->getKcCampaignBackendId());
                        $exemptionExt->setError(AdWordsErrorDetail::errorDetail($error_message));
                        $exemptionExt->setCampaignId(new ObjectId($ext->getCampaignId()));
                        $exemptionExt->setCampaignName($campaignName);
                        $exemptionExt->setTeId($ext->getTeId());

                        $dm->persist($exemptionExt);

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
            /**
             * Built two operations for creating new asset when first operation create new the asset to account
             * and second operation the asset which is linked to the campaign. To obtain the result with an error
             * must be sliced in half
             */
            foreach ($results['results'] as $operationIndex => $operationResponse) {
                /**
                 * Every odd element array the asset which is linked to the campaign.
                 * @var MutateOperationResponse $operationResponse
                 */
                if ($operationResponse->hasCampaignAssetResult() && $operationIndex % 2 !== 0) {
                    $update_data[$queryEntitiesIds[(int)floor($operationIndex / 2)]]
                        = CampaignAssetServiceClient::parseName($operationResponse->getCampaignAssetResult()->getResourceName())['asset_id'];
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
            /**
             * If the asset is created incorrectly, then an error will be returned in even array elements.
             * @var MutateOperationResponse $operationResponse
             */
            if ($operationResponse != NULL && $operationIndex % 2 === 0) {
                $errors[$queryEntitiesIds[(int)floor($operationIndex / 2)]] = $operationResponse;
            }
        }

        $this->registerErrors($errors);

        $this->getLogger()->info("Completed processing batch jobs results",
            [$hotsBatchJob->getOperandType(), $hotsBatchJob->getAction(),
                $hotsBatchJob->getId(), $hotsBatchJob->getSystemJobId()]);
    }

    /**
     * @param int $systemExtensionId
     * @param int $systemCampaignId
     * @param int $accountId
     * @param int $backendId
     * @param int $brandTemplateId
     *
     * @return ExtensionsQueue
     */
    public function makeItemForRemove(
        int $systemExtensionId,
        int $systemCampaignId,
        int $accountId,
        int $backendId,
        int $brandTemplateId
    ): ExtensionsQueue
    {
        $fieldsValue = [
            'systemAccount'         => $accountId,
            'systemExtensionId'     => $systemExtensionId,
            'systemCampaignId'      => $systemCampaignId,
            'brandTemplateId'       => $brandTemplateId,
            'kcCampaignBackendId'   => $backendId,
            'delete'                => true,
        ];

        $adForRemove = new ExtensionsQueue();
        $adForRemove->fill($fieldsValue);

        return $adForRemove;
    }
}