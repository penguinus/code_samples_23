<?php

namespace App\Extensions\AdSystem\Bing\ExternalWork\Bulk;

use App\Document\Ad;
use App\Document\AdsQueue;
use App\Document\ErrorsQueue;
use App\Document\KcCampaign;
use App\Entity\KcCampaign as MySqlKcCampaign;
use App\Entity\BingBatchJob;
use App\Entity\BrandTemplate;
use App\EntityRepository\BrandTemplateRepository;
use App\Enums\Ads\AdEnum;
use App\Extensions\AdSystem\Bing\ExternalWork\Auth\BingServiceManager;
use App\Extensions\AdSystem\Bing\ExternalWork\BingAd;
use App\Extensions\AdSystem\Bing\ExternalWork\BingErrorDetail;
use App\Extensions\Common\AdSystemEnum;
use App\Extensions\Common\ContentType;
use App\Extensions\Common\ExternalWork\Bulk\BatchJob;
use App\Interfaces\DocumentRepositoryInterface\BatchItemsUploadInterface;
use App\Interfaces\EntityInterface\BatchJobInterface;
use App\Providers\ProviderCampaignName;
use App\Providers\ProviderDocumentName;
use Doctrine\ODM\MongoDB\DocumentManager;
use Doctrine\ORM\EntityManagerInterface;
use MongoDB\BSON\ObjectId;
use Psr\Container\ContainerInterface;
use Psr\Log\LoggerInterface;
use Symfony\Component\Cache\Adapter\AdapterInterface;

/**
 * Class BingAdManager
 * @package App\Extensions\AdSystem\Bing\ExternalWork\Bulk
 */
class BingAdManager extends BingBatchManager
{
    public const BATCH_SIZE = 20000;

    public const OPERATION_FIELDS = [
        "type" => "Type",
        "status" => "Status",
        "id" => "Id",
        "parentId" => "Parent Id",
        "clientId" => "Client Id",
        "headline" => "Headline",
        "description" => "Description",
        "titlePart1" => "Title Part 1",
        "titlePart2" => "Title Part 2",
        "titlePart3" => "Title Part 3",
        "text" => "Text",
        "textPart2" => "Text Part 2",
        "finalUrl" => "Final Url",
        "trackingUrlTemplate" => "Tracking Template",
        "urlCustomParameters" => "Custom Parameter",
        "path1" => "Path 1",
        "path2" => "Path 2",
        "name" => "Name",
    ];

    /**
     * @var BingAd
     */
    protected BingAd $bingAd;

    /**
     * BingAdManager constructor.
     * @param ContainerInterface        $container
     * @param BingServiceManager        $serviceManager
     * @param EntityManagerInterface    $em
     * @param DocumentManager           $dm
     * @param LoggerInterface           $bingLogger
     * @param AdapterInterface          $cache
     * @param BingAd                    $bingAd
     */
    public function __construct(
        ContainerInterface      $container,
        BingServiceManager      $serviceManager,
        EntityManagerInterface  $em,
        DocumentManager         $dm,
        LoggerInterface         $bingLogger,
        AdapterInterface        $cache,
        BingAd                  $bingAd
    )
    {
        parent::__construct($container, $serviceManager, $em, $dm, $bingLogger, $cache);

        $this->bingAd = $bingAd;
    }

    /**
     * @return BingAd
     */
    protected function getBingAd() : BingAd
    {
        return $this->bingAd;
    }

    /**
     * @return string
     */
    public function getOperandType(): string
    {
        return BingBatchJob::OPERAND_TYPE_AD;
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
        return $this->getDocumentManager()->getRepository(AdsQueue::class);
    }

    /**
     * @param array $hots_ads
     * @param $customerId
     * @return array|bool
     */
    protected function buildAddOperations(array $hots_ads, $customerId)
    {
        $em = $this->getEntityManager();
        $dm = $this->getDocumentManager();

        $backendIds = array_unique(array_column($hots_ads, 'kcCampaignBackendId'));
        $campaigns = $dm->getRepository(KcCampaign::class)
            ->getCampaignLocationInfoByBackendIds($dm, $backendIds);

        /** @var MySqlKcCampaign $offerPlaceholders */
        $offerPlaceholders = $em->getRepository(MySqlKcCampaign::class)->getOfferPlaceholderByBackendIds($backendIds);

        $brandTemplateIds = array_unique(array_column($hots_ads, 'brandTemplateId'));
        $channelIdByTemplateIds = $em->getRepository(BrandTemplate::class)
            ->getChannelIdsByIds($brandTemplateIds, AdSystemEnum::BING);

        /** @var BrandTemplateRepository $placeholdersList */
        $brandPlaceholders = $em->getRepository(BrandTemplate::class)->getListPlaceholdersByIds($brandTemplateIds);

        /** @var BrandTemplateRepository $trackingUrlByTemplateIds */
        $trackingUrlByTemplateIds = $em->getRepository(BrandTemplate::class)
            ->getTrackingUrlByIds($brandTemplateIds, AdSystemEnum::BING);

        $cityId = null;
        $abbreviation = null;

        $operations = [];
        $entitiesIds = [];

        foreach ($hots_ads as $hots_ad) {
            if (!key_exists($hots_ad['brandTemplateId'], $brandPlaceholders)) {
//                print_r("Not have placeholder for " . $hots_ad['brandTemplateId']);
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
                $abbreviation = $em->getRepository('App:Criteria')
                    ->getByLocation($campaign['cityId'], AdSystemEnum::BING);
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
            $placeholders['adgroup'] = isset($hots_ad['adgroupPlaceholder']) ? $hots_ad['adgroupPlaceholder'] : '';

            $ad = $this->getBingAd()->makeByType($hots_ad, $cityId, $channelIdByTemplateIds[$hots_ad['brandTemplateId']],
                $trackingUrlByTemplateIds[$hots_ad['brandTemplateId']], $placeholders, $abbreviation);

            $operations[] = $ad;
            $entitiesIds[] = (string)$hots_ad['_id'];
        }

        unset($campaigns, $hots_ads);

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

        $ad = $this->initEmptyArrayByFields();
        foreach ($hots_entities as $hots_ad) {
            $ad['type'] = BingAd::getTitleByType($hots_ad['adType']);
            $ad['status'] = $hots_ad['status'] == 1 ? 'Active' : 'Paused';
            $ad['id'] = $hots_ad['systemAdId'];
            $ad['parentId'] = $hots_ad['systemAdgroupId'];
            $ad['clientId'] = (string)$hots_ad['_id'];

            $operations[] = $ad;
            $entitiesIds[] = (string)$hots_ad['_id'];
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

        $ad = $this->initEmptyArrayByFields();
        foreach ($hots_entities as $hots_ad) {
            $ad['type'] = BingAd::getTitleByType($hots_ad['adType']);
            $ad['status'] = 'Deleted';
            $ad['id'] = $hots_ad['systemAdId'];
            $ad['parentId'] = $hots_ad['systemAdgroupId'];
            $ad['clientId'] = (string)$hots_ad['_id'];

            $operations[] = $ad;
            $entitiesIds[] = (string)$hots_ad['_id'];
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
     */
    public function processCollectionsAfterAdd(array $resultSuccesses)
    {
        if (empty($resultSuccesses))
            return;

        ini_set("memory_limit", "7G");

        $dm = $this->getDocumentManager();

        $adsByTemplate = [];
        $campaignIds = [];
        $teIds = [];

        $_ids = array_map(function ($id) {
            return new ObjectId($id);
        }, array_keys($resultSuccesses));

        $selectFields = ['id', 'brandTemplateId', 'campaignId', 'teId', 'teAdgroupId',
            'kcCampaignBackendId', 'systemCampaignId', 'systemAdgroupId'];
        $ads = $dm->getRepository(AdsQueue::class)->getByIds($dm, $_ids, $selectFields, true);

        $adData = [];
        foreach ($ads as $ad) {
            $campaignIds[] = new ObjectId((string)$ad['campaignId']);
            $teIds[] = $ad['teId'];

            $adsByTemplate[$ad['brandTemplateId']][] = $ad;
            $adData[(string)$ad['campaignId']][$ad['teId']] = $resultSuccesses[(string)$ad['_id']];
        }
        unset($ads);

        $campaignIds = array_unique($campaignIds);
        $teIds = array_unique($teIds);

        $this->setSystemIdToAdsQueue($adData, $campaignIds, $teIds);
        unset($adData, $campaignIds, $teIds);

        $this->addItemsToMainCollections($adsByTemplate, $resultSuccesses);
        unset($adsByTemplate, $resultSuccesses);

        $dm->getRepository(AdsQueue::class)->removeByIds($dm, $_ids);
    }

    /**
     * @param array $adsByTemplate
     * @param array $resultSuccesses
     */
    protected function addItemsToMainCollections(array $adsByTemplate, array $resultSuccesses)
    {
        $dm = $this->getDocumentManager();

        $counter = 1;
        foreach ($adsByTemplate as $templateId => $ads) {
            $dm->setBrandTemplateId($templateId);

            foreach ($ads as $ad) {
                $hotsAd = new Ad();
                $hotsAd->setTeAdgroupId($ad['teAdgroupId']);
                $hotsAd->setTeId($ad['teId']);
                $hotsAd->setKcCampaignBackendId($ad['kcCampaignBackendId']);
                $hotsAd->setSystemCampaignId($ad['systemCampaignId']);
                $hotsAd->setSystemAdgroupId($ad['systemAdgroupId']);
                $hotsAd->setSystemAdId($resultSuccesses[(string)$ad['_id']]);

                $dm->persist($hotsAd);

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
     * @param array $adData
     * @param array $campaignIds
     * @param array $teIds
     * @throws
     */
    protected function setSystemIdToAdsQueue(array $adData, array $campaignIds, array $teIds)
    {
        $dm = $this->getDocumentManager();

        /**@var AdsQueue[] $ads */
        $ads = $dm->createQueryBuilder(AdsQueue::class)
            ->field('campaignId')->in($campaignIds)
            ->field('teId')->in($teIds)
            ->field('add')->exists(false)
            ->immortal()// no limit of time for cursor lifetime
            ->getQuery()
            ->execute(); // no limit of time for executing query

        foreach ($ads as $ad) {
            if (isset($adData[$ad->getCampaignId()][$ad->getTeId()]))
                $ad->setSystemAdId($adData[$ad->getCampaignId()][$ad->getTeId()]);
        }
        $dm->flush();

        foreach ($ads as $ad) {
            $dm->detach($ad);
            unset($ad);
        }
        $dm->flush();
        gc_collect_cycles();
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
        // Remove ad from Queue
        $dm->getRepository(AdsQueue::class)->removeByIds($dm, $_ids);
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

        $selectFields = ['brandTemplateId', 'systemAdId'];
        /**@var AdsQueue[] $ads */
        $ads = $dm->getRepository(AdsQueue::class)->getByIds($dm, $_ids, $selectFields, true);

        $systemIdsByTemplate = [];

        foreach ($ads as $ad) {
            $systemIdsByTemplate[$ad['brandTemplateId']] [] = $ad['systemAdId'];
        }

        foreach ($systemIdsByTemplate as $templateId => $systemIds) {
            $dm->setBrandTemplateId($templateId);
            $dm->getRepository(Ad::class)->removeBySystemIds($dm, $systemIds);
        }

        // Remove ad from Queue
        $dm->getRepository(AdsQueue::class)->removeByIds($dm, $_ids);
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

        /**@var AdsQueue[] $ads */
        $ads = $dm->getRepository(AdsQueue::class)->getByIds($dm, $_ids);

        $deletedIds = [];
        foreach ($ads as $index => $ad) {
            $error = $errors[$ad->getId()];
            if (!in_array($error['Error Number'], ['1308', '1357'])) {
                $ad->setError($error['Error']);
                $ad->setErrorCode($error['Error Number']);
            } else {
                $deletedIds[] = $ad->getTeId();
                unset($ads[$index]);
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
        foreach ($ads as $ad) {
            $errorDetail = BingErrorDetail::errorDetail($ad->getErrorCode());
            $errorDetail = !empty($errorDetail) ? $errorDetail : $ad->getError();
            $campaignName = ProviderCampaignName::getCampaignName($dm, $this->getCache(), $ad->getCampaignId());

            $adError = new ErrorsQueue();

            $adError->setType(strtolower(ContentType::AD));
            $adError->setErrorElementId(new ObjectId($ad->getId()));
            $adError->setRawError($ad->getError());
            $adError->setBackendId($ad->getKcCampaignBackendId());
            $adError->setError($errorDetail);
            $adError->setCampaignName($campaignName);
            $adError->setCampaignId(new ObjectId($ad->getCampaignId()));
            $adError->setAdgroup($ad->getAdgroup());
            $adError->setHeadline($ad->getHeadline());
            $adError->setTeId($ad->getTeId());
            $adError->setTeAdgroupId($ad->getTeAdgroupId());

            $dm->persist($adError);

            $dm->detach($ad);
            unset($ad);

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
     * @param BatchJobInterface $hotsBatchJob
     * @throws
     */
    protected function processResults(array $results, BatchJobInterface $hotsBatchJob)
    {
        $queryEntitiesIds = $this->jsonDecodeMetaData($hotsBatchJob);

        if ($hotsBatchJob->getAction() == BatchJob::ACTION_ADD) {
            // Success add operations processing
            $this->processCollectionsAfterAdd($results['results']);
            // Register errors in queues
            $this->registerErrors($results['errors']);

        } elseif ($hotsBatchJob->getAction() == BatchJob::ACTION_UPDATE) {
            // Success update operations processing
            $queryEntitiesIds = array_diff($queryEntitiesIds, array_keys($results['errors']));
            $this->processCollectionsAfterUpdate($queryEntitiesIds);
            // Register errors in queues
            $this->registerErrors($results['errors']);

        } elseif ($hotsBatchJob->getAction() == BatchJob::ACTION_REMOVE) {
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