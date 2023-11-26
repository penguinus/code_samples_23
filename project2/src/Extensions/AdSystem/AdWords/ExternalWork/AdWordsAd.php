<?php

namespace App\Extensions\AdSystem\AdWords\ExternalWork;

use App\Document\AdsQueue;
use App\Entity\{AdwordsAccount, BingAccount};
use App\Enums\Ads\AdEnum;
use App\Extensions\AdSystem\AdWords\ExternalWork\Auth\AdWordsServiceManager;
use App\Extensions\Common\AdSystemEnum;
use App\Extensions\Common\ExternalWork\Core\AdAbstract;
use App\Providers\ProviderEntityName;
use Doctrine\ODM\MongoDB\{DocumentManager, MongoDBException};
use Doctrine\ORM\{EntityManager, EntityManagerInterface};
use Google\Ads\GoogleAds\Util\V13\ResourceNames;
use Google\Ads\GoogleAds\V13\Common\{AdTextAsset, ResponsiveSearchAdInfo};
use Google\Ads\GoogleAds\V13\Enums\AdTypeEnum\AdType;
use Google\Ads\GoogleAds\V13\Enums\ServedAssetFieldTypeEnum\ServedAssetFieldType;
use Google\Ads\GoogleAds\V13\Resources\Ad;
use Google\Ads\GoogleAds\V13\Services\{AdGroupAdOperation, GoogleAdsRow};
use Google\ApiCore\{ApiException, ValidationException};
use Psr\Container\ContainerInterface;
use Symfony\Contracts\Service\ServiceSubscriberInterface;

/**
 * Class AdWordsAd
 *
 * @package App\Extensions\AdSystem\AdWords\ExternalWork
 */
class AdWordsAd extends AdAbstract implements ServiceSubscriberInterface
{
    public const RSA_QUEUE_GOOGLE_HEADLINE_PIN_FIELDS_MAP = [
        'pinFirstHeadlines'     => ServedAssetFieldType::HEADLINE_1,
        'pinSecondHeadlines'    => ServedAssetFieldType::HEADLINE_2,
        'pinThirdHeadlines'     => ServedAssetFieldType::HEADLINE_3,
    ];

    public const RSA_QUEUE_GOOGLE_DESCRIPTION_PIN_FIELDS_MAP = [
        'pinFirstDescriptions'  => ServedAssetFieldType::DESCRIPTION_1,
        'pinSecondDescriptions' => ServedAssetFieldType::DESCRIPTION_2,
    ];

    /** @var array */
    public const GOOGLE_AD_TYPES = [AdType::RESPONSIVE_SEARCH_AD];

    /** @var int[] */
    public const HOTS_GOOGLE_AD_TYPES = [
        AdEnum::TYPE_RSA => 'ResponsiveSearchAd'
    ];

    /**
     * @var ContainerInterface
     */
    protected ContainerInterface $container;
    /**
     * @var DocumentManager
     */
    private DocumentManager $dm;

    /**@var string */
    private string $adSystem = AdSystemEnum::ADWORDS;

    /**
     * @param ContainerInterface $container
     */
    public function __construct(ContainerInterface $container)
    {
        $this->container = $container;

    }

    /**
     * @return array
     */
    public static function getSubscribedServices(): array
    {
        return [
            'adwords.service_manager'   => AdWordsServiceManager::class,
            'doctrine'                  => EntityManagerInterface::class,
            'doctrine_mongodb'          => DocumentManager::class,
        ];
    }

    /**
     * @param $id
     * @return mixed
     */
    protected function get($id)
    {
        return $this->container->get($id);
    }

    /**
     * @return AdwordsServiceManager
     */
    protected function getGoogleServiceManager(): AdWordsServiceManager
    {
        return $this->get('adwords.service_manager');
    }

    /**
     * @return DocumentManager
     */
    protected function getDocumentManager(): DocumentManager
    {
        if(!isset($this->dm)) {
            /** @var DocumentManager $dm */
            $dm = $this->get('doctrine_mongodb');
            $dm->setAdSystem($this->adSystem);

            $this->dm = $dm;

            return $dm;
        } else {
            return $this->dm;
        }
    }

    /**
     * @return EntityManager
     */
    protected function getEntityManager(): EntityManagerInterface
    {
        return $this->get('doctrine');
    }

    /**
     * @return array
     */
    public static function getRSAHeadLinePinFieldsMap(): array
    {
        return self::RSA_QUEUE_GOOGLE_HEADLINE_PIN_FIELDS_MAP;
    }

    /**
     * @return array
     */
    public static function getRSAQueueDescriptionPinFieldsMap(): array
    {
        return self::RSA_QUEUE_GOOGLE_DESCRIPTION_PIN_FIELDS_MAP;
    }


    /**
     * $placeholders contains placeholders for replacements in brand, offer, ad group, city, state
     *
     * @param array         $queueAd
     * @param int           $cityId
     * @param int           $channelId
     * @param string|null   $trackingUrl
     * @param array         $placeholders
     * @param array|null    $abbreviation
     *
     * @return Ad
     * @throws \Exception
     */
    public function makeByType(
        array   $queueAd,
        int     $cityId,
        int     $channelId,
        ?string $trackingUrl,
        array   $placeholders,
        ?array  $abbreviation = null
    ): Ad
    {
        $adByType = $this->makeRSABase($queueAd, $placeholders, $abbreviation);

        $path2 = self::pathPlaceholdersReplacement($queueAd['path2'], $placeholders['city'], $abbreviation);

        $finalUrl = self::applyParamsForUrl($queueAd['destinationUrl'], $queueAd['kcCampaignBackendId'],
            $queueAd['adgroupId'], $channelId, $cityId);

        $adByType->setPath1($queueAd['path1']);
        $adByType->setPath2($path2);

        $ad = new Ad();
        $ad->{'set'. self::HOTS_GOOGLE_AD_TYPES[$queueAd['adType']]}($adByType);

        if (!empty($finalUrl))
            $ad->setFinalUrls([$finalUrl]);

        if (!is_null($trackingUrl)) {
            $ad->setTrackingUrlTemplate($trackingUrl);
        }

        return $ad;
    }

    /**
     * @param array         $queueAd
     * @param array         $placeholders
     * @param array|null    $abbreviation
     *
     * @return ResponsiveSearchAdInfo
     * @throws \Exception
     */
    private function makeRSABase(array $queueAd, array $placeholders, ?array $abbreviation = null): ResponsiveSearchAdInfo
    {
        $responsiveSearchAdInfo = new ResponsiveSearchAdInfo();

        # processing headlines and descriptions
        $processedFields = $this->processingRSAFields($queueAd, $placeholders, $abbreviation);
        foreach (['headlines', 'descriptions'] as $field) {
            if (!empty($processedFields[$field])) {
                $values = [];

                foreach ($processedFields[$field] as $item) {
                    $fieldText = new AdTextAsset();
                    $fieldText->setText($item['text']);

                    if (isset($item['pinnedField'])) {
                        $fieldText->setPinnedField($item['pinnedField']);
                    }

                    $values[] = $fieldText;
                }

                $responsiveSearchAdInfo->{'set' . ucfirst($field)}($values);
            } else {
                throw new \Exception("Missing $field field in Responsive Search Ad.");
            }
        }

        return $responsiveSearchAdInfo;
    }

    /**
     * @param int $customerId
     * @param int $adId
     * @param int $adGroupId
     *
     * @return AdGroupAdOperation
     */
    public function makeDeleteItemOperation(int $customerId, int $adId, int $adGroupId): AdGroupAdOperation
    {
        // Creates ad group ad resource name.
        $adGroupAdResourceName = ResourceNames::forAdGroupAd($customerId, $adGroupId, $adId);

        // Constructs an operation that will remove the ad with the specified resource name.
        $adGroupAdOperation = new AdGroupAdOperation();
        $adGroupAdOperation->setRemove($adGroupAdResourceName);

        return $adGroupAdOperation;
    }

    /**
     * @param int                   $customerId
     * @param AdGroupAdOperation[]  $adGroupAdOperations
     *
     * @return bool
     * @throws \Exception
     */
    public function pushOperations(int $customerId, array $adGroupAdOperations): bool
    {
        try {
            // Issues a mutate request to remove the ad group ad.
            $adGroupAdServiceClient = $this->getGoogleServiceManager()->getAdGroupAdServiceClient();
            $response = $adGroupAdServiceClient->mutateAdGroupAds($customerId, $adGroupAdOperations);

            foreach ($response->getResults() as $result) {
                printf(
                    "Removed ad group ad with resource name: '%s'%s",
                    $result->getResourceName(),
                    PHP_EOL
                );
            }

            return true;
        } catch (ApiException $apiException) {
            foreach ($apiException->getMetadata() as $metadatum) {
                foreach ($metadatum['errors'] as $error) {
                    printf('ApiException was thrown with message "%s"',
                        $error['message']. PHP_EOL
                    );
                }
            }

            return false;
        }
    }

    /**
     * Remove ads from AdWords by Campaign or AdGroup Level
     *
     * @param array $systemAdgroupIds
     * @param int   $accountId
     * @param int   $backendId
     * @param int   $brandTemplateId
     *
     * @return bool
     * @throws ApiException | ValidationException | MongoDBException
     */
    public function findByParentIdsAndRemove(
        array   $systemAdgroupIds,
        int     $accountId,
        int     $backendId,
        int     $brandTemplateId
    ): bool {

        $em = $this->getEntityManager();
        $dm = $this->getDocumentManager();

        /** @var AdwordsAccount|BingAccount $account */
        $account    = $em->getRepository(ProviderEntityName::getForAccountsBySystem($this->adSystem))->find($accountId);
        $customerId = $account->getSystemAccountId();

        $chunksAdgroupIds   = array_chunk($systemAdgroupIds, AdWordsServiceManager::SELECT_IN_LIMIT);

        $counterOperations = 0;
        foreach ($systemAdgroupIds as $systemAdgroupId) {
            // Creates a query that retrieves ads.
            $query = sprintf( /** @lang text */
                "SELECT ad_group.id, ad_group_ad.ad.id 
                FROM ad_group_ad 
                WHERE ad_group_ad.status != 'REMOVED' 
                AND ad_group.id = '%s'",
                $systemAdgroupId
            );

            $googleAdsServiceClient = $this->getGoogleServiceManager()->getGoogleAdsServiceClient();

            // Issues a search request by specifying page size.
            $response = $googleAdsServiceClient
                ->search($customerId, $query, ['pageSize' => $this->getGoogleServiceManager()::PAGE_SIZE]);

            $elementCount = $response->getPage()->getPageElementCount();
            // Iterates over all rows in all pages and prints the requested field values for
            // the expanded text ad in each row.
            foreach ($response->iterateAllElements() as $googleAdsRow) {
                /** @var GoogleAdsRow $googleAdsRow */
                $adGroupId = $googleAdsRow->getAdGroup()->getId();
                $adId = $googleAdsRow->getAdGroupAd()->getAd()->getId();

                $adForRemove = $this->makeItemForRemove($adId, $adGroupId, $accountId, $backendId, $brandTemplateId);
                $adForRemove->setError("Temporary");

                $dm->persist($adForRemove);

                $counterOperations++;
                if ($counterOperations % 500 == 0) {
                    $dm->flush();
                    $dm->clear();

                    printf("\r\r\r\r\r\r%d  -  %d", $elementCount, $counterOperations);
                }
            }

            $dm->flush();
            $dm->clear();

            $counterOperations = 0;
        }

        $where      = ['kcCampaignBackendId' => $backendId];
        $attributes = ['error' => null];

        $dm->getRepository(AdsQueue::class)->updateManyByAttributes($dm, $where, $attributes);

        print("\nFinished removing\n\n");

        return true;
    }

    /**
     * @param int $systemAdId
     * @param int $systemAdgroupId
     * @param int $accountId
     * @param int $backendId
     * @param int $brandTemplateId
     *
     * @return AdsQueue
     */
    public function makeItemForRemove(
        int $systemAdId,
        int $systemAdgroupId,
        int $accountId,
        int $backendId,
        int $brandTemplateId
    ): AdsQueue
    {
        $fieldsValue = [
            'systemAccount'         => $accountId,
            'systemAdgroupId'       => $systemAdgroupId,
            'systemAdId'            => $systemAdId,
            'brandTemplateId'       => $brandTemplateId,
            'kcCampaignBackendId'   => $backendId,
            'delete'                => true,
        ];

        $adForRemove = new AdsQueue();
        $adForRemove->fill($fieldsValue);

        return $adForRemove;
    }
}