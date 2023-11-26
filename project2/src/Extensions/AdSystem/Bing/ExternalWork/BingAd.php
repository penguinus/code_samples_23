<?php

namespace App\Extensions\AdSystem\Bing\ExternalWork;

use App\Document\AdsQueue;
use App\Enums\Ads\AdEnum;
use App\Extensions\AdSystem\Bing\ExternalWork\Auth\BingServiceManager;
use App\Extensions\AdSystem\Bing\ExternalWork\Bulk\BingAdManager;
use App\Extensions\Common\AdSystemEnum;
use App\Extensions\Common\ExternalWork\Core\AdAbstract;
use App\Providers\ProviderEntityName;
use Doctrine\ODM\MongoDB\DocumentManager;
use Doctrine\ODM\MongoDB\MongoDBException;
use Doctrine\ORM\{EntityManager, EntityManagerInterface};
use Microsoft\BingAds\V13\CampaignManagement\AdType;
use Microsoft\BingAds\V13\CampaignManagement\GetAdsByAdGroupIdRequest;
use Psr\Container\ContainerInterface;
use SoapFault;
use Symfony\Contracts\Service\ServiceSubscriberInterface;

/**
 * Class BingAd
 * @package App\Extensions\AdSystem\Bing\ExternalWork
 */
class BingAd extends AdAbstract implements ServiceSubscriberInterface
{
    /**
     * @var ContainerInterface
     */
    protected ContainerInterface $container;

    /**
     * @var string
     */
    private string $adSystem = AdSystemEnum::BING;

    /**
     * @var string
     */
    public const ETA_TITLE = 'Expanded Text Ad';

    /**
     * @var string
     */
    public const RSA_TITLE = 'Responsive Search Ad';

    /**
     * @var array
     */
    public const RSA_QUEUE_BING_HEADLINE_PIN_FIELDS_MAP = [
        'pinFirstHeadlines'     => "Headline1",
        'pinSecondHeadlines'    => "Headline2",
        'pinThirdHeadlines'     => "Headline3",
    ];

    /**
     * @var array
     */
    public const RSA_QUEUE_BING_DESCRIPTION_PIN_FIELDS_MAP = [
        'pinFirstDescriptions'  => 'Description1',
        'pinSecondDescriptions' => 'Description2',
    ];

    /**
     * @var array
     */
    public const BING_AD_TYPES = [AdType::ResponsiveSearch];

    /**
     * @var int[]
     */
    public const HOTS_BING_AD_TYPES = [
        AdType::ResponsiveSearch => AdEnum::TYPE_RSA
    ];

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
            'bing.service_manager'  => BingServiceManager::class,
            'doctrine'              => EntityManagerInterface::class,
            'doctrine_mongodb'      => DocumentManager::class,
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
     * @return DocumentManager
     */
    protected function getDocumentManager(): DocumentManager
    {
        if (!isset($this->dm)) {
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
     * @return EntityManagerInterface
     */
    protected function getEntityManager(): EntityManagerInterface
    {
        return $this->get('doctrine');
    }

    /**
     * @return BingServiceManager
     */
    protected function getBingServiceManager(): BingServiceManager
    {
        return $this->get('bing.service_manager');
    }

    /**
     * @return array|mixed
     */
    public static function getRSAHeadLinePinFieldsMap(): array
    {
        return self::RSA_QUEUE_BING_HEADLINE_PIN_FIELDS_MAP;
    }

    /**
     * @return array|mixed
     */
    public static function getRSAQueueDescriptionPinFieldsMap(): array
    {
        return self::RSA_QUEUE_BING_DESCRIPTION_PIN_FIELDS_MAP;
    }

    /**
     * @param int $type
     * @return string
     * @throws \Exception
     */
    public static function getTitleByType(int $type): string
    {
        switch ($type) {
            case AdEnum::TYPE_RSA:
                return self::RSA_TITLE;
            case AdEnum::TYPE_ETA:
                return self::ETA_TITLE;
            default:
                throw new \Exception("Unknown Ad type.");
        }
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
     * @return array
     * @throws \Exception
     */
    public function makeByType(
        array   $queueAd,
        int     $cityId,
        int     $channelId,
        ?string $trackingUrl,
        array   $placeholders,
        ?array  $abbreviation = null): array
    {
        $ad = $this->makeRSABase($queueAd, $placeholders, $abbreviation);

        $path2 = self::pathPlaceholdersReplacement($queueAd['path2'], $placeholders['city'], $abbreviation);

        $finalUrl = self::applyParamsForUrl($queueAd['destinationUrl'], $queueAd['kcCampaignBackendId'],
            $queueAd['adgroupId'], $channelId, $cityId);

        $ad['parentId'] = $queueAd['systemAdgroupId'];
        $ad['clientId'] = (string)$queueAd['_id'];
        $ad['path1']    = $queueAd['path1'];
        $ad['path2']    = $path2;
        $ad['status']   = $queueAd['status'] == 1 ? 'Active' : 'Paused';

        if (!empty($finalUrl))
            $ad['finalUrl'] = $finalUrl;

        if (!is_null($trackingUrl)) {
            $ad['trackingUrlTemplate'] = $trackingUrl;
        }

        return $ad;
    }

    /**
     * @param array $queueAd
     * @param array $placeholders
     * @param array|null $abbreviation
     * @return array
     * @throws \Exception
     */
    private function makeRSABase(array $queueAd, array $placeholders, ?array $abbreviation = null): array
    {
        $ad = BingAdManager::initEmptyArrayByFields();

        $ad['type'] = self::RSA_TITLE;
        # processing headlines and descriptions
        $processedFields = $this->processingRSAFields($queueAd, $placeholders, $abbreviation);

        foreach (['headline', 'description'] as $field) {
            if (!empty($processedFields[$field . "s"])) {
                // Option JSON_UNESCAPED_UNICODE encode multibyte Unicode characters literally
                // Encode special characters in right way. For example '®'.
                $ad[$field] = stripslashes(json_encode($processedFields[$field . "s"], JSON_UNESCAPED_UNICODE));
            } else {
                throw new \Exception("Missing $field field in Responsive Search Ad.");
            }
        }

        return $ad;
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
     * @throws MongoDBException
     */
    public function findByParentIdsAndRemove(
        array   $systemAdgroupIds,
        int     $accountId,
        int     $backendId,
        int     $brandTemplateId
    ): bool {

        $em = $this->getEntityManager();
        $dm = $this->getDocumentManager();

        /** @var \App\Entity\BingAccount $account */
        $account    = $em->getRepository(ProviderEntityName::getForAccountsBySystem($this->adSystem))->find($accountId);
        $customerId = $account->getSystemAccountId();

        $chunksAdgroupIds   = array_chunk($systemAdgroupIds, 100);
        $amountChunks       = count($systemAdgroupIds);

        $counterOperations = 0;
        foreach ($systemAdgroupIds as $systemAdgroupId) {
            $adsByAdGroupIdRequest = new GetAdsByAdGroupIdRequest();
            $adsByAdGroupIdRequest->AdGroupId = $systemAdgroupId;
            $adsByAdGroupIdRequest->AdTypes = BingAd::BING_AD_TYPES;

            $campaignService = $this->getBingServiceManager()->getCampaignManagementService($customerId);

            try {
                $systemAdsByAdGroupIdRequest = $campaignService->GetService()->GetAdsByAdGroupId($adsByAdGroupIdRequest);

            } catch (SoapFault $e) {
                print "\nLast SOAP request/response:\n";
                printf("Fault Code: %s\nFault String: %s\n", $e->faultcode, $e->faultstring);

                if (!$systemAdsByAdGroupIdRequest = $campaignService->GetService()->GetAdsByAdGroupId($adsByAdGroupIdRequest)) {
                    printf("Skip Bing search AdIds by AdGroupId: %s\n", $systemAdgroupId);

                    continue;
                }
            }

            if (!property_exists($systemAdsByAdGroupIdRequest->Ads, 'Ad')) {
                continue;
            }

            $systemAdRequest = $systemAdsByAdGroupIdRequest->Ads->Ad;
            $elementCount = count($systemAdRequest);

            foreach ($systemAdRequest as $ad) {
                $adGroupId = $systemAdgroupId;
                $adId = $ad->Id;

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