<?php

namespace App\Extensions\AdSystem\AdWords\ExternalWork;

use App\Entity\BrandExtension;
use App\Extensions\AdSystem\AdWords\ExternalWork\Auth\AdWordsServiceManager;
use App\Extensions\Common\AdSystemEnum;
use App\Extensions\Common\InternalWork\Basic\ExtensionManager;
use App\Services\Sync\SyncChangesWithCampaigns;
use Doctrine\ODM\MongoDB\DocumentManager;
use Google\Ads\GoogleAds\V13\Common\{CallAsset, CalloutAsset, Money, PromotionAsset, SitelinkAsset, StructuredSnippetAsset};
use Google\Ads\GoogleAds\V13\Enums\CallConversionReportingStateEnum\CallConversionReportingState;
use Google\Ads\GoogleAds\V13\Enums\PromotionExtensionDiscountModifierEnum\PromotionExtensionDiscountModifier;
use Google\Ads\GoogleAds\V13\Enums\PromotionExtensionOccasionEnum\PromotionExtensionOccasion;
use Google\Ads\GoogleAds\V13\Resources\Asset;
use Google\Ads\GoogleAds\V13\Services\GoogleAdsRow;
use Google\ApiCore\ApiException;
use Google\ApiCore\ValidationException;
use Psr\Container\ContainerInterface;

/**
 * Class AdWordsExtension
 *
 * @package App\Extensions\AdSystem\AdWords\ExternalWork
 */
class AdWordsExtension extends ExtensionManager
{
    /**
     * @return array
     */
    public function getMethodsListForExtensions(): array
    {
        return [
            BrandExtension::SITELINK       => 'SitelinkAsset',
            BrandExtension::PHONE          => 'CallAsset',
            BrandExtension::CALLOUT        => 'CalloutAsset',
            BrandExtension::STRUCTURED     => 'StructuredSnippetAsset',
            BrandExtension::PROMOTIONAL    => 'PromotionAsset',
        ];
    }
    
    /**
     * @var array
     */
    private array $promotionsMethods = [
        BrandExtension::MONETARY_DISCOUNT       => 'setMoneyAmountOff',
        BrandExtension::PERCENT_DISCOUNT        => 'setPercentOff',
        BrandExtension::UP_TO_MONETARY_DISCOUNT => 'setMoneyAmountOff',
        BrandExtension::UP_TO_PERCENT_DISCOUNT  => 'setPercentOff',
    ];

    /**
     * @var array
     */
    private array $promotionsMethodsForDetail = [
        BrandExtension::PROMOTION_ON_ORDERS_OVER_DETAIL => 'setOrdersOverAmount',
        BrandExtension::PROMOTION_PROMO_CODE_DETAIL     => 'setPromotionCode',
    ];

    /**
     * @param ContainerInterface        $container
     * @param DocumentManager           $dm
     * @param SyncChangesWithCampaigns  $syncChangesWithCampaigns
     */
    public function __construct(
        ContainerInterface          $container,
        DocumentManager             $dm,
        SyncChangesWithCampaigns    $syncChangesWithCampaigns
    ) {
        parent::__construct(AdSystemEnum::ADWORDS, $container, $dm, $syncChangesWithCampaigns);
    }

    /**
     * @return AdWordsServiceManager
     */
    private function getGoogleServiceManager(): AdWordsServiceManager
    {
        return $this->get('adwords.service_manager');
    }


    /**
     * @param $promotionType
     *
     * @return string
     */
    private function getMethodNameByPromotionType($promotionType): string
    {
        return key_exists($promotionType, $this->promotionsMethods) ? $this->promotionsMethods[$promotionType] : '';
    }

    /**
     * @param $promotionDetail
     *
     * @return string|bool
     */
    private function getMethodNameByPromotionDetail($promotionDetail)
    {
        if (key_exists($promotionDetail, $this->promotionsMethodsForDetail)) {
            return $this->promotionsMethodsForDetail[$promotionDetail];
        }

        return false;
    }

    /**
     * @param array     $extension
     * @param int|null  $channelId
     *
     * @return Asset
     * @throws \Exception
     */
    public function makeByType(array $extension, ?int $channelId): Asset
    {
        /** @var Asset $asset */
        $asset = $this->{'create' . $this->getMethodsListForExtensions()[$extension['category']]}($extension);
        
        if ($extension['category'] == BrandExtension::PROMOTIONAL || $extension['category'] == BrandExtension::SITELINK) {
            $asset->setFinalUrls([
                str_replace('?ctid={1}&chid={2}&cyid={3}', '', $extension['destinationUrl']
                . '?ctid=' . $extension['kcCampaignBackendId']
                . '&chid=' . $channelId)
            ]);

            if (isset($queueExtension['trakingTemplate'])) {
                $asset->setTrackingUrlTemplate($queueExtension['trakingTemplate']);
            }

        }

        return $asset;
    }

    /**
     * @param array $extension
     *
     * @return Asset
     */
    public function createSitelinkAsset(array $extension): Asset
    {
        $siteLinkAsset = new SitelinkAsset();
        $siteLinkAsset->setLinkText($extension['name']);
        $siteLinkAsset->setDescription1($extension['desc1']);
        $siteLinkAsset->setDescription2($extension['desc2']);

        // Wraps the sitelinks in an Asset and sets the URLs.
        $asset = new Asset();
        $asset->setSitelinkAsset($siteLinkAsset);

        return $asset;
    }

    /**
     * @param array $extension
     *
     * @return Asset
     */
    public function createCallAsset(array $extension): Asset
    {
        $callAsset = new CallAsset();
        $callAsset->setCountryCode('US');
        $callAsset->setPhoneNumber($extension['phone']);

        if (!isset($extension['platformTargeting'])) {
            $callAsset->setCallConversionReportingState(CallConversionReportingState::DISABLED);
        } else {
            $callAsset->setCallConversionReportingState(
                CallConversionReportingState::USE_RESOURCE_LEVEL_CALL_CONVERSION_ACTION);
        }

        $asset = new Asset();
        $asset->setCallAsset($callAsset);

        return $asset;
    }

    /**
     * @param array $extension
     *
     * @return Asset
     */
    public function createCalloutAsset(array $extension): Asset
    {
        $calloutAsset = new CalloutAsset();
        $calloutAsset->setCalloutText($extension['callout']);

        $asset = new Asset();
        $asset->setCalloutAsset($calloutAsset);

        return $asset;
    }

    /**
     * @param array $extension
     *
     * @return Asset
     */
    public function createStructuredSnippetAsset(array $extension): Asset
    {
        $callouts = explode(",", $extension['callout']);
        foreach ($callouts as $i => $callout) {
            if (!$callout) {
                unset($callouts[$i]);
            }
        }

        // A Structured Snippet asset.
        $structureSnippetAsset = new StructuredSnippetAsset();
        $structureSnippetAsset->setHeader($extension['platformTargeting']);
        $structureSnippetAsset->setValues($callouts);

        $asset = new Asset();
        $asset->setStructuredSnippetAsset($structureSnippetAsset);

        return $asset;
    }

    /**
     * @param array $extension
     *
     * @return Asset
     * @throws \Exception
     */
    public function createPromotionAsset(array $extension): Asset
    {
        $promotionAsset = new PromotionAsset();
        $promotionAsset->setPromotionTarget($extension['name']);

        $methodName = $this->getMethodNameByPromotionType($extension['promotionType']);
        
        if (method_exists($promotionAsset, $methodName)) {
            $promotionAsset->{$methodName}($this->getValueByPromotionType($extension));
        }

        if (in_array(
            $extension['promotionType'],
            [BrandExtension::UP_TO_MONETARY_DISCOUNT, BrandExtension::UP_TO_PERCENT_DISCOUNT]
        )) {
            $promotionAsset->setDiscountModifier(PromotionExtensionDiscountModifier::UP_TO);
        }

        if (isset($extension['promotionDateBegin'])) {
            $startDateTime = new \DateTime($extension['promotionDateBegin']);
            $promotionAsset->setStartDate($startDateTime->format('Y-m-d'));
        }

        if (isset($extension['promotionDateFinish'])) {
            $endDateTime = new \DateTime($extension['promotionDateFinish']);
            $promotionAsset->setEndDate($endDateTime->format('Y-m-d'));
        }

        if (isset($extension['occasion'])) {
            $promotionAsset->setOccasion(PromotionExtensionOccasion::value($extension['occasion']));
        }

        if (isset($extension['promotionDetail'])) {
            if ($methodNameForDetail = $this->getMethodNameByPromotionDetail($extension['promotionDetail'])) {
                $promotionAsset->{$methodNameForDetail}($this->getValueByPromotionDetail($extension));
            }
        }

        $promotionAsset->setLanguageCode('en-US');

        // Creates the Promotion asset.
        $asset = new Asset();
        $asset->setName($extension['name']);
        $asset->setPromotionAsset($promotionAsset);

        return $asset;

    }

    /**
     * @param array $extension
     *
     * @return false|Money|mixed
     */
    private function getValueByPromotionDetail(array $extension)
    {
        if ($extension['promotionDetail'] == BrandExtension::PROMOTION_ON_ORDERS_OVER_DETAIL) {
            $money = new Money();
            $money->setAmountMicros($extension['promotionTypeValue'] * 1000000);
            $money->setCurrencyCode('USD');

            return $money;
        } elseif ($extension['promotionDetail'] == BrandExtension::PROMOTION_PROMO_CODE_DETAIL) {
            return $extension['promotionTypeValue'];
        }

        return false;
    }

    /**
     * @param array $extension
     *
     * @return float|Money|int|string
     */
    private function getValueByPromotionType(array $extension)
    {
        if (in_array(
            $extension['promotionType'],
            [BrandExtension::MONETARY_DISCOUNT, BrandExtension::UP_TO_MONETARY_DISCOUNT]
        )) {
            $money = new Money();
            $money->setAmountMicros($extension['promotionTypeValue'] * 1000000);
            $money->setCurrencyCode('USD');

            return $money;
        } elseif (in_array(
            $extension['promotionType'],
            [BrandExtension::PERCENT_DISCOUNT, BrandExtension::UP_TO_PERCENT_DISCOUNT]
        )) {
            $value = $extension['promotionTypeValue'];

            if ($value > 100) {
                $value = 100;
            }

            return $value * 1000000;
        }

        return 'Error!!!!';
    }

    /**
     * @param $customerId
     * @param $resourceNames
     *
     * @return array
     * @throws ApiException|ValidationException
     */
    public function getSiteLinkAsset($customerId, $resourceNames): array
    {
        $query = "SELECT asset.name, asset.sitelink_asset.link_text, asset.sitelink_asset.description1, "
            . "asset.sitelink_asset.description2 "
            . "FROM asset WHERE asset.type = 'SITELINK' "
            . "AND asset.resource_name IN ('".$resourceNames."')";

        // Issues a search request by specifying page size.
        $response = $this->getGoogleServiceManager()->getGoogleAdsServiceClient()->search(
            $customerId,
            $query,
            ['pageSize' => $this->getGoogleServiceManager()::PAGE_SIZE, 'returnTotalResultsCount' => true]
        );

        $siteLinkFields = [];
        /** @var GoogleAdsRow $googleAdsRow */
        foreach ($response->iterateAllElements() as $googleAdsRow) {
            $siteLinkFields[$googleAdsRow->getAsset()->getResourceName()]['linkText']
                = $googleAdsRow->getAsset()->getSitelinkAsset()->getLinkText();
            $siteLinkFields[$googleAdsRow->getAsset()->getResourceName()]['description1']
                = $googleAdsRow->getAsset()->getSitelinkAsset()->getDescription1();
            $siteLinkFields[$googleAdsRow->getAsset()->getResourceName()]['description2']
                = $googleAdsRow->getAsset()->getSitelinkAsset()->getDescription2();
        }

        return $siteLinkFields;
    }

    /**
     * @param $customerId
     * @param $resourceNames
     *
     * @return array
     * @throws ApiException|ValidationException
     */
    public function getCalloutAsset($customerId, $resourceNames): array
    {
        $query = "SELECT asset.name, asset.callout_asset.callout_text "
            . "FROM asset WHERE asset.type = 'CALLOUT' "
            . "AND asset.resource_name IN ('".$resourceNames."')";

        // Issues a search request by specifying page size.
        $response = $this->getGoogleServiceManager()->getGoogleAdsServiceClient()->search(
            $customerId,
            $query,
            ['pageSize' => $this->getGoogleServiceManager()::PAGE_SIZE, 'returnTotalResultsCount' => true]
        );

        $calloutText = [];
        /** @var GoogleAdsRow $googleAdsRow */
        foreach ($response->iterateAllElements() as $googleAdsRow) {
            $calloutText[$googleAdsRow->getAsset()->getResourceName()]
                = $googleAdsRow->getAsset()->getCalloutAsset()->getCalloutText();

        }

        return $calloutText;
    }

    /**
     * @param $customerId
     * @param $resourceNames
     *
     * @return array
     * @throws ApiException|ValidationException
     */
    public function getStructuredSnippetAsset($customerId, $resourceNames): array
    {
        $query = "SELECT asset.name, asset.structured_snippet_asset.header, asset.structured_snippet_asset.values "
            . "FROM asset WHERE asset.type = 'STRUCTURED_SNIPPET' "
            . "AND asset.resource_name IN ('".$resourceNames."')";

        // Issues a search request by specifying page size.
        $response = $this->getGoogleServiceManager()->getGoogleAdsServiceClient()->search(
            $customerId,
            $query,
            ['pageSize' => $this->getGoogleServiceManager()::PAGE_SIZE, 'returnTotalResultsCount' => true]
        );

        $structuredSnippets = [];
        /** @var GoogleAdsRow $googleAdsRow */
        foreach ($response->iterateAllElements() as $googleAdsRow) {
            foreach ($googleAdsRow->getAsset()->getStructuredSnippetAsset()->getValues() as $structuredSnippet) {
                $structuredSnippets[$googleAdsRow->getAsset()->getResourceName()][] = $structuredSnippet;
            }
        }

        return $structuredSnippets;
    }

    /**
     * @param $customerId
     * @param $resourceNames
     *
     * @return array
     * @throws ApiException|ValidationException
     */
    public function getPromotionAsset($customerId, $resourceNames): array
    {
        $query = "SELECT asset.name, asset.promotion_asset.promotion_target "
            . "FROM asset WHERE asset.type = 'PROMOTION' "
            . "AND asset.resource_name IN ('".$resourceNames."')";

        // Issues a search request by specifying page size.
        $response = $this->getGoogleServiceManager()->getGoogleAdsServiceClient()->search(
            $customerId,
            $query,
            ['pageSize' => $this->getGoogleServiceManager()::PAGE_SIZE, 'returnTotalResultsCount' => true]
        );

        $promotion = [];
        /** @var GoogleAdsRow $googleAdsRow */
        foreach ($response->iterateAllElements() as $googleAdsRow) {
            $promotion[$googleAdsRow->getAsset()->getResourceName()]
                = $googleAdsRow->getAsset()->getPromotionAsset()->getPromotionTarget();
        }

        return $promotion;
    }

    /**
     * @param $customerId
     * @param $resourceNames
     *
     * @return array
     * @throws ApiException|ValidationException
     */
    public function getCallAsset($customerId, $resourceNames): array
    {
        $query = "SELECT asset.name, asset.call_asset.phone_number "
            . "FROM asset WHERE asset.type = 'CALL' "
            . "AND asset.resource_name IN ('".$resourceNames."')";

        // Issues a search request by specifying page size.
        $response = $this->getGoogleServiceManager()->getGoogleAdsServiceClient()->search(
            $customerId,
            $query,
            ['pageSize' => $this->getGoogleServiceManager()::PAGE_SIZE, 'returnTotalResultsCount' => true]
        );

        $call = [];
        /** @var GoogleAdsRow $googleAdsRow */
        foreach ($response->iterateAllElements() as $googleAdsRow) {
            $call[$googleAdsRow->getAsset()->getResourceName()]
                = $googleAdsRow->getAsset()->getCallAsset()->getPhoneNumber();
        }

        return $call;
    }

    public function syncExtensionsWithAdSystem()
    {
        // TODO: Implement syncExtensionsWithAdSystem() method.
    }
}