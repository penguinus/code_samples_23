<?php

namespace App\Extensions\AdSystem\Bing\Traits;

use App\Document\ExtensionsQueue;
use App\Entity\BrandExtension;
use App\Entity\BrandTemplate;
use App\Entity\KcCampaign;
use Microsoft\BingAds\V13\CampaignManagement\CallAdExtension;
use Microsoft\BingAds\V13\CampaignManagement\CalloutAdExtension;
use Microsoft\BingAds\V13\CampaignManagement\Date;
use Microsoft\BingAds\V13\CampaignManagement\PromotionAdExtension;
use Microsoft\BingAds\V13\CampaignManagement\PromotionDiscountModifier;
use Microsoft\BingAds\V13\CampaignManagement\SitelinkAdExtension;
use Microsoft\BingAds\V13\CampaignManagement\StructuredSnippetAdExtension;
use SoapVar;

/**
 * Trait BingExtensionTrait
 * @package App\Extensions\AdSystem\Bing\Traits
 */
trait BingExtensionTrait
{
    /**
     * @var array
     */
    private $promotionTypeFieldMap = [
        BrandExtension::MONETARY_DISCOUNT => 'MoneyAmountOff',
        BrandExtension::PERCENT_DISCOUNT => 'PercentOff',
        BrandExtension::UP_TO_MONETARY_DISCOUNT => 'MoneyAmountOff',
        BrandExtension::UP_TO_PERCENT_DISCOUNT => 'PercentOff',
    ];

    /**
     * @param $promotionType
     * @return string
     */
    private function getFieldByPromotionType($promotionType)
    {
        return key_exists($promotionType, $this->promotionTypeFieldMap)
            ? $this->promotionTypeFieldMap[$promotionType] : null;
    }

    /**
     * @param ExtensionsQueue $extension
     * @return int|mixed
     */
    private function getValueByPromotionType(ExtensionsQueue $extension)
    {
        $value = $extension->getPromotionTypeValue();

        if (in_array($extension->getPromotionType(),
            [BrandExtension::PERCENT_DISCOUNT, BrandExtension::UP_TO_PERCENT_DISCOUNT])
        ) {
            if ($value > 100) {
                $value = 100;
            }
        }

        return $value;
    }

    /**
     * @var array
     */
    private $promotionDetailFieldMap = [
        BrandExtension::PROMOTION_ON_ORDERS_OVER_DETAIL => 'OrdersOverAmount',
        BrandExtension::PROMOTION_PROMO_CODE_DETAIL => 'PromotionCode',
    ];

    /**
     * @param int $promotionDetail
     * @return string|null
     */
    private function getFieldByPromotionDetail(int $promotionDetail)
    {
        return key_exists($promotionDetail, $this->promotionDetailFieldMap) ?
            $this->promotionDetailFieldMap[$promotionDetail] : null;
    }

    /**
     * @param ExtensionsQueue $extInQueue
     * @return SoapVar
     */
    public function getPromotionAdExtension(ExtensionsQueue $extInQueue)
    {
        $promAdExt = new PromotionAdExtension();

        $promAdExt->PromotionItem = $extInQueue->getName();
        $promAdExt->Language = 'English';

        $value = $this->getValueByPromotionType($extInQueue);

        if ($filedName = $this->getFieldByPromotionType($extInQueue->getPromotionType())) {
            $promAdExt->{$filedName} = $value;
        }

        if (in_array($extInQueue->getPromotionType(),
            [BrandExtension::UP_TO_MONETARY_DISCOUNT, BrandExtension::UP_TO_PERCENT_DISCOUNT])
        ) {
            $promAdExt->DiscountModifier = PromotionDiscountModifier::UpTo;
        }

        if ($extInQueue->getPromotionDateBegin()) {
            $dateArray = explode('/', $extInQueue->getPromotionDateBegin());
            if (is_array($dateArray) && count($dateArray) == 3) {
                $date = new Date();
                $date->Day = $dateArray[1];
                $date->Month = $dateArray[0];
                $date->Year = $dateArray[2];

                $promAdExt->PromotionEndDate = $date;
                $promAdExt->PromotionStartDate = $date;
            }
        }

        if ($extInQueue->getPromotionDateFinish()) {
            $dateArray = explode('/', $extInQueue->getPromotionDateFinish());
            if (is_array($dateArray) && count($dateArray) == 3) {
                $date = new Date();
                $date->Day = $dateArray[1];
                $date->Month = $dateArray[0];
                $date->Year = $dateArray[2];

                $promAdExt->PromotionEndDate = $date;
            }
        }

        if ($extInQueue->getOccasion()) {
            $promAdExt->PromotionOccasion = str_replace(
                '_', '', ucwords(strtolower($extInQueue->getOccasion()), '_'));
        }

        if ($extInQueue->getPromotionDetail()) {
            if ($fieldDetail = $this->getFieldByPromotionDetail($extInQueue->getPromotionDetail())) {
                $promAdExt->{$fieldDetail} = $extInQueue->getPromotionDetailValue();
            }
        }

        if ($extInQueue->getTrakingTemplate()) {
            $promAdExt->TrackingUrlTemplate = $extInQueue->getTrakingTemplate();
        }

        if(!is_null($promAdExt->MoneyAmountOff) || !is_null($promAdExt->OrdersOverAmount)) {
            # This field is only applicable if you set MoneyAmountOff or OrdersOverAmount.
            $promAdExt->CurrencyCode = 'USD';
        }

        $em = $this->getEntityManager();

        /** @var KcCampaign $kcCampaignInMySql */
        $kcCampaignInMySql = $em->getRepository('App:KcCampaign')
            ->findOneBy(['backendId' => $extInQueue->getKcCampaignBackendId()], []);
        /** @var BrandTemplate $brandTemplate */
        $brandTemplate = $em->getRepository('App:BrandTemplate')
            ->findOneBy(['id' => $kcCampaignInMySql->getBrandTemplate()], []);

        $destUrl = str_replace('?ctid={1}&chid={2}&cyid={3}', '', $extInQueue->getDestinationUrl() .
            '?ctid=' . $extInQueue->getKcCampaignBackendId() .
            '&chid=' . $brandTemplate->getChannelAdwordsId());

        $promAdExt->FinalUrls = [$destUrl];

        return new SoapVar(
            $promAdExt,
            SOAP_ENC_OBJECT,
            'PromotionAdExtension',
            $this->_managedCustomerService->GetNamespace());
    }

    /**
     * @param $extensionInMongo
     * @return SoapVar
     */
    public function getStructuredSnippetAdExtension($extensionInMongo)
    {
        $callouts = explode(",", $extensionInMongo->getCallout());

        foreach ($callouts as $i => $callout) {
            if (!$callout) {
                unset($callouts[$i]);
            }
        }

        $extension = new StructuredSnippetAdExtension();

        $extension->Header = $extensionInMongo->getPlatformTargeting();
        $extension->Values = $callouts;

        return new SoapVar(
            $extension,
            SOAP_ENC_OBJECT,
            'StructuredSnippetAdExtension',
            $this->_managedCustomerService->GetNamespace());
    }

    /**
     * @param $extensionInMongo
     * @return SoapVar
     */
    public function getCalloutAdExtension($extensionInMongo)
    {
        $extension = new CalloutAdExtension();
        $extension->Text = $extensionInMongo->getCallout();

        return new SoapVar(
            $extension,
            SOAP_ENC_OBJECT,
            'CalloutAdExtension',
            $this->_managedCustomerService->GetNamespace());
    }

    /**
     * @param $extensionInMongo
     * @return SoapVar
     */
    public function getCallAdExtension($extensionInMongo)
    {
        $extension = new CallAdExtension();
        $extension->CountryCode = "US";
        $extension->PhoneNumber = $extensionInMongo->getPhone();

        return new SoapVar(
            $extension,
            SOAP_ENC_OBJECT,
            'CallAdExtension',
            $this->_managedCustomerService->GetNamespace());
    }

    /**
     * @param $extension
     * @return SoapVar
     */
    public function getSitelinkAdExtension($extension)
    {
        $attributeValue = new SitelinkAdExtension();
        $attributeValue->Description1 = $extension->getDesc1();
        $attributeValue->Description2 = $extension->getDesc2();
        $attributeValue->DisplayText = $extension->getName();

        if ($extension->getTrakingTemplate()) {
            $attributeValue->TrackingUrlTemplate = $extension->getTrakingTemplate();
        }

        $em = $this->getEntityManager();

        $brandTemplate = $em->getRepository('App:KcCampaign')->findOneBy(['backendId' => $extension->getKcCampaignBackendId()], []);

        if ($brandTemplate) {
            $brandTemplate = $em->getRepository('App:BrandTemplate')->findOneBy(['id' => $brandTemplate->getBrandTemplate()->getId()], []);
        }

        $destUrl = str_replace('?ctid={1}&chid={2}&cyid={3}', '', $extension->getDestinationUrl() .
            '?ctid=' . $extension->getKcCampaignBackendId() .
            '&chid=' . $brandTemplate->getChannelBingId());

//        $attributeValue->DestinationUrl = $destUrl;
        $attributeValue->FinalUrls = [$destUrl];

        return new SoapVar(
            $attributeValue, SOAP_ENC_OBJECT,
            'SitelinkAdExtension',
            $this->_managedCustomerService->GetNamespace());
    }
}