<?php

namespace App\Extensions\AdSystem\AdWords\ExternalWork;

use Airbrake\Notifier;
use App\Document\CampaignProcess;
use App\Extensions\AdSystem\AdWords\ExternalWork\Auth\AdWordsServiceManager;
use App\Extensions\AdSystem\AdWords\ExternalWork\Error\AdWordsInitLocationError;
use App\Extensions\AdSystem\AdWords\InternalWork\AdWordsCampaignManager;
use App\Extensions\Common\AdSystemEnum;
use App\Extensions\Common\ExternalWork\Core\CampaignAbstract;
use Doctrine\ODM\MongoDB\MongoDBException;
use Google\Ads\GoogleAds\Util\FieldMasks;
use Google\Ads\GoogleAds\Util\V13\ResourceNames;
use Google\Ads\GoogleAds\V13\Common\{FrequencyCapEntry, FrequencyCapKey, LanguageInfo, LocationInfo, ManualCpc};
use Google\Ads\GoogleAds\V13\Enums\AdvertisingChannelTypeEnum\AdvertisingChannelType;
use Google\Ads\GoogleAds\V13\Enums\BiddingStrategyTypeEnum\BiddingStrategyType;
use Google\Ads\GoogleAds\V13\Enums\CampaignStatusEnum\CampaignStatus;
use Google\Ads\GoogleAds\V13\Enums\FrequencyCapEventTypeEnum\FrequencyCapEventType;
use Google\Ads\GoogleAds\V13\Enums\FrequencyCapLevelEnum\FrequencyCapLevel;
use Google\Ads\GoogleAds\V13\Enums\FrequencyCapTimeUnitEnum\FrequencyCapTimeUnit;
use Google\Ads\GoogleAds\V13\Enums\NegativeGeoTargetTypeEnum\NegativeGeoTargetType;
use Google\Ads\GoogleAds\V13\Enums\PositiveGeoTargetTypeEnum\PositiveGeoTargetType;
use Google\Ads\GoogleAds\V13\Resources\{Campaign, CampaignCriterion};
use Google\Ads\GoogleAds\V13\Errors\OperationAccessDeniedErrorEnum\OperationAccessDeniedError;
use Google\Ads\GoogleAds\V13\Resources\Campaign\{NetworkSettings, GeoTargetTypeSetting};
use Google\Ads\GoogleAds\V13\Services\{CampaignCriterionOperation, CampaignServiceClient, CampaignOperation, GoogleAdsRow};
use Google\ApiCore\{ApiException, ValidationException};
use Psr\Container\ContainerInterface;
use Psr\Log\LoggerInterface;


/**
 * Class AdWordsCampaign
 *
 * @package App\Extensions\AdSystem\AdWords\ExternalWork
 */
class AdWordsCampaign extends CampaignAbstract
{
    /**
     * @var AdWordsServiceManager
     */
    private AdWordsServiceManager $serviceManager;

    /**
     * @var LoggerInterface
     */
    private LoggerInterface $logger;


    /**
     * AdWordsCampaign constructor.
     * @param ContainerInterface    $container
     * @param AdWordsServiceManager $adWordsServiceManager
     * @param LoggerInterface       $logger
     */
    public function __construct(ContainerInterface      $container,
                                AdWordsServiceManager   $adWordsServiceManager,
                                LoggerInterface         $logger
    )
    {
        parent::__construct($container);

        $this->serviceManager   = $adWordsServiceManager;
        $this->logger           = $logger;
    }

    /**
     * @return string
     */
    protected function getAdSystem(): string
    {
        return AdSystemEnum::ADWORDS;
    }

    /**
     * @return AdWordsServiceManager
     */
    protected function getGoogleServiceManager(): AdWordsServiceManager
    {
        return $this->serviceManager;
    }

    /**
     * @return AdWordsCampaignManager
     */
    protected function getGoogleCampaignManager(): AdWordsCampaignManager
    {
        return $this->get('adwords.campaign_manager');
    }

    /**
     * @return LoggerInterface
     */
    public function getLogger(): LoggerInterface
    {
        return $this->logger;
    }

    /**
     * create new campaign in Google Ads
     * return google campaign id or array errors or false operation
     * detail: https://developers.google.com/google-ads/api/reference/rpc/v9/Campaign
     *
     * @param int               $clientCustomerId
     * @param string            $campaignName
     * @param CampaignProcess   $hotsCampaign
     *
     * @return int | false
     * @throws \Exception
     */
    public function addCampaign(int $clientCustomerId, string $campaignName, CampaignProcess $hotsCampaign): ?int
    {
        // CREATE CAMPAIGN
        $campaign = new Campaign();
        $campaign->setName($campaignName);
        $campaign->setAdvertisingChannelType(AdvertisingChannelType::SEARCH);
        $campaign->setCampaignBudget(ResourceNames::forCampaignBudget($clientCustomerId, $hotsCampaign->getSystemBudgetId()));

        // Set the bidding strategy to ManualCpc.
        $manualCpc = new ManualCpc();
        $manualCpc->setEnhancedCpcEnabled(false);
        $campaign->setManualCpc($manualCpc);

        // Set bidding strategy (required).
        $campaign->setBiddingStrategyType(BiddingStrategyType::MANUAL_CPC);

        // Configures the campaign network options.
        $networkSettings = new NetworkSettings();
        $networkSettings->setTargetGoogleSearch(true);
        $networkSettings->setTargetSearchNetwork(false);
        $networkSettings->setTargetContentNetwork(false);
        $campaign->setNetworkSettings($networkSettings);

        // Recommendation: Set the campaign to PAUSED when creating it to prevent
        // the ads from immediately serving. Set to ENABLED once you've added
        // targeting and the ads are ready to serve.
        $campaign->setStatus(CampaignStatus::PAUSED);
        $campaign->setStartDate(date('Ymd', strtotime('+1 day')));

        // A group of fields used as keys for a frequency cap.
        $frequencyCapKey = new FrequencyCapKey();
        // Cap is applied to all the ad group of this level.
        $frequencyCapKey->setLevel(FrequencyCapLevel::AD_GROUP);
        // The cap applies on ad impressions.
        $frequencyCapKey->setEventType(FrequencyCapEventType::IMPRESSION);
        // The cap would define limit per one day.
        $frequencyCapKey->setTimeUnit(FrequencyCapTimeUnit::DAY);

        // A rule specifying the maximum number of times an ad (or some set of ads) can
        // be shown to a user over a particular time period.
        $frequencyCapEntry = new FrequencyCapEntry();
        $frequencyCapEntry->setKey($frequencyCapKey);
        // Maximum number of events allowed during the time range by this cap.
        $frequencyCapEntry->setCap(5);
        // A list that limits how often each user will see this campaign's ads.
        $campaign->setFrequencyCaps([$frequencyCapEntry]);

        // Set collection of settings related to ads geo targeting.
        $geoTargetTypeSetting = new GeoTargetTypeSetting();
        // Set that an ad is triggered if the user is in or regularly in advertiser's targeted locations.
        $geoTargetTypeSetting->setPositiveGeoTargetType(PositiveGeoTargetType::PRESENCE);
        // Set that a user is excluded from seeing the ad if they are in,
        // or show interest in, advertiser's excluded locations.
        $geoTargetTypeSetting->setNegativeGeoTargetType(NegativeGeoTargetType::PRESENCE);
        // Apply geo targeting for campaign
        $campaign->setGeoTargetTypeSetting($geoTargetTypeSetting);

        // Creates a campaign operation.
        $campaignOperation = new CampaignOperation();
        $campaignOperation->setCreate($campaign);
        $campaignOperations[] = $campaignOperation;

        $addedCampaignId = null;
        try {
            // Issues a mutate request to add campaigns.
            $campaignServiceClient = $this->getGoogleServiceManager()->getCampaignServiceClient();
            $response = $campaignServiceClient->mutateCampaigns($clientCustomerId, $campaignOperations);

            /** @var Campaign $result */
            foreach ($response->getResults() as $result) {
                $addedCampaign = CampaignServiceClient::parseName($result->getResourceName());
                $addedCampaignId = (int)$addedCampaign['campaign_id'];

                $campaignDetails = [
                    'id' => $hotsCampaign->getCampaignId(),
                    'backendId' => $hotsCampaign->getBackendId(),
                    'name' => $campaignName,
                ];

                $this->initLocation($clientCustomerId, $addedCampaignId, $hotsCampaign->getCityId(), $campaignDetails);

                $this->initLanguage($clientCustomerId, $addedCampaignId, $campaignDetails);
            }

        } catch (ApiException $apiException) {
            foreach ($apiException->getMetadata() as $metadatum) {
                foreach ($metadatum['errors'] as $error) {
                    $this->getGoogleCampaignManager()->registerCampaignUploadingErrors(
                        $hotsCampaign , [$error['message']], $campaignName);

                    if (!strpos(AdWordsErrorDetail::errorDetail($error['message']), "internal error")) {
                        $this->getAirbrakeNotifier()->notify(new \Exception(
                            '[Google] ApiException was thrown with message: "'. $error['message']. '"'
                            . ' when the process was running "addCampaign()".'
                            . '. Customer ID: ' . $clientCustomerId
                            . '. CampaignName: '. $campaignName
                            . '. RequestId: ' . $metadatum['requestId'] . PHP_EOL
                        ));
                    }
                }
            }
        }

        return $addedCampaignId ?: false;
    }

    /**
     * @param int       $clientCustomerId
     * @param int       $systemCampaignId
     * @param string    $newCampaignName
     *
     * @return bool
     */
    public function updateCampaignName(int $systemCampaignId, string $newCampaignName, int $clientCustomerId): bool
    {
        // Creates a campaign object with the specified resource name and other changes.
        $campaign = new Campaign();
        $campaign->setResourceName(ResourceNames::forCampaign($clientCustomerId, $systemCampaignId));
        $campaign->setName($newCampaignName);

        // Constructs an operation that will update the campaign with the specified resource name,
        // using the FieldMasks utility to derive the update mask. This mask tells the Google Ads
        // API which attributes of the campaign you want to change.
        $campaignOperation = new CampaignOperation();
        $campaignOperation->setUpdate($campaign);
        $campaignOperation->setUpdateMask(FieldMasks::allSetFieldsOf($campaign));
        $operations[] = $campaignOperation;

        try {
            // Issues a mutate request to update the campaign.
            $campaignServiceClient = $this->getGoogleServiceManager()->getCampaignServiceClient();
            $campaignServiceClient->mutateCampaigns($clientCustomerId, $operations);

        } catch (ApiException $apiException) {
            foreach ($apiException->getMetadata() as $metadatum) {
                foreach ($metadatum['errors'] as $error) {
                    if (!strpos(AdWordsErrorDetail::errorDetail($error['message']), "internal error")) {
                        $this->getAirbrakeNotifier()->notify(new \Exception(
                            '[Google] ApiException was thrown with message "'. $error['message']. '"'
                            . ' when the process was running "updateCampaignName()".'
                            . ' New campaign name: '. $newCampaignName . ' for systemCampaignId: ' . $systemCampaignId . '.'
                            . ' Customer Id: ' . $clientCustomerId . PHP_EOL
                        ));
                    }
                }
            }

            return false;
        }

        return true;
    }

    /**
     * @param int $systemCampaignId
     * @param int $clientCustomerId
     *
     * @return bool
     */
    public function deleteCampaign(int $systemCampaignId, int $clientCustomerId): bool
    {
        // Creates the resource name of a campaign to remove.
        $campaignResourceName = ResourceNames::forCampaign($clientCustomerId, $systemCampaignId);

        // Creates a campaign operation.
        $campaignOperation = new CampaignOperation();
        $campaignOperation->setRemove($campaignResourceName);

        try {
            // Issues a mutate request to remove the campaign.
            $campaignServiceClient = $this->getGoogleServiceManager()->getCampaignServiceClient();
            $campaignServiceClient->mutateCampaigns($clientCustomerId, [$campaignOperation]);

        } catch (ApiException $apiException) {
            foreach ($apiException->getMetadata() as $metadatum) {
                foreach ($metadatum['errors'] as $error) {
                    if (isset($error['errorCode']) && isset($error['errorCode']['operationAccessDeniedError'])) {
                        if (OperationAccessDeniedError::value($error['errorCode']['operationAccessDeniedError'])
                            == OperationAccessDeniedError::OPERATION_NOT_PERMITTED_FOR_REMOVED_RESOURCE) {
                            return true;
                        }
                    } else {
                        if (!strpos(AdWordsErrorDetail::errorDetail($error['message']), "internal error")) {
                            $this->getAirbrakeNotifier()->notify(new \Exception(
                                '[Google] ApiException was thrown with message - ' . $error['message']
                                . ' (' . $error['trigger']['stringValue'] . '). Customer Id: '
                                . $clientCustomerId . PHP_EOL
                            ));
                        }

                        return false;
                    }
                }
            }
        }

        return true;
    }

    /**
     * @param array $systemCampaignIds
     * @param int   $systemBudgetId
     * @param int   $clientCustomerId
     * @return bool
     */
    public function updateCampaignsBudget(array $systemCampaignIds, int $systemBudgetId, int $clientCustomerId): bool
    {
        if (empty($systemCampaignIds)) {
            return false;
        }

        $operations = [];
        foreach ($systemCampaignIds as $systemCampaignId) {
            // Creates a campaign object with the specified resource name and other changes.
            $campaign = new Campaign();
            $campaign->setResourceName(ResourceNames::forCampaign($clientCustomerId, $systemCampaignId));
            $campaign->setCampaignBudget(ResourceNames::forCampaignBudget($clientCustomerId, $systemBudgetId));

            // Constructs an operation that will update the campaign with the specified resource name,
            // using the FieldMasks utility to derive the update mask. This mask tells the Google Ads
            // API which attributes of the campaign you want to change.
            $campaignOperation = new CampaignOperation();
            $campaignOperation->setUpdate($campaign);
            $campaignOperation->setUpdateMask(FieldMasks::allSetFieldsOf($campaign));
            $operations[] = $campaignOperation;
        }

        try {
            // Issues a mutate request to update the campaign.
            $campaignServiceClient = $this->getGoogleServiceManager()->getCampaignServiceClient();
            $campaignServiceClient->mutateCampaigns($clientCustomerId, $operations);

        } catch (ApiException $apiException) {
            foreach ($apiException->getMetadata() as $metadatum) {
                foreach ($metadatum['errors'] as $error) {
                    if (!strpos(AdWordsErrorDetail::errorDetail($error['message']), "internal error")) {
                        $this->getAirbrakeNotifier()->notify(new \Exception(
                            '[Google] ApiException was thrown with message "'. $error['message']. '"'
                            . ' when the process was running "updateCampaignsBudget()".'
                            . ' Customer Id: "' . $clientCustomerId . PHP_EOL
                        ));
                    }
                }
            }

            return false;
        }

        return true;
    }

    /**
     * @param int   $clientCustomerId
     * @param array $systemCampaignIds
     * @param bool  $status
     *
     * @return bool
     */
    public function changeCampaignStatus(int $clientCustomerId, array $systemCampaignIds, bool $status): bool
    {
        if (empty($systemCampaignIds)) {
            return false;
        }

        $operations = [];
        foreach ($systemCampaignIds as $systemCampaignId) {
            // Creates a campaign object with the specified resource name and other changes.
            $campaign = new Campaign();
            $campaign->setResourceName(ResourceNames::forCampaign($clientCustomerId, $systemCampaignId));
            $campaign->setStatus($status ? CampaignStatus::ENABLED : CampaignStatus::PAUSED);

            // Constructs an operation that will update the campaign with the specified resource name,
            // using the FieldMasks utility to derive the update mask. This mask tells the Google Ads
            // API which attributes of the campaign you want to change.
            $campaignOperation = new CampaignOperation();
            $campaignOperation->setUpdate($campaign);
            $campaignOperation->setUpdateMask(FieldMasks::allSetFieldsOf($campaign));
            $operations[] = $campaignOperation;
        }

        try {
            // Issues a mutate request to update the campaign.
            $campaignServiceClient = $this->getGoogleServiceManager()->getCampaignServiceClient();
            $campaignServiceClient->mutateCampaigns($clientCustomerId, $operations);

        } catch (ApiException $apiException) {
            foreach ($apiException->getMetadata() as $metadatum) {
                foreach ($metadatum['errors'] as $error) {
                    if (!strpos(AdWordsErrorDetail::errorDetail($error['message']), "internal error")) {
                        $this->getAirbrakeNotifier()->notify(new \Exception(
                            '[Google] ApiException was thrown with message "'. $error['message']. '"'
                            . ' when the process was running "changeCampaignStatus()".'
                            . '" Customer ID: ' . $clientCustomerId . PHP_EOL
                        ));
                    }
                }
            }

            return false;
        }

        return true;
    }

    /**
     * @param int $clientCustomerId
     * @param string $campaignName
     *
     * @return int | null
     * @throws ValidationException
     */
    public function findCampaignInAdSystemByName(int $clientCustomerId, string $campaignName): ?int
    {
        // Creates a query that retrieves the location constants where the id includes a given campaign id.
        $query = sprintf( /** @lang text */
            "SELECT campaign.id, campaign.name, campaign.status 
            FROM campaign 
            WHERE campaign.status in ('PAUSED', 'ENABLED') 
            AND campaign.name = '%s'",
            $campaignName
        );

        $systemCampaignId = null;
        try {
            $googleAdsServiceClient = $this->getGoogleServiceManager()->getGoogleAdsServiceClient();
            $search = $googleAdsServiceClient->search($clientCustomerId, $query);

            // Iterates over all rows in all messages and prints the requested field values for
            // the campaign in each row.
            foreach ($search->iterateAllElements() as $googleAdsRow) {
                /** @var GoogleAdsRow $googleAdsRow */
                $systemCampaignId = $googleAdsRow->getCampaign()->getId();
            }

        } catch (ApiException $apiException) {
            foreach ($apiException->getMetadata() as $metadatum) {
                foreach ($metadatum['errors'] as $error) {
                    if (!strpos(AdWordsErrorDetail::errorDetail($error['message']), "internal error")) {
                        $this->getAirbrakeNotifier()->notify(new \Exception(
                            '[Google] ApiException was thrown with message "'. $error['message']. '"'
                            . ' when the process was running "findCampaignInAdSystemByName()".'
                            . ' search campaign name: '. $campaignName
                            . '" Customer ID: ' . $clientCustomerId . PHP_EOL
                        ));
                    }
                }
            }
        }

        return $systemCampaignId ?: false;
    }

    /**
     *  Creates a campaign criterion operation for the specified location ID.
     *
     * @param int   $clientCustomerId
     * @param int   $systemCampaignId
     * @param int   $locationId
     * @param array $campaignDetails
     *
     * @return bool
     * @throws MongoDBException
     */
    public function initLocation(int $clientCustomerId, int $systemCampaignId, int $locationId, array $campaignDetails): bool
    {
        // Creates a query that retrieves the location constants where the id includes a given campaign id.
        $query = sprintf( /** @lang text */
            "SELECT campaign_criterion.campaign, campaign_criterion.criterion_id, 
            campaign_criterion.location.geo_target_constant 
            FROM campaign_criterion 
            WHERE campaign.id = '%s' AND campaign_criterion.type = LOCATION",
            $systemCampaignId
        );

        try {
            // Issues a search request.
            $googleAdsServiceClient = $this->getGoogleServiceManager()->getGoogleAdsServiceClient();
            $response = $googleAdsServiceClient->search($clientCustomerId, $query);

            // Only one location can be set for a company
            if (!empty($response->iterateAllElements())) {
                // Iterates over all rows in all pages and prints the requested field values for
                // the campaign criterion in each row.
                foreach ($response->iterateAllElements() as $googleAdsRow) {
                    /** @var GoogleAdsRow $googleAdsRow */
                    $resourceName = $googleAdsRow->getCampaignCriterion()->getLocation()->getGeoTargetConstant();

                    // https://developers.google.com/google-ads/api/reference/rpc/v11/GeoTargetConstant?hl=en
                    // geoTargetConstants/{geo_target_constant_id}
                    $geoTargetConstant = $this->getGoogleServiceManager()->parseName(
                        'geoTargetConstants/{geo_target_constant_id}', $resourceName);

                    if ($geoTargetConstant['geo_target_constant_id'] === $locationId) {
                        $errorMessage = sprintf('The location already initialized for campaign (Location ID: %s).',
                            $geoTargetConstant['geo_target_constant_id']
                        );
                    } else {
                        $errorMessage = sprintf('Location ID: %s cant be initialized because thin campaign have '.
                            'Location ID: %s".',
                            $locationId,
                            $geoTargetConstant['geo_target_constant_id']
                        );
                    }

                    throw new \Exception($errorMessage);
                }
            }

        } catch (ApiException $apiException) {
            foreach ($apiException->getMetadata() as $metadatum) {
                foreach ($metadatum['errors'] as $error) {
                    $errorMessage = sprintf('Init campaign location error: %s',
                        $error['message']);

                    $this->registerCampaignErrors(
                        $error['message'], $errorMessage, $clientCustomerId, $systemCampaignId, $campaignDetails);

                    $this->getAirbrakeNotifier()->notify(new \Exception(
                        '[Google] ApiException was thrown with message "'. $error['message']. '"'
                        . ' when the process was running "initLocation()".'
                        . ' Search campaignId: '. $systemCampaignId
                        . '" Customer ID: ' . $clientCustomerId . PHP_EOL
                    ));
                }
            }

            return false;
        } catch (\Exception $exception) {
            $this->registerCampaignErrors(
                $exception->getMessage(), $exception->getMessage(), $clientCustomerId, $systemCampaignId, $campaignDetails);

            return false;
        }

        // Constructs a campaign criterion for the specified campaign ID using the specified
        // location ID.
        $campaignCriterion = new CampaignCriterion();
        // Creates a location using the specified location ID.
        $locationInfo = new LocationInfo();
        $locationInfo->setGeoTargetConstant(ResourceNames::forGeoTargetConstant($locationId));
        $campaignCriterion->setLocation($locationInfo);
        $campaignCriterion->setCampaign(ResourceNames::forCampaign($clientCustomerId, $systemCampaignId));

        // Creates a campaign criterion operation for the specified location ID.
        $campaignCriterionOperation = new CampaignCriterionOperation();
        $campaignCriterionOperation->setCreate($campaignCriterion);
        $operations[] = $campaignCriterionOperation;

        try {
            // Issues a mutate request to add the campaign criterion.
            $campaignCriterionServiceClient = $this->getGoogleServiceManager()->getCampaignCriterionServiceClient();
            $campaignCriterionServiceClient->mutateCampaignCriteria($clientCustomerId, $operations);

        } catch (ApiException $apiException) {
            foreach ($apiException->getMetadata() as $metadatum) {
                foreach ($metadatum['errors'] as $error) {
                    if ($error['errorCode']['criterionError'] === AdWordsInitLocationError::CANNOT_TARGET_CRITERION) {
                        $errorMessage = sprintf('%s Location ID: %s has been deprecated.',
                            $error['message'],
                            $error['trigger']['int64Value']
                        );

                        $this->registerInitLocationErrorsAndDeleteCampaign(
                            (int)$error['trigger']['int64Value'],
                            $errorMessage,
                            $apiException->getBasicMessage(),
                            $clientCustomerId,
                            $systemCampaignId,
                            $campaignDetails
                        );
                    } else {
                        $errorMessage = sprintf('%s (Location ID: %s, Request ID: %s).',
                            $error['message'],
                            $error['trigger']['int64Value'],
                            $metadatum['requestId']
                        );

                        $this->registerCampaignErrors(
                            $apiException->getBasicMessage(),
                            $errorMessage,
                            $clientCustomerId,
                            $systemCampaignId,
                            $campaignDetails
                        );
                    }

                    return false;
                }
            }
        }

        return true;
    }

    /**
     * @param       $customerId
     * @param       $systemCampaignId
     * @param array $campaignDetails
     *
     * @return bool
     * @throws MongoDBException | ValidationException
     */
    protected function initLanguage($customerId, $systemCampaignId, array $campaignDetails): bool
    {
        // Creates a query that retrieves the language constants where the id includes a given campaign id.
        $query = sprintf( /** @lang text */
            "SELECT campaign_criterion.language.language_constant 
            FROM campaign_criterion 
            WHERE campaign.id = '%s'",
            $systemCampaignId
        );

        try {
            // Issues a search search request.
            $googleAdsServiceClient = $this->getGoogleServiceManager()->getGoogleAdsServiceClient();
            $response = $googleAdsServiceClient->search($customerId, $query);

            // Iterates over all rows in all messages and prints the requested field values for
            // the language constant in each row.
            if (!empty($response->iterateAllElements())) {
                foreach ($response->iterateAllElements() as $googleAdsRow) {
                    /** @var GoogleAdsRow $googleAdsRow */
                    if ($googleAdsRow->getCampaignCriterion()->hasLanguage()) {
                        $languageConstantResourceName
                            = $googleAdsRow->getCampaignCriterion()->getLanguage()->getLanguageConstant();

                        // https://developers.google.com/google-ads/api/reference/rpc/v11/LanguageConstant?hl=en
                        // Language constant resource names have the form: languageConstants/{criterion_id}
                        $languageConstant = $this->getGoogleServiceManager()->parseName(
                            'languageConstants/{criterion_id}', $languageConstantResourceName);

                        // https://developers.google.com/google-ads/api/reference/data/codes-formats?hl=en
                        // Language for targeting. English = 1000
                        if ($languageConstant['criterion_id'] === 1000) {
                            $errorMessage = sprintf('The Language already initialized for campaign '.
                                '(Language constant: %s).',
                                $languageConstant['criterion_id']);
                        } else {
                            $errorMessage = sprintf('The Language ID: 1000 cant be initialized because '.
                                'thin campaign have Location ID: %s',
                                $languageConstant['criterion_id']);
                        }

                        $this->registerCampaignErrors(
                            $errorMessage,
                            $errorMessage,
                            $customerId,
                            $systemCampaignId,
                            $campaignDetails
                        );

                        return false;
                    }
                }
            }

        } catch (ApiException $apiException) {
            foreach ($apiException->getMetadata() as $metadatum) {
                foreach ($metadatum['errors'] as $error) {
                    $errorMessage = sprintf('Init campaign language error: %s',
                        $error['message']);

                    $this->registerCampaignErrors(
                        $error['message'], $errorMessage, $customerId, $systemCampaignId, $campaignDetails);

                    $this->getAirbrakeNotifier()->notify(new \Exception(
                        '[Google] ApiException was thrown with message "'. $error['message']. '"'
                        . ' when the process was running "initLanguage()".'
                        . ' Search campaignId: '. $systemCampaignId
                        . '" Customer ID: ' . $customerId . PHP_EOL
                    ));

                }
            }

            return false;
        }

        // A campaign criterion where add language for campaign.
        $campaignCriterion = new CampaignCriterion();
        $campaignCriterion->setCampaign(ResourceNames::forCampaign($customerId, $systemCampaignId));

        // A language criterion.
        $languageInfo = new LanguageInfo();

        // For a list of all language codes, see:
        // https://developers.google.com/google-ads/api/reference/data/codes-formats#expandable-7
        $languageInfo->setLanguageConstant(ResourceNames::forLanguageConstant(1000));

        // Set the language.
        $campaignCriterion->setLanguage($languageInfo);
        $campaignCriterionOperation = new CampaignCriterionOperation();
        $campaignCriterionOperation->setCreate($campaignCriterion);
        $operations[] = $campaignCriterionOperation;

        try {
            // Issues a mutate request to add the campaign criterion.
            $campaignCriterionServiceClient = $this->getGoogleServiceManager()->getCampaignCriterionServiceClient();
            $campaignCriterionServiceClient->mutateCampaignCriteria($customerId, $operations);

        } catch (ApiException $apiException) {
            foreach ($apiException->getMetadata() as $metadatum) {
                foreach ($metadatum['errors'] as $error) {
                    $errorMessage = sprintf('Init campaign language error: %s',
                        $error['message']);

                    $this->registerCampaignErrors(
                        $error['message'], $errorMessage, $customerId, $systemCampaignId, $campaignDetails);

                    return false;
                }
            }
        }

        return true;
    }
}