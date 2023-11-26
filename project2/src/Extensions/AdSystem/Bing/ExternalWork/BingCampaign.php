<?php

namespace App\Extensions\AdSystem\Bing\ExternalWork;

use App\Document\CampaignProcess;
use App\Extensions\AdSystem\Bing\ExternalWork\Auth\BingServiceManager;
use App\Extensions\AdSystem\Bing\ExternalWork\Error\BingInitLocationError;
use App\Extensions\AdSystem\Bing\InternalWork\BingCampaignManager;
use App\Extensions\Common\AdSystemEnum;
use App\Extensions\Common\ExternalWork\CampaignInterface;
use App\Extensions\Common\ExternalWork\Core\CampaignAbstract;
use Doctrine\ODM\MongoDB\MongoDBException;
use Exception;
use Microsoft\BingAds\V13\CampaignManagement\BidMultiplier;
use Microsoft\BingAds\V13\CampaignManagement\CampaignCriterionStatus;
use Microsoft\BingAds\V13\CampaignManagement\DeviceCriterion;
use Microsoft\BingAds\V13\CampaignManagement\EnhancedCpcBiddingScheme;
use Microsoft\BingAds\V13\CampaignManagement\GetCampaignCriterionsByIdsRequest;
use Microsoft\BingAds\V13\CampaignManagement\AddCampaignCriterionsRequest;
use Microsoft\BingAds\V13\CampaignManagement\BiddableCampaignCriterion;
use Microsoft\BingAds\V13\CampaignManagement\CampaignCriterionType;
use Microsoft\BingAds\V13\CampaignManagement\CampaignStatus;
use Microsoft\BingAds\V13\CampaignManagement\DeleteCampaignCriterionsRequest;
use Microsoft\BingAds\V13\CampaignManagement\DeleteCampaignsRequest;
use Microsoft\BingAds\V13\CampaignManagement\IntentOption;
use Microsoft\BingAds\V13\CampaignManagement\LocationCriterion;
use Microsoft\BingAds\V13\CampaignManagement\LocationIntentCriterion;
use Microsoft\BingAds\V13\CampaignManagement\UpdateCampaignsRequest;
use Microsoft\BingAds\V13\CampaignManagement\AddCampaignsRequest;
use Microsoft\BingAds\V13\CampaignManagement\Campaign;
use Microsoft\BingAds\V13\CampaignManagement\CampaignType;
use Microsoft\BingAds\V13\CampaignManagement\ManualCpcBiddingScheme;
use Psr\Container\ContainerExceptionInterface;
use Psr\Container\ContainerInterface;
use Psr\Container\NotFoundExceptionInterface;
use SoapFault;
use SoapVar;


/**
 * Class BingCampaign
 * @package App\Extensions\AdSystem\Bing\ExternalWork
 */
class BingCampaign extends CampaignAbstract
{
    /**
     * @var BingServiceManager
     */
    private BingServiceManager $serviceManager;


    /**
     * BingCampaign constructor.
     * @param ContainerInterface $container
     * @param BingServiceManager $bingServiceManager
     */
    public function __construct(ContainerInterface $container, BingServiceManager $bingServiceManager)
    {
        parent::__construct($container);

        $this->container        = $container;
        $this->serviceManager   = $bingServiceManager;
    }

    /**
     * @return string
     */
    protected function getAdSystem(): string
    {
        return AdSystemEnum::BING;
    }

    /**
     * @return BingServiceManager
     */
    protected function getBingServicesManager(): BingServiceManager
    {
        return $this->serviceManager;
    }

    /**
     * @return BingCampaignManager
     * @throws ContainerExceptionInterface|NotFoundExceptionInterface
     */
    protected function getBingCampaignManager(): BingCampaignManager
    {
        return $this->get('bing.campaign_manager');
    }

    /**
     * @param int $clientCustomerId
     * @param string $campaignName
     * @param CampaignProcess $hotsCampaign
     *
     * @return int|false
     */
    public function addCampaign(int $clientCustomerId, string $campaignName, CampaignProcess $hotsCampaign): ?int
    {
        // CREATE CAMPAIGN

        // Get the CampaignService, which loads the required classes.
        $campaignService = $this->getBingServicesManager()->getCampaignManagementService($clientCustomerId);

        $campaigns = [];
        $campaign = new Campaign();
        $campaign->CampaignType = $this->getCampaignType();
        // Languages must be set for Audience campaigns
        // You can set on adGroup level. Maybe soon you will able to set on campaign level.
//        $campaign->Languages = array("English");
        $campaign->Name = $campaignName;
//        $campaign->Description = $campaignName;
        $campaign->BudgetId = $hotsCampaign->getSystemBudgetId();

        $biddingScheme = new EnhancedCpcBiddingScheme();
        $campaign->BiddingScheme = new SoapVar(
            $biddingScheme,
            SOAP_ENC_OBJECT,
            'EnhancedCpcBiddingScheme',
            $campaignService->GetNamespace());

        $campaign->TimeZone = "EasternTimeUSCanada";

        $campaign->Status = CampaignStatus::Paused;

        // Set network targeting (optional).
        // You can set on adGroup level
        // For adwords you can set on campaign level. See code for adwords


        // Set start date
        // You can set on adGroup level
        // For adwords you can set on campaign level.
        /* $campaign->setStartDate(date('Ymd', strtotime('+1 day')));
         $campaign->setEndDate(null);*/

//        $campaign->setAdServingOptimizationStatus('ROTATE'); ?????????????????????????????????????????

        // Set frequency cap (optional).
        /*$frequencyCap = new FrequencyCap();
        $frequencyCap->setImpressions(5);
        $frequencyCap->setTimeUnit('DAY');
        $frequencyCap->setLevel('ADGROUP');
        $campaign->setFrequencyCap($frequencyCap);*/

        $campaigns[] = $campaign;

        $request = new AddCampaignsRequest();

        $request->AccountId = $clientCustomerId;
        $request->Campaigns = $campaigns;

        $systemCampaignId = null;
        try {
            $addCampaignsResponse = $campaignService->GetService()->AddCampaigns($request);

            if (!empty($addCampaignsResponse->CampaignIds->long)) {
                foreach ($addCampaignsResponse->CampaignIds->long as $systemCampaignId) {

                    $systemCampaignId = (int)$systemCampaignId;

                    $campaignDetails = [
                        'id' => $hotsCampaign->getCampaignId(),
                        'backendId' => $hotsCampaign->getBackendId(),
                        'name' => $campaignName,
                    ];
                    $this->initLocation(
                        $clientCustomerId, $systemCampaignId, $hotsCampaign->getCityId(), $campaignDetails);
                    //                $this->initLanguage($clientCustomerId, $googleCampaignId);

                }
            } elseif (!empty($addCampaignsResponse->PartialErrors->BatchError)) {
                // Register errors
                $errors = $addCampaignsResponse->PartialErrors->BatchError;
                $this->getBingCampaignManager()->registerCampaignUploadingErrors($hotsCampaign, $errors, $campaignName);

                return false;
            }  // return false operation
            else {
                return false;
            }
        } catch (\Exception $e) {
            $this->getAirbrakeNotifier()->notify(new \Exception(
                '[Bing] SoapFault was thrown with message: ' . $e->getMessage()
                . '. Customer ID: ' . $clientCustomerId
                . '. Request: ' . $campaignService->GetService()->__getLastRequest()
                . '. Response: '. $campaignService->GetService()->__getLastResponse(). PHP_EOL
            ));

            return false;
        }

        return $systemCampaignId ?: false;
    }

    /**
     * @return string
     */
    protected function getCampaignType()
    {
        return CampaignType::Search;
    }

    /**
     * @param int $clientCustomerId
     * @param int $systemCampaignId
     * @param int $locationId
     * @param array $campaignDetails
     *
     * @return bool
     * @throws MongoDBException
     */
    public function initLocation(int $clientCustomerId, int $systemCampaignId, int $locationId, array $campaignDetails): bool
    {
        $bsm = $this->getBingServicesManager();
        $campaignService = $bsm->getCampaignManagementService($clientCustomerId);
        // Create locations. The IDs can be found in the documentation or retrieved
        // with the LocationCriterionService.
        $request = new GetCampaignCriterionsByIdsRequest();

        $request->CampaignId = $systemCampaignId;
        $request->CriterionType = CampaignCriterionType::Location;

        try {
            $campaignCriterions = $campaignService->GetService()->GetCampaignCriterionsByIds($request);
        } catch (Exception $e) {
            print $e->getMessage() . PHP_EOL;
        }

        if (empty($campaignCriterions) || !isset($campaignCriterions->CampaignCriterions->CampaignCriterion)) {
            $campaignCriterions = [];

            $campaignService = $bsm->refreshLastServiceClient();

            $campaignCriterions[] = $this->makeCampaignLocationCriterion(
                $systemCampaignId, $locationId, $campaignService->GetNamespace());

            $campaignCriterions[] = $this->makeCampaignLocationIntentCriterion(
                $systemCampaignId, $campaignService->GetNamespace());

            $request = new AddCampaignCriterionsRequest();
            $request->CampaignCriterions = $campaignCriterions;
            $request->CriterionType = CampaignCriterionType::Targets;

            try {
                $result = $campaignService->GetService()->AddCampaignCriterions($request);
                $this->processInitLocationErrors($result, $locationId, $clientCustomerId, $systemCampaignId, $campaignDetails);

            } catch(Exception $e) {
                $message = "Something went wrong during location initialization (location id: {$locationId}).";
                $this->registerCampaignErrors(
                    $message, $message, $clientCustomerId, $systemCampaignId, $campaignDetails);

                return false;
            }
        } else {
            $message = "The location already initialized for campaign (location id: {$locationId}).";
            $this->registerCampaignErrors($message, $message, $clientCustomerId, $systemCampaignId, $campaignDetails);

            return false;
        }

        return true;
    }

    /**
     * @param $result
     * @param int $cityId
     * @param $clientCustomerId
     * @param $systemCampaignId
     * @param array $campaignDetails
     * @throws MongoDBException
     */
    private function processInitLocationErrors(
        $result,
        int $cityId,
        $clientCustomerId,
        $systemCampaignId,
        array $campaignDetails
    ) {
        $nestedPartialErrors = $result->NestedPartialErrors;

        if (count((array)$nestedPartialErrors) != 0 || isset($nestedPartialErrors->BatchErrorCollection)) {
            foreach ($nestedPartialErrors->BatchErrorCollection as $error) {
                # CRITERION_ALREADY_EXISTS - such location already init.
                if ($error->Code != BingInitLocationError::SUCH_CRITERION_ALREADY_EXISTS) {
                    if ($error->Code == BingInitLocationError::LOCATION_ID_IS_DEPRECATED) {
                        $errorDetails = "Location ID is invalid. LocationId" .
                            " {$cityId} has been deprecated.";

                        $this->registerInitLocationErrorsAndDeleteCampaign($cityId, $error->Message, $errorDetails,
                            $clientCustomerId, $systemCampaignId, $campaignDetails);
                    } else {
                        $errorDetails = $error->Message . ' ' . $error->Details . " (location id: {$cityId}).";

                        $this->registerCampaignErrors(
                            $error->Message, $errorDetails, $clientCustomerId, $systemCampaignId, $campaignDetails);
                    }
                }
            }
        }
    }

    /**
     * @param int $systemCampaignId
     * @param int $cityId
     * @param $serviceNamespace
     * @return SoapVar
     */
    public function makeCampaignLocationCriterion($systemCampaignId, $cityId, $serviceNamespace)
    {
        $locationBiddableCampaignCriterion = new BiddableCampaignCriterion();
        $locationBiddableCampaignCriterion->CampaignId = $systemCampaignId;

        $locationCriterion = new LocationCriterion();
        $locationCriterion->LocationId = $cityId;

        $encodedLocationCriterion = new SoapVar(
            $locationCriterion,
            SOAP_ENC_OBJECT,
            'LocationCriterion',
            $serviceNamespace
        );

        $locationBiddableCampaignCriterion->Criterion = $encodedLocationCriterion;

        return new SoapVar(
            $locationBiddableCampaignCriterion,
            SOAP_ENC_OBJECT,
            'BiddableCampaignCriterion',
            $serviceNamespace
        );
    }

    /**
     * @param int $systemCampaignId
     * @param int $serviceNamespace
     * @param string $intentOption
     * @return SoapVar
     */
    public function makeCampaignLocationIntentCriterion($systemCampaignId, $serviceNamespace, $intentOption = IntentOption::PeopleIn)
    {
        $locationIntentBiddableCampaignCriterion = new BiddableCampaignCriterion();
        $locationIntentBiddableCampaignCriterion->CampaignId = $systemCampaignId;

        $locationIntentCriterion = new LocationIntentCriterion();
        $locationIntentCriterion->IntentOption = $intentOption;

        $encodedLocationIntentCriterion = new SoapVar(
            $locationIntentCriterion,
            SOAP_ENC_OBJECT,
            'LocationIntentCriterion',
            $serviceNamespace
        );

        $locationIntentBiddableCampaignCriterion->Criterion = $encodedLocationIntentCriterion;

        return new SoapVar(
            $locationIntentBiddableCampaignCriterion,
            SOAP_ENC_OBJECT,
            'BiddableCampaignCriterion',
            $serviceNamespace);
    }

    /**
     * @param int $systemCampaignId
     * @param string $newCampaignName
     * @param int $clientCustomerId
     * @return bool
     */
    public function updateCampaignName(int $systemCampaignId, string $newCampaignName, int $clientCustomerId): bool
    {
        $campaignService = $this->getBingServicesManager()->getCampaignManagementService($clientCustomerId);

        $updateCampaign = new Campaign();
        $updateCampaign->Id = $systemCampaignId;
        $updateCampaign->Name = $newCampaignName;

        $request = new UpdateCampaignsRequest();
        $request->AccountId = $clientCustomerId;
        $request->Campaigns = [$updateCampaign];

        try {
            $updateCampaignsResponse = $campaignService->GetService()->UpdateCampaigns($request);

            if (!empty($updateCampaignsResponse->PartialErrors->BatchError)) {
                foreach ($updateCampaignsResponse->PartialErrors->BatchError as $error) {
                    $this->getAirbrakeNotifier()->notify(new \Exception(
                        '[Bing] BatchError was thrown with message "'. $error->Message. '"'
                        . ' when the process was running "updateCampaignName()".'
                        . ' New campaign name: '. $newCampaignName . ' for systemCampaignId: ' . $systemCampaignId . '.'
                        . ' Customer Id: ' . $clientCustomerId . PHP_EOL
                    ));
                }

                return false;
            }

        } catch (\Exception $e) {
            $this->getAirbrakeNotifier()->notify(new \Exception(
                '[Bing] SoapFault was thrown with message "Cannot Update Campaign name - '
                . $e->getMessage() . '". Customer ID: ' . $clientCustomerId
                . '. Request: ' . $campaignService->GetService()->__getLastRequest()
                . '. Response: '. $campaignService->GetService()->__getLastResponse(). PHP_EOL
            ));

            return false;
        }

        return true;
    }

    /**
     * @param int $systemCampaignId
     * @param int $clientCustomerId
     * @return bool
     */
    public function deleteCampaign(int $systemCampaignId, int $clientCustomerId): bool
    {
        $campaignService = $this->getBingServicesManager()->getCampaignManagementService($clientCustomerId);

        $request = new DeleteCampaignsRequest();
        $request->AccountId = $clientCustomerId;
        $request->CampaignIds = [$systemCampaignId];

        try {
            $deleteCampaignsResponse = $campaignService->GetService()->DeleteCampaigns($request);

            if (!empty($deleteCampaignsResponse->PartialErrors->BatchError)) {
                $errors = $deleteCampaignsResponse->PartialErrors->BatchError;

                foreach ($errors as $error) {
                    print $error->Message . PHP_EOL;
                }

                return false;
            }

        } catch (\Exception $e) {
            $this->getAirbrakeNotifier()->notify(new \Exception(
                '[Bing] SoapFault was thrown with message "Cannot delete systemCampaignId - '
                . $systemCampaignId . '". Error: ' . $e->getMessage() . '". Customer ID: ' . $clientCustomerId
                . '. Request: ' . $campaignService->GetService()->__getLastRequest()
                . '. Response: ' . $campaignService->GetService()->__getLastResponse(). PHP_EOL
            ));

            return false;
        }

        return true;
    }

    /**
     * @param array $systemCampaignIds
     * @param int   $systemBudgetId
     * @param int   $clientCustomerId
     *
     * @return bool
     */
    public function updateCampaignsBudget(array $systemCampaignIds, int $systemBudgetId, int $clientCustomerId): bool
    {
        $campaignService = $this->getBingServicesManager()->getCampaignManagementService($clientCustomerId);

        $campaigns = [];
        $counter = 0;
        foreach ($systemCampaignIds as $systemCampaignId) {
            $counter++;

            $updateCampaign = new Campaign();
            $updateCampaign->Id = $systemCampaignId;
            $updateCampaign->BudgetId = $systemBudgetId;

            $campaigns[] = $updateCampaign;

            if ($counter % 100 == 0) {
                $request = new UpdateCampaignsRequest();
                $request->AccountId = $clientCustomerId;
                $request->Campaigns = $campaigns;

                try {
                    $updateCampaignsResponse = $campaignService->GetService()->UpdateCampaigns($request);

                    if (!empty($updateCampaignsResponse->PartialErrors->BatchError)) {
                        $errors = $updateCampaignsResponse->PartialErrors->BatchError;

                        foreach ($errors as $error) {
                            print $error->Message.PHP_EOL;
                        }

                        return false;
                    }

                    $campaigns = [];

                } catch (\Exception $e) {
                    $this->getAirbrakeNotifier()->notify(new \Exception(
                        '[Bing] SoapFault was thrown with message "Cannot update campaigns budget - '
                        . $e->getMessage() . '". Customer ID: ' . $clientCustomerId
                        . '. Request: ' . $campaignService->GetService()->__getLastRequest()
                        . '. Response: '. $campaignService->GetService()->__getLastResponse(). PHP_EOL
                    ));

                    return false;
                }
            }
        }

        if (!empty($campaigns)) {
            $request = new UpdateCampaignsRequest();
            $request->AccountId = $clientCustomerId;
            $request->Campaigns = $campaigns;

            try {
                $updateCampaignsResponse = $campaignService->GetService()->UpdateCampaigns($request);

                if (!empty($updateCampaignsResponse->PartialErrors->BatchError)) {
                    $errors = $updateCampaignsResponse->PartialErrors->BatchError;

                    foreach ($errors as $error) {
                        print $error->Message.PHP_EOL;
                    }

                    return false;
                }

            } catch (\Exception $e) {
                $this->getAirbrakeNotifier()->notify(new \Exception(
                    '[Bing] SoapFault was thrown with message "Cannot update campaigns budget - '
                    . $e->getMessage() . '". Customer ID: ' . $clientCustomerId
                    . '. Request: ' . $campaignService->GetService()->__getLastRequest()
                    . '. Response: ' . $campaignService->GetService()->__getLastResponse(). PHP_EOL
                ));

                return false;
            }
        }

        return true;
    }

    /**
     * @param int $clientCustomerId
     * @param string $campaignName
     *
     * @return int|null
     */
    public function findCampaignInAdSystemByName(int $clientCustomerId, string $campaignName): ?int
    {
        /** Maybe when Bing will make a norm library and it will be able to get a campaign by name. */
        return null;
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
        // Get the CampaignService, which loads the required classes.
        $campaignService = $this->getBingServicesManager()->getCampaignManagementService($clientCustomerId);

        $campaigns = [];
        $counter = 0;
        $status = $status ? CampaignStatus::Active : CampaignStatus::Paused;
        foreach ($systemCampaignIds as $systemCampaignId) {
            $counter++;

            $campaign = new Campaign();
            // CampaignType must be set for Audience campaigns
            $campaign->Id = $systemCampaignId;
            $campaign->Status = $status;

            $campaigns [] = $campaign;

            if ($counter % 100 == 0) {
                $request = new UpdateCampaignsRequest();
                $request->AccountId = $clientCustomerId;
                $request->Campaigns = $campaigns;

                try {
                    $response = $campaignService->GetService()->UpdateCampaigns($request);

                    if (!empty($response->PartialErrors->BatchError)) {
                        $message = $response->PartialErrors->BatchError[0]->Message;
                        print $message.PHP_EOL;

                        return false;
                    }

                    $campaigns = [];

                } catch (\Exception $e) {
                    $this->getAirbrakeNotifier()->notify(new \Exception(
                    '[Bing] SoapFault was thrown with message "Cannot change campaigns status - '
                        . $e->getMessage() .'". Customer ID: ' . $clientCustomerId
                        . '. Request: ' . $campaignService->GetService()->__getLastRequest()
                        . '. Response: '. $campaignService->GetService()->__getLastResponse(). PHP_EOL
                    ));

                    return false;
                }
            }
        }

        if (!empty($campaigns)) {
            $request = new UpdateCampaignsRequest();
            $request->AccountId = $clientCustomerId;
            $request->Campaigns = $campaigns;

            try {
                $response = $campaignService->GetService()->UpdateCampaigns($request);

                if (!empty($response->PartialErrors->BatchError)) {
                    $message = str_replace(
                        'campaign ID',
                        'campaign ID "'. $campaigns[$response->PartialErrors->BatchError[0]->Index]->Id .'"',
                        $response->PartialErrors->BatchError[0]->Message);
                    print $message. ' Cannot changed status to "'. $status . '".'. PHP_EOL;

                    return false;
                }

            } catch (\Exception $e) {
                $this->getAirbrakeNotifier()->notify(new \Exception(
                    '[Bing] SoapFault was thrown with message "Cannot change campaigns status - '
                    . $e->getMessage() . '". Customer ID: ' . $clientCustomerId
                    . '. Request: ' . $campaignService->GetService()->__getLastRequest()
                    . '. Response: '. $campaignService->GetService()->__getLastResponse(). PHP_EOL
                ));

                return false;
            }
        }

        return true;
    }
}