<?php

namespace App\Extensions\AdSystem\AdWords\ExternalWork;

use Airbrake\Notifier;
use App\Extensions\AdSystem\AdWords\ExternalWork\Auth\AdWordsServiceManager;
use App\Extensions\Common\ExternalWork\SchedulesInterface;
use Google\Ads\GoogleAds\Util\V13\ResourceNames;
use Google\Ads\GoogleAds\V13\Common\AdScheduleInfo;
use Google\Ads\GoogleAds\V13\Resources\CampaignCriterion;
use Google\Ads\GoogleAds\V13\Services\{CampaignCriterionOperation, CampaignCriterionServiceClient, GoogleAdsRow};
use Google\ApiCore\ApiException;
use Google\ApiCore\ValidationException;
use Psr\Container\ContainerInterface;

/**
 *
 */
class AdWordsSchedules implements SchedulesInterface
{
    /**
     * @var AdWordsServiceManager
     */
    private AdWordsServiceManager $serviceManager;

    /**
     * @var ContainerInterface
     */
    private ContainerInterface $container;

    /**
     * BingSchedules constructor.
     * @param AdWordsServiceManager $serviceManager
     * @param ContainerInterface    $container
     */
    public function __construct(AdWordsServiceManager $serviceManager, ContainerInterface $container)
    {
        $this->serviceManager   = $serviceManager;
        $this->container        = $container;
    }

    /**
     * @return AdWordsServiceManager
     */
    protected function getGoogleServiceManager(): AdWordsServiceManager
    {
        return $this->serviceManager;
    }

    /**
     * @return Notifier
     */
    protected function getAirbrakeNotifier(): Notifier
    {
        return $this->container->get('ami_airbrake.notifier');
    }

    /**
     * @param int   $clientCustomerId
     * @param int   $systemCampaignId
     * @param array $schedulesData
     *
     * @return bool
     * @throws \Exception
     */
    public function createSchedules(int $clientCustomerId, int $systemCampaignId, array $schedulesData): bool
    {

        $campaignCriterionOperations = [];
        foreach ($schedulesData as $scheduleData) {
            $schedule = new AdScheduleInfo();
            $schedule->setDayOfWeek($scheduleData['dayOfWeek']);
            $schedule->setStartHour($scheduleData['startTimeHour']);
            $schedule->setStartMinute($scheduleData['startTimeMinute']);
            $schedule->setEndHour($scheduleData['endTimeHour']);
            $schedule->setEndMinute($scheduleData['endTimeMinute']);

            $campaignCriterion = new CampaignCriterion();
            $campaignCriterion->setCampaign(ResourceNames::forCampaign($clientCustomerId, $systemCampaignId));
            $campaignCriterion->setAdSchedule($schedule);

            // Run at normal bid rates
            $campaignCriterion->setBidModifier('1.0');

            $campaignCriterionOperation = new CampaignCriterionOperation();
            $campaignCriterionOperation->setCreate($campaignCriterion);

            $campaignCriterionOperations[] = $campaignCriterionOperation;
        }

        if (!empty($campaignCriterionOperations)) {
            try {
                // Issues a mutate request to add the campaign criterion.
                $campaignCriterionServiceClient = $this->getGoogleServiceManager()->getCampaignCriterionServiceClient();
                $campaignCriterionServiceClient->mutateCampaignCriteria($clientCustomerId, $campaignCriterionOperations);

            } catch (ApiException $apiException) {
                foreach ($apiException->getMetadata() as $metadatum) {
                    foreach ($metadatum['errors'] as $error) {
                        if (!strpos(AdWordsErrorDetail::errorDetail($error['message']), "internal error")) {
                            $this->getAirbrakeNotifier()->notify(new \Exception(
                                '[Google] ApiException was thrown with message - ' . $error['message']
                                . ', when the process was running "createSchedules()" for system campaign ID: '
                                . $systemCampaignId . PHP_EOL
                            ));
                        }
                    }
                }

                return false;
            }
        }

        return true;
    }

    /**
     * @param int $systemCampaignId
     * @param int $clientCustomerId
     *
     * @return array
     * @throws ValidationException
     * @throws \Exception
     */
    public function getUploadedSchedules(int $systemCampaignId, int $clientCustomerId): array
    {
        $query = sprintf(/** @lang text */
            "SELECT campaign_criterion.resource_name, campaign_criterion.ad_schedule.day_of_week, 
            campaign_criterion.ad_schedule.start_hour, campaign_criterion.ad_schedule.start_minute, 
            campaign_criterion.ad_schedule.end_hour, campaign_criterion.ad_schedule.end_minute 
            FROM campaign_criterion 
            WHERE campaign.id = %s ",
            $systemCampaignId
        );

        $uploadedTimeRanges = [];
        try {
            $googleAdsServiceClient = $this->getGoogleServiceManager()->getGoogleAdsServiceClient();

            // Issues a search request by specifying page size.
            $campaignCriterionResponse = $googleAdsServiceClient->search(
                $clientCustomerId,
                $query,
                ['pageSize' => $this->getGoogleServiceManager()::PAGE_SIZE]
            );

            if (!empty($campaignCriterionResponse->iterateAllElements())) {
                // Iterates over all rows in all pages and prints the requested field values for
                // the campaign criterion in each row.
                /** @var GoogleAdsRow $googleAdsRow */
                foreach ($campaignCriterionResponse->iterateAllElements() as $googleAdsRow) {
                    if ($googleAdsRow->getCampaignCriterion()->hasAdSchedule()) {
                        $criterionId = CampaignCriterionServiceClient::parseName(
                            $googleAdsRow->getCampaignCriterion()->getResourceName())['criterion_id'];

                        $uploadedTimeRanges[$criterionId]
                            = $googleAdsRow->getCampaignCriterion()->getAdSchedule()->getDayOfWeek()
                            . intval($googleAdsRow->getCampaignCriterion()->getAdSchedule()->getStartHour())
                            . $googleAdsRow->getCampaignCriterion()->getAdSchedule()->getStartMinute()
                            . intval($googleAdsRow->getCampaignCriterion()->getAdSchedule()->getEndHour())
                            . $googleAdsRow->getCampaignCriterion()->getAdSchedule()->getEndMinute();
                    }
                }
            }
        } catch (ApiException $apiException) {
            foreach ($apiException->getMetadata() as $metadatum) {
                foreach ($metadatum['errors'] as $error) {
                    if (!strpos(AdWordsErrorDetail::errorDetail($error['message']), "internal error")) {
                        $this->getAirbrakeNotifier()->notify(new \Exception(
                            '[Google] ApiException was thrown with message - ' . $error['message']
                            . ', when the process was running "getUploadedSchedules()" for system campaign ID: '
                            . $systemCampaignId . PHP_EOL
                        ));
                    }
                }
            }
        }

        return $uploadedTimeRanges;
    }

    /**
     * @param int   $clientCustomerId
     * @param int   $systemCampaignId
     * @param int[] $scheduleIds
     *
     * @return bool
     * @throws \Exception
     */
    public function deleteSchedules(int $clientCustomerId, int $systemCampaignId, array $scheduleIds): bool
    {
        $campaignCriterionOperations = [];
        foreach ($scheduleIds as $scheduleId) {
            // Creates a campaign ad schedule criterion with the proper resource name and any other changes.
            $resourceName = ResourceNames::forCampaignCriterion(
                $clientCustomerId,
                $systemCampaignId,
                $scheduleId
            );

            // Constructs an operation that will remove ad schedule with the specified resource name.
            $campaignCriterionOperation = new CampaignCriterionOperation();
            $campaignCriterionOperation->setRemove($resourceName);
            $campaignCriterionOperations[] = $campaignCriterionOperation;
        }

        if (!empty($campaignCriterionOperations)) {
            try {
                // Issues a mutate request to remove ad schedule.
                $campaignCriterionServiceClient = $this->getGoogleServiceManager()->getCampaignCriterionServiceClient();
                $campaignCriterionServiceClient->mutateCampaignCriteria($clientCustomerId, $campaignCriterionOperations);

            } catch (ApiException $apiException) {
                foreach ($apiException->getMetadata() as $metadatum) {
                    foreach ($metadatum['errors'] as $error) {
                        if (!strpos(AdWordsErrorDetail::errorDetail($error['message']), "internal error")) {
                            $this->getAirbrakeNotifier()->notify(new \Exception(
                                '[Google] ApiException was thrown with message - ' . $error['message']
                                . ', when the process was running "deleteSchedules()" for system campaign ID: '
                                . $systemCampaignId . PHP_EOL
                            ));
                        }
                    }
                }

                return false;
            }
        }

        return true;
    }
}