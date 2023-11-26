<?php

namespace App\Extensions\AdSystem\Bing\ExternalWork;

use Airbrake\Notifier;
use App\Extensions\AdSystem\Bing\ExternalWork\Auth\BingServiceManager;
use App\Extensions\Common\ExternalWork\SchedulesInterface;
use Microsoft\BingAds\V13\CampaignManagement\{BidMultiplier, GetCampaignCriterionsByIdsRequest, AddCampaignCriterionsRequest,
    BiddableCampaignCriterion, CampaignCriterionType, DayTimeCriterion, DeleteCampaignCriterionsRequest};
use Psr\Container\ContainerInterface;
use SoapVar;
use SoapFault;
use Exception;

/**
 * Class BingSchedules
 *
 * @package KC\DataBundle\Extensions\AdSystem\Bing\ExternalWork
 */
class BingSchedules implements SchedulesInterface
{
    /**
     * @var BingServiceManager
     */
    private BingServiceManager $serviceManager;

    /**
     * @var ContainerInterface
     */
    private ContainerInterface $container;

    /**
     * BingSchedules constructor.
     * @param BingServiceManager $serviceManager
     * @param ContainerInterface $container
     */
    public function __construct(BingServiceManager $serviceManager, ContainerInterface $container)
    {
        $this->serviceManager   = $serviceManager;
        $this->container        = $container;
    }

    /**
     * @return BingServiceManager
     */
    protected function getBingServicesManager(): BingServiceManager
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
     * @throws Exception
     */
    public function createSchedules(int $clientCustomerId , int $systemCampaignId, array $schedulesData): bool
    {
        $campaignService = $this->getBingServicesManager()->getCampaignManagementService($clientCustomerId);

        $campaignCriterions = [];
        foreach ($schedulesData as $scheduleData) {
            $dayTimeBiddableCampaignCriterion = new BiddableCampaignCriterion();
            $dayTimeBiddableCampaignCriterion->CampaignId = $systemCampaignId;

            $criterionBid = new BidMultiplier();
            $criterionBid->Multiplier = 0;
            $dayTimeBiddableCampaignCriterion->CriterionBid = new SoapVar(
                $criterionBid, SOAP_ENC_OBJECT,
                'BidMultiplier',
                $campaignService->GetNamespace());

            $schedule               = new DayTimeCriterion();
            $schedule->Day          = $scheduleData['dayOfWeek'];
            $schedule->FromHour     = $scheduleData['startTimeHour'];
            $schedule->FromMinute   = $scheduleData['startTimeMinute'];
            $schedule->ToHour       = $scheduleData['endTimeHour'];
            $schedule->ToMinute     = $scheduleData['endTimeMinute'];

            $encodedDayTimeCriterion = new SoapVar(
                $schedule,
                SOAP_ENC_OBJECT,
                'DayTimeCriterion',
                $campaignService->GetNamespace());

            $dayTimeBiddableCampaignCriterion->Criterion = $encodedDayTimeCriterion;

            $encodedCriterion = new SoapVar(
                $dayTimeBiddableCampaignCriterion,
                SOAP_ENC_OBJECT,
                'BiddableCampaignCriterion',
                $campaignService->GetNamespace());

            $campaignCriterions[] = $encodedCriterion;
        }

        if (!empty($campaignCriterions)) {
            $request = new AddCampaignCriterionsRequest();

            $request->CampaignCriterions = $campaignCriterions;
            $request->CriterionType = CampaignCriterionType::Targets;

            try {
                $campaignService->GetService()->AddCampaignCriterions($request);

                return true;
            } catch (SoapFault $e) {
                $this->getAirbrakeNotifier()->notify(new \Exception(
                    '[Bing] SoapFault was thrown with message - ' . $e->getMessage()
                    . ', when the process was running "createSchedules" for system campaign ID: '
                    . $systemCampaignId . PHP_EOL
                ));

                return false;
            }
        }

        return true;
    }

    /**
     * @param int $systemCampaignId
     * @param int $clientCustomerId
     * @return array
     */
    public function getUploadedSchedules(int $systemCampaignId, int $clientCustomerId): array
    {
        $campaignService = $this->getBingServicesManager()->getCampaignManagementService($clientCustomerId);

        $request = new GetCampaignCriterionsByIdsRequest();

        $request->CampaignId = $systemCampaignId;
        $request->CriterionType = CampaignCriterionType::DayTime;

        $campaignCriterions = $campaignService->GetService()->GetCampaignCriterionsByIds($request);

        $uploadedTimeRanges = [];
        if (isset($campaignCriterions->CampaignCriterions->CampaignCriterion)) {
            $schedules = $campaignCriterions->CampaignCriterions->CampaignCriterion;

            foreach ($schedules as $schedule) {
                $uploadedTimeRanges[$schedule->Id] = $schedule->Criterion->Day . intval($schedule->Criterion->FromHour) .
                    $schedule->Criterion->FromMinute . intval($schedule->Criterion->ToHour) . $schedule->Criterion->ToMinute;
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
     * @throws Exception
     */
    public function deleteSchedules(int $clientCustomerId, int $systemCampaignId, array $scheduleIds): bool
    {
        $campaignService = $this->getBingServicesManager()->getCampaignManagementService($clientCustomerId);

        if (!empty($scheduleIds)) {
            $request = new DeleteCampaignCriterionsRequest();

            $request->CampaignCriterionIds = $scheduleIds;
            $request->CampaignId = $systemCampaignId;
            $request->CriterionType = CampaignCriterionType::Targets;

            try {
                $campaignService->GetService()->DeleteCampaignCriterions($request);

                return true;
            } catch (SoapFault $e) {
                $this->getAirbrakeNotifier()->notify(new \Exception(
                    '[Bing] SoapFault was thrown with message - ' . $e->getMessage()
                    . ', when the process was running "deleteSchedules" for system campaign ID: '
                    . $systemCampaignId. PHP_EOL
                ));

                return false;
            }
        }

        return true;
    }
}