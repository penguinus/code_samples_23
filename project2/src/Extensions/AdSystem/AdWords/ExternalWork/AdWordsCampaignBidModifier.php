<?php

namespace App\Extensions\AdSystem\AdWords\ExternalWork;

use Airbrake\Notifier;
use App\Extensions\AdSystem\AdWords\ExternalWork\Auth\AdWordsServiceManager;
use App\Extensions\Common\ExternalWork\CampaignBidModifierInterface;
use Google\Ads\GoogleAds\Util\V13\ResourceNames;
use Google\Ads\GoogleAds\V13\Common\DeviceInfo;
use Google\Ads\GoogleAds\V13\Enums\DeviceEnum\Device;
use Google\Ads\GoogleAds\V13\Resources\CampaignCriterion;
use Google\Ads\GoogleAds\V13\Services\CampaignCriterionOperation;
use Google\ApiCore\ApiException;
use Psr\Container\ContainerInterface;
use Psr\Log\LoggerInterface;

/**
 * Class AdWordsCampaignBidModifier
 *
 * @package App\Extensions\AdSystem\AdWords\ExternalWork
 */
class AdWordsCampaignBidModifier implements CampaignBidModifierInterface
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
     * @var ContainerInterface
     */
    protected ContainerInterface $container;

    /**
     * AdWordsBudget constructor.
     * @param ContainerInterface    $container
     * @param AdWordsServiceManager $serviceManager
     * @param LoggerInterface       $adwordsLogger
     */
    public function __construct(
        ContainerInterface      $container,
        AdWordsServiceManager   $serviceManager,
        LoggerInterface         $adwordsLogger
    ) {
        $this->serviceManager   = $serviceManager;
        $this->logger           = $adwordsLogger;
    }

    /**
     * @return AdWordsServiceManager
     */
    protected function getGoogleServiceManager(): AdWordsServiceManager
    {
        return $this->serviceManager;
    }

    /**
     * @return LoggerInterface
     */
    protected function getLogger(): LoggerInterface
    {
        return $this->logger;
    }

    /**
     * @return Notifier
     */
    protected function getAirbrakeNotifier(): Notifier
    {
        return $this->container->get('ami_airbrake.notifier');
    }

    /**
     * @param int|string    $clientCustomerId
     * @param int|string    $systemCampaignId
     * @param string        $typeDevice
     * @param float         $deviceBidModifier
     *
     * @return CampaignCriterionOperation
     */
    public function makeDeviceBidModifierOperation(
                $clientCustomerId,
                $systemCampaignId,
        string  $typeDevice,
        float   $deviceBidModifier
    ): CampaignCriterionOperation
    {
        // Create mobile platform. The ID can be found in the documentation.

        $deviceInfo = new DeviceInfo();
        if (strcasecmp($typeDevice,'desktopBidModifier') == 0) {
            $deviceInfo->setType(Device::DESKTOP);
        }
        if (strcasecmp($typeDevice,'mobileBidModifier') == 0) {
            $deviceInfo->setType(Device::MOBILE);
        }
        if (strcasecmp($typeDevice,'tabletBidModifier') == 0) {
            $deviceInfo->setType(Device::TABLET);
        }

        // Create criterion with modified bid.
        $campaignCriterion = new CampaignCriterion();
        $campaignCriterion->setCampaign(ResourceNames::forCampaign($clientCustomerId, $systemCampaignId));
        $campaignCriterion->setDevice($deviceInfo);
        $campaignCriterion->setBidModifier($deviceBidModifier);

        // Create campaign criterion.
        $campaignCriterionOperation = new CampaignCriterionOperation();
        $campaignCriterionOperation->setCreate($campaignCriterion);

        return $campaignCriterionOperation;
    }

    /**
     * @param array     $operations
     * @param int|null  $clientCustomerId
     *
     * @return bool
     */
    public function uploadCriterionOperations(array $operations, ?int $clientCustomerId = null): bool
    {

        try {
            // Issues a mutate request to add the campaign criterion.
            $campaignCriterionServiceClient = $this->getGoogleServiceManager()->getCampaignCriterionServiceClient();
            $campaignCriterionServiceClient->mutateCampaignCriteria($clientCustomerId, $operations);

            return true;

        } catch (ApiException $apiException) {
            foreach ($apiException->getMetadata() as $metadatum) {
                foreach ($metadatum['errors'] as $error) {
                    if (!strpos(AdWordsErrorDetail::errorDetail($error['message']), "internal error")) {
                        $this->getAirbrakeNotifier()->notify(new \Exception(
                            '[Google] ApiException was thrown with message - ' . $error['message']
                            . '. When the process was running "uploadCriterionOperations()" ' . PHP_EOL
                            . '. Customer Id: ' . $clientCustomerId . PHP_EOL
                        ));
                    }
                }
            }

            return false;
        }
    }

    /**
     * @param string $message
     */
    private function writeToLog(string $message)
    {
        $this->getLogger()->error($message);
    }
}