<?php

namespace App\Extensions\AdSystem\Bing\ExternalWork;

use Airbrake\Notifier;
use App\Extensions\AdSystem\Bing\ExternalWork\Auth\BingServiceManager;
use App\Extensions\AdSystem\Bing\ExternalWork\Enum\BingBidModifier;
use App\Extensions\Common\ExternalWork\CampaignBidModifierInterface;
use Microsoft\BingAds\V13\AdInsight\DeviceCriterion;
use Microsoft\BingAds\V13\CampaignManagement\AddCampaignCriterionsRequest;
use Microsoft\BingAds\V13\CampaignManagement\BiddableCampaignCriterion;
use Microsoft\BingAds\V13\CampaignManagement\BidMultiplier;
use Microsoft\BingAds\V13\CampaignManagement\CampaignCriterionStatus;
use Microsoft\BingAds\V13\CampaignManagement\CampaignCriterionType;
use Microsoft\BingAds\V13\CampaignManagement\DeleteCampaignCriterionsRequest;
use Psr\Container\ContainerInterface;
use Psr\Log\LoggerInterface;
use SoapVar;
use SoapFault;

/**
 * Class BingCampaignBidModifier
 * @package App\Extensions\AdSystem\AdWords\ExternalWork
 */
class BingCampaignBidModifier implements CampaignBidModifierInterface
{
    /**
     * @var BingServiceManager
     */
    private $serviceManager;

    /**
     * @var LoggerInterface
     */
    private $logger;

    /**
     * @var ContainerInterface
     */
    private ContainerInterface $container;

    /**
     * BingCampaignBidModifier constructor.
     * @param ContainerInterface $container
     * @param BingServiceManager $bingServiceManager
     * @param LoggerInterface    $bingLogger
     */
    public function __construct(
        ContainerInterface  $container,
        BingServiceManager  $bingServiceManager,
        LoggerInterface     $bingLogger
    ) {
        $this->serviceManager = $bingServiceManager;
        $this->logger = $bingLogger;
    }

    /**
     * @return BingServiceManager
     */
    protected function getBingServiceManager(): BingServiceManager
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
     * @param string $message
     * @param int|null $code
     */
    private function writeToLog(string $message, ?int $code = null)
    {
        if(isset($code)) {
            $message = "Bing error code: $code. " . $message;
        }
        $this->getLogger()->error($message);
    }

    /**
     * @param int|string    $clientCustomerId
     * @param int|string    $systemCampaignId
     * @param string        $typeDevice
     * @param float         $deviceBidModifier
     *
     * @return BiddableCampaignCriterion
     */
    public function makeDeviceBidModifierOperation(
                $clientCustomerId,
                $systemCampaignId,
        string  $typeDevice,
        float   $deviceBidModifier
    ): BiddableCampaignCriterion
    {
        $systemCampaignManager = $this->getBingServiceManager()->getCampaignManagementService();

        # If -1 it means that we need to exclusion of the corresponding Device. The value should be -100 for this.
        $deviceBidModifier = $deviceBidModifier == (float)-1 ? 0 : $deviceBidModifier;

        $bitTablets = new BidMultiplier();
        $bitTablets->Multiplier = ($deviceBidModifier - 1) * 100;

        $DeviceCriterion = new DeviceCriterion();
        $DeviceCriterion->Type = 'DeviceCriterion';
        $DeviceCriterion->DeviceName = $typeDevice;

        $criterionTablets = new BiddableCampaignCriterion();
        $criterionTablets->CampaignId = $systemCampaignId;
        $criterionTablets->Criterion = new SoapVar($DeviceCriterion, SOAP_ENC_OBJECT, 'DeviceCriterion', $systemCampaignManager->GetNamespace());
        $criterionTablets->Status = CampaignCriterionStatus::Active;
        $criterionTablets->Type = CampaignCriterionType::Device;
        $criterionTablets->CriterionBid = new SoapVar($bitTablets, SOAP_ENC_OBJECT, 'BidMultiplier', $systemCampaignManager->GetNamespace());

        return $criterionTablets;
    }

    /**
     * @param string $string
     * @return string
     * @throws \Exception
     */
    public static function getQueueFieldByDeviceType(string $string): string
    {
        switch ($string) {
            case BingBidModifier::COMPUTERS:
                return 'desktopBidModifier';
            case BingBidModifier::SMARTPHONES:
                return 'mobileBidModifier';
            case BingBidModifier::TABLETS:
                return 'tabletBidModifier';
            default:
                throw new \Exception('Unknown Bind device type');
        }
    }

    /**
     * @param array $operations
     * @param int|null $clientCustomerId
     * @return mixed
     */
    public function uploadCriterionOperations(array $operations, ?int $clientCustomerId = null)
    {
        $systemCampaignManager = $this->getBingServiceManager()->getCampaignManagementService();

        $request = new AddCampaignCriterionsRequest();
        $request->CriterionType = CampaignCriterionType::Targets;
        $request->CampaignCriterions = $operations;

        try {
            $result = $systemCampaignManager->GetService()->AddCampaignCriterions($request); //ADD

            if (!empty($result->NestedPartialErrors->BatchErrorCollection)) {
                foreach ($result->NestedPartialErrors->BatchErrorCollection as $error) {
                    $this->getAirbrakeNotifier()->notify(new \Exception(
                        '[Bing] Extension was thrown with message "Cannot delete bid modifier - '
                        . '" ' . $error->Message . '. Customer ID: ' . $clientCustomerId. PHP_EOL
                    ));
                    $this->writeToLog($error->Message, $error->Code);
                }

                return false;
            } else {
                return $result->CampaignCriterionIds->long;
            }
        } catch (SoapFault $e) {
            $this->getAirbrakeNotifier()->notify(new \Exception(
                '[Bing] SoapFault was thrown with message "Cannot update bid modifier - '
                . '" ' . $e->getMessage() . '. Customer ID: ' . $clientCustomerId
                . '. Request: ' . $systemCampaignManager->GetService()->__getLastRequest()
                . '. Response: '. $systemCampaignManager->GetService()->__getLastResponse(). PHP_EOL
            ));

//            print "\nLast SOAP request/response:\n";
//            printf("Fault Code: %s\nFault String: %s\n", $e->faultcode, $e->faultstring);
//            print $systemCampaignManager->GetWsdl() . "\n";
//            print $systemCampaignManager->GetService()->__getLastRequest() . "\n";
//            print $systemCampaignManager->GetService()->__getLastResponse() . "\n";

            if (isset($e->detail->AdApiFaultDetail)) {
                $systemCampaignManager->GetService()->OutputAdApiFaultDetail($e->detail->AdApiFaultDetail);
            }

            return false;
        } catch (\Exception $e) {
            if (!$e->getPrevious()) {
                $this->getAirbrakeNotifier()->notify(new \Exception(
                    '[Bing] Extension was thrown with message "Cannot update bid modifier - '
                    . '" ' . $e->getMessage() . '. Customer ID: ' . $clientCustomerId. PHP_EOL
                ));

//                $this->writeToLog($e->getMessage());
                //print $e->getTraceAsString() . "\n\n";
            }

            return false;
        }
    }

    /**
     * @param $systemCampaignId
     * @param array $ids
     * @return bool
     */
    public function deleteCampaignCriterionsByCampaign(
        $systemCampaignId,
        array $ids = []
    ): bool {
        $systemCampaignManager = $this->getBingServiceManager()->getCampaignManagementService();

        $arrayDevicesBidModifier = [];
        if (!empty($ids)) {
            $arrayDevicesBidModifier = array_filter($ids, function ($el) {
                return (bool)($el);
            });
        }

        if (count($arrayDevicesBidModifier)) {
            $request = new DeleteCampaignCriterionsRequest();
            $request->CampaignCriterionIds = $arrayDevicesBidModifier;
            $request->CampaignId = $systemCampaignId;
            $request->CriterionType = CampaignCriterionType::Targets;

            try {
                $result = $systemCampaignManager->GetService()->DeleteCampaignCriterions($request);

                if(!empty($result->NestedPartialErrors->BatchErrorCollection)) {
                    foreach ($result->NestedPartialErrors->BatchErrorCollection as $error) {
                        $this->getAirbrakeNotifier()->notify(new \Exception(
                            '[Bing] Extension was thrown with message "Cannot delete bid modifier - '
                            . '" ' . $error->Message . '. System campaign ID: ' . $systemCampaignId. PHP_EOL
                        ));

                        $this->writeToLog($error->Message, $error->Code);
                    }

                    return false;
                }

                return true;
            } catch (SoapFault $e) {
                $this->getAirbrakeNotifier()->notify(new \Exception(
                    '[Bing] SoapFault was thrown with message "Cannot delete bid modifier - '
                    . '" ' . $e->getMessage() . '. System campaign ID: ' . $systemCampaignId
                    . '. Request: ' . $systemCampaignManager->GetService()->__getLastRequest()
                    . '. Response: '. $systemCampaignManager->GetService()->__getLastResponse(). PHP_EOL
                ));

//                print "\nLast SOAP request/response:\n";
//                printf("Fault Code: %s\nFault String: %s\n", $e->faultcode, $e->faultstring);
//                print $systemCampaignManager->GetWsdl() . "\n";
//                print $systemCampaignManager->GetService()->__getLastRequest() . "\n";
//                print $systemCampaignManager->GetService()->__getLastResponse() . "\n";

                if (isset($e->detail->AdApiFaultDetail)) {
                    $systemCampaignManager->GetService()->OutputAdApiFaultDetail($e->detail->AdApiFaultDetail);
                }

                return false;
            } catch (\Exception $e) {
                if (!$e->getPrevious()) {
                    $this->getAirbrakeNotifier()->notify(new \Exception(
                        '[Bing] Extension was thrown with message "Cannot update bid modifier - '
                        . '" ' . $e->getMessage() . '. System campaign ID: ' . $systemCampaignId. PHP_EOL
                    ));

//                    $this->writeToLog($e->getMessage());

                    return false;
                    //print $e->getTraceAsString() . "\n\n";
                }
            }
        }

        return true;
    }
}