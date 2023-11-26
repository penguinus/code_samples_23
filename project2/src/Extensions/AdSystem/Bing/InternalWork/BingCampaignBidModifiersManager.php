<?php

namespace App\Extensions\AdSystem\Bing\InternalWork;

use App\Document\Campaign;
use App\Document\CampaignProcess;
use App\Document\DevicesBidModifier;
use App\Document\KcCampaign;
use App\Extensions\AdSystem\Bing\ExternalWork\BingCampaignBidModifier;
use App\Extensions\AdSystem\Bing\ExternalWork\Auth\BingServiceManager;
use App\Extensions\AdSystem\Bing\ExternalWork\Enum\BingBidModifier;
use App\Extensions\Common\AdSystemEnum;
use App\Extensions\Common\InternalWork\Basic\CampaignBidModifiersManager;
use App\Providers\ProviderEntityName;
use Doctrine\ODM\MongoDB\DocumentManager;
use Doctrine\ORM\EntityManagerInterface;
use Microsoft\BingAds\V13\CampaignManagement\CampaignCriterionType;
use Microsoft\BingAds\V13\CampaignManagement\GetCampaignCriterionsByIdsRequest;
use MongoDB\BSON\ObjectId;
use Psr\Log\LoggerInterface;
use SoapVar;

/**
 * Class BingCampaignBidModifiersManager
 * @package App\Extensions\AdSystem\Bing\InternalWork
 */
class BingCampaignBidModifiersManager extends CampaignBidModifiersManager
{
    /**
     */
    const AMOUNT_ITEM_TO_PROCESS = 99;

    /**
     * @var BingCampaignBidModifier
     */
    private $campaignBidModifier;

    /**
     * @var BingServiceManager
     */
    private $serviceManager;

    /**
     * BingCampaignBidModifiersManager constructor.
     * @param BingCampaignBidModifier $campaignBidModifier
     * @param BingServiceManager $serviceManager
     * @param EntityManagerInterface $em
     * @param DocumentManager $dm
     * @param LoggerInterface $bingLogger
     */
    public function __construct(
        BingCampaignBidModifier $campaignBidModifier,
        BingServiceManager $serviceManager,
        EntityManagerInterface $em,
        DocumentManager $dm,
        LoggerInterface $bingLogger
    ) {
        parent::__construct(AdSystemEnum::BING, $em, $dm, $bingLogger);

        $this->campaignBidModifier = $campaignBidModifier;
        $this->serviceManager = $serviceManager;
    }

    /**
     * @return BingServiceManager
     */
    public function getBingServiceManager()
    {
        return $this->serviceManager;
    }

    /**
     * @throws \Doctrine\ODM\MongoDB\MongoDBException
     */
    public function uploadBidModifiers()
    {
        $em = $this->getEntityManager();
        $dm = $this->getDocumentManager();

        $systemCampaignsByAccount = [];

        $adwCampaignsForUpdate = $dm->getRepository(CampaignProcess::class)
            ->getListForUpdate($dm, self::AMOUNT_ITEM_TO_PROCESS);

        $entityName = ProviderEntityName::getForAccountsBySystem($this->adSystem);

        foreach ($adwCampaignsForUpdate as $systemCampaign) {
            $systemCampaignsByAccount[$systemCampaign['systemAccount']][] = $systemCampaign;
        }

        foreach ($systemCampaignsByAccount as $systemAccountId => $adwCampaigns) {
            $account = $em->getRepository($entityName)->findOneBy(['id' => $systemAccountId]);
            if (empty($account)) {
                $message = date('m/d/Y h:i:s ', time()). PHP_EOL
                    . "Exist Bing account ( id:$systemAccountId ) in HOTS database". PHP_EOL;
                $this->writeToLog($message);

                continue;
            }

            $systemCampaignManager = $this->getBingServiceManager()
                ->getCampaignManagementService($account->getSystemAccountId());

            $IdsForDeleteFromQueue = [];

            foreach ($adwCampaigns as $adwCampaign) {
                /** @var KcCampaign $kcCampaign */
                $kcCampaign = $dm->getRepository(KcCampaign::class)->getByAttributesOne($dm, [
                    'campaigns._id' => new ObjectId($adwCampaign['campaignId'])
                ]);

                // Delete device bid modifiers if they exists.
                // We can save device bid modifiers ids after uploading and use them for an update next time.
                // But we decided to use logic when removing and uploading device bid modifiers again.
                /** @var Campaign $campaign */
                foreach ($kcCampaign->getCampaigns() as $campaign) {
                    if ($campaign->getId() == (string)$adwCampaign['campaignId']) {
                        $currentIds = [];

                        $request = new GetCampaignCriterionsByIdsRequest();

                        $request->CampaignId = $adwCampaign['systemCampaignId'];
                        $request->CriterionType = CampaignCriterionType::Device;

                        $result = $systemCampaignManager->GetService()->GetCampaignCriterionsByIds($request);

                        if (isset($result->CampaignCriterions) &&
                            isset($result->CampaignCriterions->CampaignCriterion)
                            && count($result->CampaignCriterions->CampaignCriterion)
                        ) {
                            $result = $result->CampaignCriterions->CampaignCriterion;

                            foreach ($result as $item) {
                                $currentIds[] = $item->Id;
                            }
                        }

                        if ($currentIds) {
                            $resultRemoveCriterionsInSystem = $this->campaignBidModifier
                                ->deleteCampaignCriterionsByCampaign($adwCampaign['systemCampaignId'], $currentIds);
                        }

                        $dm->flush();
                        break;
                    }
                }

                // Upload new device bid modifiers

                $operations = [];

                foreach (BingBidModifier::DEVICE_TYPES as $DEVICE_TYPE) {
                    $deviceFieldName = BingCampaignBidModifier::getQueueFieldByDeviceType($DEVICE_TYPE);

                    if(isset($adwCampaign['devicesBidModifier'][$deviceFieldName])) {
                        $result = $this->campaignBidModifier->makeDeviceBidModifierOperation(
                            $account->getSystemAccountId(),
                            $adwCampaign['systemCampaignId'],
                            $DEVICE_TYPE,
                            $adwCampaign['devicesBidModifier'][$deviceFieldName]
                        );

                        $operations[] = new SoapVar(
                            $result,
                            SOAP_ENC_OBJECT,
                            'BiddableCampaignCriterion',
                            $systemCampaignManager->GetNamespace()
                        );
                    }
                }

                $ids = $this->campaignBidModifier->uploadCriterionOperations($operations);

                if($ids) {
                    $IdsForDeleteFromQueue[] = new ObjectId($adwCampaign['_id']);
                }
            }

            if (!empty($IdsForDeleteFromQueue)) {
                $dm->getRepository(CampaignProcess::class)->removeByIds($dm, $IdsForDeleteFromQueue);
            }
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
