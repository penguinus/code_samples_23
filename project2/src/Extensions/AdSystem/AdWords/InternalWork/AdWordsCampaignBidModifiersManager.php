<?php

namespace App\Extensions\AdSystem\AdWords\InternalWork;

use App\Document\CampaignProcess;
use App\Extensions\AdSystem\AdWords\ExternalWork\AdWordsCampaignBidModifier;
use App\Extensions\Common\AdSystemEnum;
use App\Extensions\Common\InternalWork\Basic\CampaignBidModifiersManager;
use App\Interfaces\EntityInterface\SystemAccountInterface;
use App\Providers\ProviderEntityName;
use Doctrine\ODM\MongoDB\DocumentManager;
use Doctrine\ORM\EntityManagerInterface;
use Psr\Log\LoggerInterface;

/**
 * Class AdWordsCampaignBidModifiersManager
 *
 * @package App\Extensions\AdSystem\Bing\InternalWork
 */
class AdWordsCampaignBidModifiersManager extends CampaignBidModifiersManager
{
    /**
     * @var AdWordsCampaignBidModifier
     */
    private AdWordsCampaignBidModifier $campaignBidModifier;

    /**
     * AdWordsCampaignBidModifiersManager constructor.
     *
     * @param AdWordsCampaignBidModifier    $campaignBidModifier
     * @param EntityManagerInterface        $em
     * @param DocumentManager               $dm
     * @param LoggerInterface               $adwordsLogger
     */
    public function __construct(
        AdWordsCampaignBidModifier  $campaignBidModifier,
        EntityManagerInterface      $em,
        DocumentManager             $dm,
        LoggerInterface             $adwordsLogger
    ) {
        parent::__construct(AdSystemEnum::ADWORDS, $em, $dm, $adwordsLogger);

        $this->campaignBidModifier = $campaignBidModifier;
    }

    /**
     */
    public function uploadBidModifiers()
    {
        $em = $this->getEntityManager();
        $dm = $this->getDocumentManager();

        $adwCampaignsForUpdate = $dm->getRepository(CampaignProcess::class)->getListForUpdate($dm);

        // sort by account
        $adwCampaignsByAccount = [];

        $entityName = ProviderEntityName::getForAccountsBySystem($this->adSystem);
        foreach ($adwCampaignsForUpdate as $adwCampaign) {
            $adwCampaignsByAccount[$adwCampaign['systemAccount']][] = $adwCampaign;
        }

        foreach ($adwCampaignsByAccount as $systemAccountId => $adwCampaigns) {
            /** @var SystemAccountInterface $account */
            $account = $em->getRepository($entityName)->findOneBy(['id' => $systemAccountId]);

            if (empty($account)) {
                $message = date('m/d/Y h:i:s ', time()). PHP_EOL
                    . "Google Account doesn't exists ( id:$systemAccountId ) in HOTS database". PHP_EOL;
                $this->writeToLog($message);

                continue;
            }

            $IdsForDeleteFromQueue = [];
            $operations = [];
            foreach ($adwCampaigns as $adwCampaign) {
                foreach ($adwCampaign['devicesBidModifier'] as $typeDevice => $deviceBidModifier) {
                    //make operation
                    $operations[] = $this->campaignBidModifier->makeDeviceBidModifierOperation(
                        $account->getSystemAccountId(),
                        $adwCampaign['systemCampaignId'],
                        $typeDevice,
                        $deviceBidModifier
                    );

                }

                $IdsForDeleteFromQueue[] = $adwCampaign['_id'];
            }

            if (!empty($operations)) {
                $result = $this->campaignBidModifier
                    ->uploadCriterionOperations($operations, $account->getSystemAccountId());

                if ($result) {
                    //delete items from Queue
                    $dm->getRepository(CampaignProcess::class)->removeByIds($dm, $IdsForDeleteFromQueue);
                }
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
