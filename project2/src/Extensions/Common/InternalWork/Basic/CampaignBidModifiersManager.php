<?php

namespace App\Extensions\Common\InternalWork\Basic;

use App\Document\Campaign;
use App\Document\CampaignProcess;
use App\Document\DevicesBidModifier;
use App\Document\KcCampaign;
use App\Extensions\Common\InternalWork\Interfaces\BidModifiersManagerInterface;
use Doctrine\ODM\MongoDB\DocumentManager;
use Doctrine\ORM\EntityManager;
use Doctrine\ORM\EntityManagerInterface;
use MongoDB\BSON\ObjectId;
use Psr\Log\LoggerInterface;

/**
 * Class CampaignBidModifiersManager
 * @package App\Extensions\Common\InternalWork\Basic
 */
abstract class CampaignBidModifiersManager implements BidModifiersManagerInterface
{
    /**
     * @var string
     */
    protected string $adSystem;

    /**
     * @var EntityManagerInterface
     */
    private EntityManagerInterface $em;

    /**
     * @var DocumentManager
     */
    private DocumentManager $dm;

    /**
     * @var LoggerInterface
     */
    protected LoggerInterface $logger;

    /**
     * CampaignBidModifiersManager constructor.
     * @param string                    $adSystem
     * @param EntityManagerInterface    $em
     * @param DocumentManager           $dm
     * @param LoggerInterface           $logger
     */
    public function __construct(string $adSystem, EntityManagerInterface $em, DocumentManager $dm, LoggerInterface $logger)
    {
        $this->adSystem = strtolower($adSystem);
        $this->em = $em;

        $dm->setAdSystem($this->adSystem);
        $this->dm = $dm;

        $this->logger = $logger;
    }

    /**
     * @return LoggerInterface
     */
    public function getLogger() : LoggerInterface
    {
        return $this->logger;
    }

    /**
     * @return EntityManager
     */
    protected function getEntityManager(): EntityManagerInterface
    {
        return $this->em;
    }

    /**
     * @return DocumentManager
     */
    protected function getDocumentManager(): DocumentManager
    {
        return $this->dm;
    }

    /**
     * @param KcCampaign $kcCampaignInMongo
     * @param array $bidModifiers
     * @return bool
     */
    public function syncBidModifiers(KcCampaign $kcCampaignInMongo, array $bidModifiers): bool
    {
        $dm = $this->getDocumentManager();

        try {
            $kcCampaignInMongo->setDesktopBidModifier($bidModifiers['desktop']);
            $kcCampaignInMongo->setMobileBidModifier($bidModifiers['mobile']);
            $kcCampaignInMongo->setTabletBidModifier($bidModifiers['tablet']);

            //change in format for ad system
            $bidModifiers['mobile']  = isset($bidModifiers['mobile']) ? ($bidModifiers['mobile'] / 100 + 1) : -1;
            $bidModifiers['tablet']  = isset($bidModifiers['tablet']) ? ($bidModifiers['tablet'] / 100 + 1) : -1;
            $bidModifiers['desktop'] = isset($bidModifiers['desktop']) ? ($bidModifiers['desktop'] / 100 + 1) : -1;

            // Sync with campaigns. Add items to queue
            if (!empty($bidModifiers)) {
                $this->syncBidModifiersWithKcCampaign($kcCampaignInMongo, $bidModifiers);
            }

            $dm->persist($kcCampaignInMongo);
            $dm->flush();
        } catch (\Exception $e) {
            $this->getLogger()->error($e->getMessage());

            return false;
        }

        return true;
    }

    /**
     * @param KcCampaign $kcCampaignInMongo
     * @param array $bidModifiers
     * @throws \Doctrine\ODM\MongoDB\MongoDBException
     */
    private function syncBidModifiersWithKcCampaign(KcCampaign $kcCampaignInMongo, array $bidModifiers)
    {
        $dm = $this->getDocumentManager();

        $counter = 1;

        /** @var Campaign $campaign */
        foreach ($kcCampaignInMongo->getCampaigns() as $campaign) {
            if (empty($campaign->getId())) {
                continue;
            }

            if (!$this->updateStillExistCampaignInQueueForUpdate($campaign->getId(), $bidModifiers)) {
                $campaignForQueue = $this->getCampaignForQueueToUpdate(
                    $campaign, $kcCampaignInMongo->getBackendId(), $bidModifiers);

                $dm->persist($campaignForQueue);

                if (($counter % 50) === 0) {
                    $dm->flush();
                    $dm->clear();
                }

                $counter++;
            }
        }

        $dm->flush();
        $dm->clear();

        $this->unSetFieldDevicesBidModifierInAllCampaigns($kcCampaignInMongo->getBackendId());
    }

    /**
     * @param int $kcCampaignBackendId
     * @throws \Doctrine\ODM\MongoDB\MongoDBException
     */
    private function unSetFieldDevicesBidModifierInAllCampaigns(int $kcCampaignBackendId)
    {
        $dm = $this->getDocumentManager();

        $dm->createQueryBuilder('\App\Document\KcCampaign')
            ->updateMany()
            ->field('backendId')->equals($kcCampaignBackendId)
            ->field('campaigns.devicesBidModifier')->exists(true)
            ->field('campaigns.$.devicesBidModifier')->unsetField()
            ->getQuery()
            ->execute();
    }

    /**
     * @param string $campaignId
     * @param array $bidModifiers
     * @return bool
     * @throws \Doctrine\ODM\MongoDB\MongoDBException
     */
    private function updateStillExistCampaignInQueueForUpdate(string $campaignId, array $bidModifiers): bool
    {
        $dm = $this->getDocumentManager();

        /** @var CampaignProcess $existCampaign */
        $existCampaign = $dm->createQueryBuilder('\App\Document\CampaignProcess')
            ->field('update')->exists(true)
            ->field('campaignId')->equals(new ObjectId($campaignId))
            ->getQuery()->getSingleResult();

        if (!empty($existCampaign)) {
            $deviceBidModifier = $existCampaign->getDevicesBidModifier();

            if (isset($bidModifiers['desktop']))
                $deviceBidModifier->setDesktopBidModifier($bidModifiers['desktop']);
            if (isset($bidModifiers['mobile']))
                $deviceBidModifier->setMobileBidModifier($bidModifiers['mobile']);
            if (isset($bidModifiers['tablet']))
                $deviceBidModifier->setTabletBidModifier($bidModifiers['tablet']);

            $existCampaign->setDevicesBidModifier($deviceBidModifier);

            $dm->flush();

            return true;
        }

        return false;
    }

    /**
     * @param Campaign $campaign
     * @param int $kcCampaignBackendId
     * @param array $bidModifiers
     * @return CampaignProcess
     */
    private function getCampaignForQueueToUpdate(
        Campaign $campaign,
        int $kcCampaignBackendId,
        array $bidModifiers
    ): CampaignProcess {
        $campaignToQueue = new CampaignProcess();

        $campaignToQueue->setBackendId($kcCampaignBackendId);
        $campaignToQueue->setCampaignId(new ObjectId($campaign->getId()));
        $campaignToQueue->setSystemCampaignId($campaign->getSystemCampaignId());
        $campaignToQueue->setSystemAccount($campaign->getSystemAccount());

        $deviceBidModifier = new DevicesBidModifier();

        if (isset($bidModifiers['desktop'])) {
            $deviceBidModifier->setDesktopBidModifier($bidModifiers['desktop']);
        }

        if (isset($bidModifiers['mobile'])) {
            $deviceBidModifier->setMobileBidModifier($bidModifiers['mobile']);
        }

        if (isset($bidModifiers['tablet'])) {
            $deviceBidModifier->setTabletBidModifier($bidModifiers['tablet']);
        }

        $campaignToQueue->setDevicesBidModifier($deviceBidModifier);
        $campaignToQueue->setUpdate(true);

        return $campaignToQueue;
    }
}