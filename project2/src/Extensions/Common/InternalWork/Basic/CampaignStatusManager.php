<?php

namespace App\Extensions\Common\InternalWork\Basic;

use App\Document\Campaign;
use App\Document\KcCampaign as MongoKcCampaign;
use App\Entity\Client;
use App\Entity\KcCampaign as MySqlKcCampaign;
use App\Exceptions\KcCampaignLockedException;
use App\Extensions\AdSystem\Bing\ExternalWork\Exception\RefreshTokenExpiredException;
use App\Extensions\Common\AdSystemEnum;
use App\Extensions\Common\ExternalWork\CampaignInterface;
use App\Extensions\Common\ServiceEnum;
use App\Extensions\ServiceManager;
use App\Interfaces\EntityInterface\SystemAccountInterface;
use App\Providers\ProviderEntityName;
use App\Services\ChangeLogManager;
use Doctrine\ODM\MongoDB\DocumentManager;
use Doctrine\ODM\MongoDB\DocumentNotFoundException;
use Doctrine\ORM\EntityManagerInterface;
use Doctrine\ORM\EntityNotFoundException;
use MongoDB\BSON\ObjectId;

class CampaignStatusManager
{
    /**
     * @var EntityManagerInterface
     */
    protected EntityManagerInterface $em;

    /**
     * @var DocumentManager
     */
    protected DocumentManager $dm;

    /**
     * @var ServiceManager
     */
    protected ServiceManager $serviceManager;

    /**
     * @var ChangeLogManager
     */
    protected ChangeLogManager $changeLogManager;

    /**
     * CampaignStatusManager constructor.
     * @param EntityManagerInterface    $em
     * @param DocumentManager           $dm
     * @param ServiceManager            $serviceManager
     * @param ChangeLogManager          $changeLogManager
     */
    public function __construct(
        EntityManagerInterface  $em,
        DocumentManager         $dm,
        ServiceManager          $serviceManager,
        ChangeLogManager        $changeLogManager
    ) {
        $this->em = $em;
        $this->dm = $dm;
        $this->serviceManager   = $serviceManager;
        $this->changeLogManager = $changeLogManager;
    }

    /**
     * @return EntityManagerInterface
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
     * @return ServiceManager
     */
    protected function getServiceManager(): ServiceManager
    {
        return $this->serviceManager;
    }

    /**
     * @return ChangeLogManager
     */
    protected function getChangeLogManager(): ChangeLogManager
    {
        return $this->changeLogManager;
    }

    /**
     * @param Client $client
     * @param bool $status
     * @throws KcCampaignLockedException | DocumentNotFoundException
     * @throws \Exception
     */
    public function updateKcCampaignsByClientStatus(Client $client, bool $status)
    {
        /** @var EntityManagerInterface $em */
        $em = $this->getEntityManager();

        if ($status) {
            $attributes = ['client' => $client->getId(), 'hold' => 0];
            $unHoldAmount = $em->getRepository(MySqlKcCampaign::class)->countBy($attributes);

            if ($unHoldAmount == 0) {
                throw new KcCampaignLockedException('All kc campaigns are held! Click on "UnHold" to activate specific kc campaign.');
            }
        }

        /**@var \App\Entity\KcCampaign[] $kcCampaignsInMySql*/
        $kcCampaignsInMySql = $em->getRepository('App:KcCampaign')->findBy(['client' => $client->getid()]);

        foreach ($kcCampaignsInMySql as $kcCampaignInMySql) {
            try {
                $this->updateKcCampaignStatus($kcCampaignInMySql, $status);
            } catch (KcCampaignLockedException $e) {
            }
        }

        $client->setStatus($status);

        $em->persist($client);
        $em->flush();

        $this->getChangeLogManager()
            ->changeLog($status ? "UnPause" : "Pause", "Client", null, $client->getName(), null, null);
    }

    /**
     * @param MySqlKcCampaign $kcCampaignInMySql
     * @param bool $status
     * @throws KcCampaignLockedException | DocumentNotFoundException
     * @throws \Exception
     */
    public function updateKcCampaignStatus(MySqlKcCampaign $kcCampaignInMySql, bool $status)
    {
        /** @var EntityManagerInterface $em */
        $em = $this->getEntityManager();

        if ($kcCampaignInMySql->getHold() && $status) {
            throw new KcCampaignLockedException('Kc campaign is held! Click on "UnHold" to activate kc campaign.');
        }

        foreach (AdSystemEnum::AD_SYSTEMS as $adSystem) {
            try {
                $this->updateSystemKcCampaignStatus($kcCampaignInMySql, $adSystem, $status);
            } catch (KcCampaignLockedException | DocumentNotFoundException $e) {
            }
        }
        # Set common Kc Campaign status in MySql
        $kcCampaignInMySql->setStatusHots($status);
        $em->flush();
    }

    /**
     * @param MySqlKcCampaign $kcCampaignInMySql
     * @param bool $status
     * @param string $adSystem
     * @throws KcCampaignLockedException | DocumentNotFoundException
     * @throws \Exception
     */
    public function updateKcCampaignStatusBySystem(MySqlKcCampaign $kcCampaignInMySql, string $adSystem, bool $status)
    {
        /** @var EntityManagerInterface $em */
        $em = $this->getEntityManager();

        $this->updateSystemKcCampaignStatus($kcCampaignInMySql, $adSystem, $status);

        # Set common Kc Campaign status in MySql
        # If even one system kc campaign or single campaign is active entire kc campaign will be considered as active.
        if ($status) {
            $kcCampaignInMySql->setStatusHots($status);
        } else {
            $dm = $this->getDocumentManager();
            $dm->setAdsystem($adSystem);

            $commonKcCampaignStatus = CampaignManager::getCommonKcCampaignStatus(
                $kcCampaignInMySql->getBackendId(), $adSystem, $dm);


            $kcCampaignInMySql->setStatusHots($commonKcCampaignStatus);
        }

        $em->flush();
    }

    /**
     * @param MySqlKcCampaign $kcCampaignInMySql
     * @param string $adSystem
     * @param int $status
     * @throws KcCampaignLockedException | DocumentNotFoundException
     * @throws RefreshTokenExpiredException
     * @throws \Doctrine\ODM\MongoDB\MongoDBException
     */
    protected function updateSystemKcCampaignStatus(
        MySqlKcCampaign $kcCampaignInMySql,
        string $adSystem,
        int $status
    ) {
        /** @var EntityManagerInterface $em */
        $em = $this->getEntityManager();

        $dm = $this->getDocumentManager();
        $dm->setAdsystem($adSystem);

        $attributes = ['backendId' => $kcCampaignInMySql->getBackendId()];
        $select = ['backendId', 'campaigns', 'hold'];
        /** @var MongoKcCampaign $kcCampaignInMongo */
        $kcCampaignInMongo = $dm->getRepository(MongoKcCampaign::class)->getByAttributesOne($dm, $attributes, $select);

        if(!isset($kcCampaignInMongo)) {
            throw DocumentNotFoundException
                ::documentNotFound(ucfirst($adSystem) . 'KcCampaign', 'backendId: ' . $kcCampaignInMySql->getBackendId());
        }

        if ($kcCampaignInMongo->getHold() && $status) {
            throw new KcCampaignLockedException(
                ucfirst($adSystem).' kc campaign is held! Click on "UnHold" to activate kc campaign.'
            );
        }

        /**@var CampaignInterface $campaignManager*/
        $campaignManager = $this->getServiceManager()->getService($adSystem, ServiceEnum::CAMPAIGN);

        $accountEntityName = ProviderEntityName::getForAccountsBySystem($adSystem);
        $clientCustomerId = $systemAccount = null;
        $systemCampaignIds = [];

        foreach ($kcCampaignInMongo->getCampaigns() as $hotsCampaign) {
            # don't need activate campaign when the status hold is active.
            if (!$hotsCampaign->getSystemCampaignId() || ($hotsCampaign->getHold() && $status)) {
                continue;
            }

            if ($systemAccount !== $hotsCampaign->getSystemAccount()) {
                if (!is_null($systemAccount)) {
                    // Make the mutate request. Change status for campaigns
                    if (!$campaignManager->changeCampaignStatus($clientCustomerId, $systemCampaignIds, $status)) {
                        throw new \Exception('Kc Campaign status can\'t be changed. Customer ID: '
                            . $clientCustomerId);
                    }
                    $systemCampaignIds = [];
                }

                $systemAccount = $hotsCampaign->getSystemAccount();
                $clientCustomerId = $em->getRepository($accountEntityName)->findOneBy(['id' => $systemAccount, 'available' => 1])->getSystemAccountId();
            }

            $systemCampaignIds[] = $hotsCampaign->getSystemCampaignId();
            /* Update status in database */
            $hotsCampaign->setStatus($status);
        }

        if (count($systemCampaignIds)) {
            // Make the mutate request. Change status for campaigns
            if (!$campaignManager->changeCampaignStatus($clientCustomerId, $systemCampaignIds, $status)) {
                throw new \Exception('Something wrong! Kc Campaign status can\'t be changed. Customer ID: '
                    . $clientCustomerId);
            }
            $dm->flush();
        }

        $kcCampaignInMongo->setStatusHots($status);
        $dm->flush();

        $this->getChangeLogManager()->changeLog($status ? "UnPause" : "Pause", "KcCampaign",
            null, null, null, $kcCampaignInMySql, null, $adSystem);
    }

    /**
     * @param string $id
     * @param int $backendId
     * @param string $adSystem
     * @param bool $status
     * @throws DocumentNotFoundException
     * @throws KcCampaignLockedException
     * @throws \Doctrine\ODM\MongoDB\MongoDBException
     */
    public function pauseCampaignAction(string $id, int $backendId, string $adSystem, bool $status)
    {
        $em = $this->getEntityManager();

        $dm = $this->getDocumentManager();
        $dm->setAdSystem($adSystem);

        /** @var MongoKcCampaign $kcCampaignInMongo */
        $kcCampaignInMongo = $dm->createQueryBuilder('\App\Document\KcCampaign')
            ->refresh(true)
            ->select('campaigns')
            ->field('backendId')->equals($backendId)
            ->field('campaigns.id')->equals(new ObjectId($id))
            ->field('campaigns.systemCampaignId')->exists(true)
            ->getQuery()->getSingleResult();

        if(!isset($kcCampaignInMongo)) {
            throw DocumentNotFoundException
                ::documentNotFound(ucfirst($adSystem) . 'Campaign', 'id' . $id);
        }

        /** @var Campaign $hotsCampaign */
        foreach ($kcCampaignInMongo->getCampaigns() as $hotsCampaign) {
            if ((strcasecmp($hotsCampaign->getId(), $id) != 0))
                continue;

            if ($hotsCampaign->getHold() && $status) {
                throw new KcCampaignLockedException(
                    ucfirst($adSystem).' Campaign is held! Click on "UnHold" to activate campaign.'
                );
            }

            $accountEntityName = ProviderEntityName::getForAccountsBySystem($adSystem);
            $clientCustomerId = $em->getRepository($accountEntityName)
                ->find($hotsCampaign->getSystemAccount())->getSystemAccountId();
            $systemCampaignIds[] = $hotsCampaign->getSystemCampaignId();

            /**@var CampaignInterface $campaignManager*/
            $campaignManager = $this->getServiceManager()->getService($adSystem, ServiceEnum::CAMPAIGN);
            # Pauses/Actives in ad system
            if (!$campaignManager->changeCampaignStatus($clientCustomerId, $systemCampaignIds, $status)) {
                throw new \Exception('Something wrong! Campaign status can\'t be changed. Customer ID: '
                    . $clientCustomerId);
            }

            $hotsCampaign->setStatus($status);
            $dm->flush();

            /** @var \App\Entity\KcCampaign $kcCampaign */
            $kcCampaign = $em->getRepository('App:KcCampaign')->findOneBy(['backendId' => $backendId]);

            $this->updateKcCampaignStatusByCampaignStatus($kcCampaign, $adSystem, $status);

            $adwCampaignName = $kcCampaign->getName() . "/" . $hotsCampaign->getState() . "/" . $hotsCampaign->getCity();
            $this->getChangeLogManager()->changeLog($status == 0 ? "Pause" : "UnPause", "KcCampaign",
                "Campaign", $adwCampaignName, null, $kcCampaign);

            break;
        }
    }

    /**
     * @param MySqlKcCampaign $kcCampaignInMySql
     * @param bool $status
     * @param string $adSystem
     * @throws \Doctrine\ODM\MongoDB\MongoDBException
     */
    public function updateKcCampaignStatusByCampaignStatus(
        MySqlKcCampaign $kcCampaignInMySql,
        string $adSystem,
        bool $status
    ) {
        $em = $this->getEntityManager();
        $dm = $this->getDocumentManager();
        $dm->setAdSystem($adSystem);

        if ($status) {
            # Set status Active for system kc campaign in Mongo
            $where = ['backendId' => $kcCampaignInMySql->getBackendId()];
            $attributes = ['statusHots' => $status];
            $dm->getRepository(MongoKcCampaign::class)->updateByAttributes($dm, $where, $attributes);
            # Set common status Active for kc campaign in MySql
            # If even one system kc campaign or single campaign is active entire kc campaign will be considered as active.
            $kcCampaignInMySql->setStatusHots($status);
        } else {
            $activeCampaigns = $dm->createQueryBuilder('\App\Document\KcCampaign')
                ->count()
                ->field('backendId')->equals($kcCampaignInMySql->getBackendId())
                ->field('campaigns.status')->equals(true)
                ->getQuery()->execute();

            if ($activeCampaigns == 0) {
                # Set status Pause for system kc campaign in Mongo
                $where = ['backendId' => $kcCampaignInMySql->getBackendId()];
                $attributes = ['statusHots' => $status];
                $dm->getRepository(MongoKcCampaign::class)->updateByAttributes($dm, $where, $attributes);
                # Set common status in MySQL by statuses kc campaign in all systems
                # If even one system kc campaign or single campaign is active entire kc campaign will be considered as active.
                $commonKcCampaignStatus = CampaignManager::getCommonKcCampaignStatus(
                    $kcCampaignInMySql->getBackendId(), $adSystem, $dm);

                $kcCampaignInMySql->setStatusHots($commonKcCampaignStatus);
            }
        }

        $em->flush();
    }

    // Hold

    /**
     * @param MySqlKcCampaign $kcCampaignInMySql
     * @param bool $hold
     * @throws KcCampaignLockedException | DocumentNotFoundException
     * @throws \Exception
     */
    public function updateKcCampaignHold(MySqlKcCampaign $kcCampaignInMySql, bool $hold)
    {
        /** @var EntityManagerInterface $em */
        $em = $this->getEntityManager();

        foreach (AdSystemEnum::AD_SYSTEMS as $adSystem) {
            try {
                $this->updateSystemKcCampaignHoldStatus($kcCampaignInMySql, $adSystem, $hold);
            } catch (KcCampaignLockedException $e) {
            }
        }
        # If hold is activated campaign should be paused
        $status = $hold ? false : true;
        // Set common Kc Campaign status in MySql
        $kcCampaignInMySql->setStatusHots($status);
        $kcCampaignInMySql->setHold($hold);

        $em->flush();
    }

    /**
     * @param bool $hold
     * @param MySqlKcCampaign $kcCampaignInMySql
     * @param string $adSystem
     * @throws DocumentNotFoundException
     * @throws \Doctrine\ODM\MongoDB\MongoDBException
     * @throws \Exception
     */
    public function updateSystemKcCampaignHoldStatus(
        MySqlKcCampaign $kcCampaignInMySql,
        string $adSystem,
        bool $hold
    ) {
        /** @var EntityManagerInterface $em */
        $em = $this->getEntityManager();

        $dm = $this->getDocumentManager();
        $dm->setAdsystem($adSystem);

        $select = ['backendId', 'campaigns', 'statusHots', 'hold'];
        $attributes = ['backendId' => $kcCampaignInMySql->getBackendId()];

        /** @var MongoKcCampaign $kcCampaignInMongo */
        $kcCampaignInMongo = $dm->getRepository(MongoKcCampaign::class)
            ->getByAttributesOne($dm, $attributes, $select);

        if (!isset($kcCampaignInMongo)) {
            throw DocumentNotFoundException
                ::documentNotFound(ucfirst($adSystem) . 'KcCampaign', 'backendId: ' . $kcCampaignInMySql->getBackendId());
        }

        /**@var CampaignInterface $campaignManager*/
        $campaignManager = $this->getServiceManager()->getService($adSystem, ServiceEnum::CAMPAIGN);

        # If hold is activated campaign should be paused
        $status = $hold ? false : true;

        $systemAccount = false;
        $clientCustomerId = null;
        $adSystemCampaignIds = [];

        foreach ($kcCampaignInMongo->getCampaigns() as $hotsCampaign) {
            if (!$hotsCampaign->getSystemCampaignId())
                continue;

            if ($systemAccount !== $hotsCampaign->getSystemAccount()) {

                if ($systemAccount !== false) {
                    // Make the mutate request. Change status for campaigns
                    if (!$campaignManager->changeCampaignStatus($clientCustomerId, $adSystemCampaignIds, $status)) {
                        throw new \Exception(
                            "Something wrong! ". ucfirst($adSystem) ." Kc Campaign status can't be changed. Customer ID: "
                            . $clientCustomerId
                        );
                    }
                    $adSystemCampaignIds = [];
                }

                $systemAccount = $hotsCampaign->getSystemAccount();
                $repositoryName = ProviderEntityName::getForAccountsBySystem($adSystem);

                /**@var SystemAccountInterface $account*/
                $account = $em->getRepository($repositoryName)->find($systemAccount);
                $clientCustomerId = $account->getSystemAccountId();
            }

            $adSystemCampaignIds[] = $hotsCampaign->getSystemCampaignId();
            /* Update status in database */
            $hotsCampaign->setStatus($status);
            $hotsCampaign->setHold($hold ?: null);
        }

        if (count($adSystemCampaignIds)) {
            // Make the mutate request. Change status for campaigns
            if (!$campaignManager->changeCampaignStatus($clientCustomerId, $adSystemCampaignIds, $status)) {
                throw new \Exception(
                    "Something wrong! ". ucfirst($adSystem) ." Kc Campaign status can't be changed. Customer ID: "
                    . $clientCustomerId
                );
            }
        }

        $kcCampaignInMongo->setStatusHots($status);
        $kcCampaignInMongo->setHold($hold ?: null);
        $dm->flush();

        $this->getChangeLogManager()->changeLog($hold ? "Hold" : "UnHold", "KcCampaign",
            null, null, null, $kcCampaignInMySql, null, $adSystem);
    }

    /**
     * @param MySqlKcCampaign $kcCampaignInMySql
     * @param string $adSystem
     * @param bool $hold
     * @throws \Doctrine\ODM\MongoDB\MongoDBException
     */
    public function updateKcCampaignHoldStatus(
        MySqlKcCampaign $kcCampaignInMySql,
        string $adSystem,
        bool $hold
    ) {
        $em = $this->getEntityManager();
        $dm = $this->getDocumentManager();
        $dm->setAdSystem($adSystem);

        if ($hold) {
            $builder = $dm->createQueryBuilder('\App\Document\KcCampaign');
            $unHoldAmount = $builder->count()
                ->field('backendId')->equals($kcCampaignInMySql->getBackendId())
                ->field('campaigns')->elemMatch(
                    $builder->expr()->field('hold')->exists(false)
                )
                ->getQuery()->execute();

            if ($unHoldAmount == 0) {
                # Set status Hold for kc campaign by system in Mongo
                $where = ['backendId' => $kcCampaignInMySql->getBackendId()];
                $attributes = ['hold' => $hold];
                $dm->getRepository(MongoKcCampaign::class)->updateByAttributes($dm, $where, $attributes);
                # Set common hold status in MySQL
                # If all system kc campaign is hold entire kc campaign will be considered as hold.
                $commonHoldStatus = CampaignManager::getCommonKcCampaignHoldStatus(
                    $kcCampaignInMySql->getBackendId(), $adSystem, $dm);

                $kcCampaignInMySql->setHold($commonHoldStatus);
            }
        } else {
            # Set status UnHold for kc campaign by system in Mongo
            $where = ['backendId' => $kcCampaignInMySql->getBackendId()];
            $attributes = ['hold' => null];
            $dm->getRepository(MongoKcCampaign::class)->updateByAttributes($dm, $where, $attributes);
            # Set common status UnHold for kc campaign in MySql
            # If all system kc campaign is hold entire kc campaign will be considered as hold.
            $kcCampaignInMySql->setHold($hold);
        }

        $em->flush();
    }

    /**
     * @param MySqlKcCampaign $kcCampaignInMySql
     * @param string $id
     * @param string $adSystem
     * @param bool $hold
     * @throws \Doctrine\ODM\MongoDB\MongoDBException
     */
    public function updateCampaignHold(
        MySqlKcCampaign $kcCampaignInMySql,
        string $id,
        string $adSystem,
        bool $hold
    )
    {
        /** @var EntityManagerInterface $em */
        $em = $this->getEntityManager();
        $dm = $this->getDocumentManager();
        $dm->setAdsystem($adSystem);

        $attributes = ['backendId' => $kcCampaignInMySql->getBackendId(), 'campaigns.id' => new ObjectId($id)];
        $selectFields = ['hold','campaigns.id', 'campaigns.systemCampaignId', 'campaigns.state',
            'campaigns.city', 'campaigns.status', 'campaigns.hold', 'campaigns.systemAccount'];
        $existence = ['campaigns.systemCampaignId' => true];

        /**
         * Refreshes the persistent state of an entity from the database,
         * overriding any local changes that have not yet been persisted.
         *
         * @var KcCampaign $kcCampaignInMongo
         */
        $kcCampaignInMongo = $dm->getRepository(MongoKcCampaign::class)
            ->resetAndGetByAttributesOne($dm, $attributes, $selectFields, $existence);

        if (empty($kcCampaignInMongo)) {
            throw $this->createNotFoundException(
                "Current campaign backend Id: {$kcCampaignInMySql->getBackendId()} wasn\'t loaded into $adSystem yet!"
            );
        }

        /** @var CampaignInterface $campaignManager */
        $campaignManager = $this->getServiceManager()->getService($adSystem, ServiceEnum::CAMPAIGN);

        /** @var Campaign $hotsCampaign */
        foreach ($kcCampaignInMongo->getCampaigns() as $hotsCampaign) {
            if (strcasecmp($hotsCampaign->getId(), $id) == 0) {
                // IF status hold is true campaign must be paused and vice versa
                $hotsCampaign->setHold($hold ?: null);

                $systemAccount = $hotsCampaign->getSystemAccount();
                $repositoryName = ProviderEntityName::getForAccountsBySystem($adSystem);

                /**@var SystemAccountInterface $account*/
                $account = $em->getRepository($repositoryName)->find($systemAccount);
                $clientCustomerId = $account->getSystemAccountId();
                $adSystemCampaignIds[] = $hotsCampaign->getSystemCampaignId();

                # If hold is activated campaign should be paused
                $status = $hold ? false : true;

                // Make the mutate request. Change status for campaigns
                if (!$campaignManager->changeCampaignStatus($clientCustomerId, $adSystemCampaignIds, $status)) {
                    throw new \Exception('Something wrong! Kc Campaign status can\'t be changed. Customer Id:'
                        . $clientCustomerId
                    );
                }

                $hotsCampaign->setStatus($status);

                $dm->flush();

                $this->updateKcCampaignStatusByCampaignStatus($kcCampaignInMySql, $adSystem, $status);
                $this->updateKcCampaignHoldStatus($kcCampaignInMySql, $adSystem, $hold);

                $adwCampaignName = $kcCampaignInMySql->getName() . "/" . $hotsCampaign->getState() . "/" . $hotsCampaign->getCity();
                $this->getChangeLogManager()->changeLog($hold == 1 ? "Hold" : "UnHold", "KcCampaign", "Campaign",
                    $adwCampaignName, null, $kcCampaignInMySql, null, $adSystem);

                break;
            }
        }
    }

    /**
     * @param MySqlKcCampaign $kcCampaignInMySql
     * @param string $adSystem
     * @param bool $hold
     * @throws DocumentNotFoundException
     * @throws \Doctrine\ODM\MongoDB\MongoDBException
     */
    public function updateKcCampaignHoldBySystem(MySqlKcCampaign $kcCampaignInMySql, string $adSystem, bool $hold)
    {
        /** @var EntityManagerInterface $em */
        $em = $this->getEntityManager();

        /** @var DocumentManager $dm */
        $dm = $this->getDocumentManager();

        $this->updateSystemKcCampaignHoldStatus($kcCampaignInMySql, $adSystem, $hold);
        $this->getChangeLogManager()->changeLog($hold ? "Hold" : "UnHold", "KcCampaign", null, null,
            null, $kcCampaignInMySql, null, $adSystem);

        # If hold is activated campaign should be paused
        $status = $hold ? false : true;

        # Set common Kc Campaign status in MySql
        # If even one system kc campaign is active entire kc campaign will be considered as active.
        if ($status) {
            $kcCampaignInMySql->setStatusHots($status);
        } else {
            $commonKcCampaignStatus = CampaignManager::getCommonKcCampaignStatus(
                $kcCampaignInMySql->getBackendId(), $adSystem, $dm);

            $kcCampaignInMySql->setStatusHots($commonKcCampaignStatus);
        }

        # Set common hold status in MySQL
        # If all system kc campaign is hold entire kc campaign will be considered as hold.
        if ($hold) {
            $commonHoldStatus = CampaignManager::getCommonKcCampaignHoldStatus(
                $kcCampaignInMySql->getBackendId(), $adSystem, $dm);

            $kcCampaignInMySql->setHold($commonHoldStatus);
        } else {
            $kcCampaignInMySql->setHold($hold);
        }

        $em->flush();
    }
}