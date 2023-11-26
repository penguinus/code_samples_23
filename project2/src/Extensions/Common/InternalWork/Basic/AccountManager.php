<?php

namespace App\Extensions\Common\InternalWork\Basic;

use Airbrake\Notifier;
use App\Document\AdgroupsQueue;
use App\Document\AdsQueue;
use App\Document\Campaign;
use App\Document\CampaignProcess;
use App\Document\ExtensionsQueue;
use App\Document\KcCampaign as MongoKcCampaign;
use App\Document\KeywordsQueue;
use App\Entity\AdwordsAccount;
use App\Entity\BingAccount;
use App\Entity\KcCampaign as MysqlKcCampaign;
use App\Entity\TimeRange;
use App\Enums\Account\AccountLifeCycleStatus;
use App\Extensions\Common\AdSystemEnum;
use App\Extensions\Common\ExternalWork\AccountInterface;
use App\Extensions\Common\InternalWork\Interfaces\AccountManagerInterface;
use App\Extensions\Common\ServiceEnum;
use App\Interfaces\EntityInterface\SystemAccountInterface;
use App\Providers\ProviderEntityName;
use App\Providers\ProviderServiceManager;
use Doctrine\ODM\MongoDB\DocumentManager;
use Doctrine\ODM\MongoDB\MongoDBException;
use Doctrine\ORM\EntityManager;
use Doctrine\ORM\EntityManagerInterface;
use Doctrine\ORM\OptimisticLockException;
use Doctrine\ORM\ORMException;
use Psr\Container\ContainerInterface;

/**
 * Class AccountManager
 *
 * @package App\Extensions\Common\InternalWork\Basic
 */
abstract class AccountManager implements AccountManagerInterface
{
    /**
     * @var ContainerInterface
     */
    protected ContainerInterface $container;

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
     * AccountManager constructor.
     *
     * @param ContainerInterface        $container
     * @param string                    $adSystem
     * @param EntityManagerInterface    $em
     */
    public function __construct(ContainerInterface $container, string $adSystem, EntityManagerInterface $em)
    {
        $this->container    = $container;
        $this->adSystem     = strtolower($adSystem);
        $this->em           = $em;
    }

    /**
     * @return EntityManagerInterface
     */
    protected function getEntityManager(): EntityManagerInterface
    {
        return $this->em;
    }

    /**
     * @return Notifier
     */
    protected function getAirbrakeNotifier(): Notifier
    {
        return $this->container->get('ami_airbrake.notifier');
    }

    /**
     * @return DocumentManager
     */
    protected function getDocumentManager(): DocumentManager
    {
        if (isset($this->dm) && $this->dm instanceof DocumentManager) {
            return $this->dm;
        } else {
            /** @var DocumentManager $dm */
            $dm = $this->get('doctrine_mongodb')->getManager();
            $dm->setAdSystem($this->adSystem);
            $this->dm = $dm;

            return $dm;
        }
    }

    /**
     * @param string $id
     *
     * @return object
     */
    abstract public function get($id);

    /**
     * @return int
     */
    abstract public function getKeywordLimitInAccount(): int;

    /**
     * @return SystemAccountInterface
     */
    abstract public function getModel(): SystemAccountInterface;

    /**
     * @return array
     */
    abstract public function getAccounts(): array;

    /**
     * @param array $accounts
     * @return array
     */
    abstract public function getActiveAccounts(array $accounts): array;

    /**
     * @param array $accounts
     * @return array
     */
    abstract public function getInactiveAccounts(array $accounts): array;

    /**
     * @return array
     */
    abstract public function getActiveAccountIds(): array;

    /**
     * @param string $name
     * @return bool
     */
    abstract public function checkUniqueName(string $name): bool;

    /**
     * @param SystemAccountInterface $account
     * @return int
     */
    abstract public function getAmountKeywordsByAccount(SystemAccountInterface $account): int;

    /**
     * @param string $name
     * @throws \Exception
     */
    public function create(string $name)
    {
        try {
            $accountExternalName = ProviderServiceManager::getServiceName($this->adSystem, ServiceEnum::ACCOUNT);

            /**@var AccountInterface $accountExternal */
            $accountExternal = $this->get($accountExternalName);

            $id = $accountExternal->create($name);

            if ($id && $id != 0) {
                /** @var EntityManager $em */
                $em = $this->getEntityManager();
                $accountInternal = $this->getModel();
                $accountInternal->fill(['keywords' => 0, 'name' => $name, 'systemAccountId' => $id]);

                $em->persist($accountInternal);
                $em->flush();
            } else {
                throw new \Exception('Failed to create a new account.', 400);
            }
        } catch (\Exception $e) {
            throw new \Exception($e->getMessage(), 400);
        }
    }

    /**
     * @param string $name
     * @param SystemAccountInterface $account
     * @throws \Exception
     */
    public function update(string $name, SystemAccountInterface $account)
    {
        try {
            $accountExternalName = ProviderServiceManager::getServiceName($this->adSystem, ServiceEnum::ACCOUNT);
            /**@var AccountInterface $accountExternal */
            $accountExternal = $this->get($accountExternalName);
            $accountExternal->update($name, $account->getSystemAccountId());

            /** @var EntityManager $em */
            $em = $this->getEntityManager();
            $account->setName($name);
            $em->flush();
        } catch (\Exception $e) {
            throw new \Exception($e->getMessage(), 400);
        }
    }

    /**
     * @param int[] $accounts
     * @throws \Exception
     */
    public function delete(array $accounts)
    {
        try {
            $dm = $this->getDocumentManager();
            $dm->setAdSystem($this->adSystem);

            $accountExternalName = ProviderServiceManager::getServiceName($this->adSystem, ServiceEnum::ACCOUNT);

            foreach ($accounts as $account) {
                $this->get($accountExternalName)->delete($account);
            }
            $accountIds = array_keys($accounts);

            $this->cleanUpAllContentByAccounts($dm, $accountIds);
        } catch (\Exception $e) {
            throw new \Exception($e->getMessage(), 400);
        }
    }

    /**
     * @param array $accounts
     * @param int   $brandTemplateKeywords
     *
     * @return SystemAccountInterface|null
     * @throws ORMException|OptimisticLockException
     */
    public function getAvailableAccount(array $accounts, int $brandTemplateKeywords): ?SystemAccountInterface
    {
        /** @var EntityManager $em */
        $em = $this->getEntityManager();

        $entityName = ProviderEntityName::getForAccountsBySystem($this->adSystem);
        $result = null;
        foreach ($accounts as $account) {
            /**@var SystemAccountInterface $systemAccount */
            $systemAccount = $em->getRepository($entityName)->findOneBy(['id' => $account, 'available' => 1]);

            if (!empty($systemAccount)) {
                $kwdsCount = $systemAccount->getKeywords();
                $kwdsAvailable = $this->getKeywordLimitInAccount() - $kwdsCount;

                if ($kwdsAvailable >= $brandTemplateKeywords) {
                    $kwdsCount += $brandTemplateKeywords;
                    $systemAccount->setKeywords($kwdsCount);
                    $em->flush();
                    $result = $systemAccount;
                    break;
                }
            }
        }

        return $result;
    }

    /**
     * @throws MongoDBException
     * @throws \Exception
     */
    public function updateAccounts()
    {
        /** @var EntityManager $em */
        $em = $this->getEntityManager();

        $dm = $this->getDocumentManager();
        $dm->setAdSystem($this->adSystem);

        $entityName = ProviderEntityName::getForAccountsBySystem($this->adSystem);

        $accountServiceName = ProviderServiceManager::getServiceName($this->adSystem, ServiceEnum::ACCOUNT);
        $accountService = $this->get($accountServiceName);

        $accounts = $this->getAccounts();

        $activeAccounts = $this->getActiveAccounts($accounts);

        if ($activeAccounts) {
            foreach ($activeAccounts as $account) {
                $entity = $em->getRepository($entityName)->findOneBy(['systemAccountId' => $account['systemAccountId']]);

                if (is_null($entity)) {
                    $entity = $this->getModel();

                    $entity->fill([
                        'systemAccountId' => $account['systemAccountId'],
                        'name' => $account['name'],
                        'keywords' => $accountService->getAmountKeywords($account['systemAccountId'])
                    ]);

                    $em->persist($entity);
                } else {
                    $entity->fill([
                        'available' => true,
                        'name' => $account['name'],
                        'status' => AccountLifeCycleStatus::HOTS_ACCOUNT_STATUS[AccountLifeCycleStatus::ACTIVE]
                    ]);
                }
            }
        }

        $inactiveAccounts = $this->getInactiveAccounts($accounts);
        if ($inactiveAccounts) {
            /** @var AdwordsAccount[]|BingAccount[] $accounts */
            $accounts = $em->getRepository($entityName)
                ->findByAttributesIn(['systemAccountId' => array_column($inactiveAccounts, 'systemAccountId')]);

            if (!empty($accounts)) {
                $accountIds = [];
                foreach ($accounts as $account) {
                    switch ($account->getStatus()) {
                        case AccountLifeCycleStatus::HOTS_ACCOUNT_STATUS[AccountLifeCycleStatus::ACTIVE]:
                            $this->getAirbrakeNotifier()->notify(new \Exception(
                                '['.ucfirst(strtolower($this->adSystem)).'] "'. $account->getName() .
                                '" account is no longer active.'. PHP_EOL
                            ));

                            $kcCampaign = $dm->getRepository(MongoKcCampaign::class)
                                ->getByAttributesIn($dm, ['accounts' => [$account->getId()]], [],true);

                            if (!empty($kcCampaign)) {
                                $accountIds['OnlyQueue'][] = $account->getId();
                                $account->setStatus(AccountLifeCycleStatus::HOTS_ACCOUNT_STATUS[AccountLifeCycleStatus::PENDING_APPROVED]);
                                $account->setAvailable(false);
                            } else {
                                $em->remove($account);
                            }

                            break;
                        case AccountLifeCycleStatus::HOTS_ACCOUNT_STATUS[AccountLifeCycleStatus::CLEANUP]:
                            print date('m/d/Y h:i:s ', time()). PHP_EOL;
                            print "\"{$account->getName()}\" account is cleanup...". PHP_EOL;

                            $accountIds['AllContent'][] = $account->getId();
                            break;
                    }
                }

                if (isset($accountIds['OnlyQueue'])) {
                    $dm->getRepository(AdgroupsQueue::class)
                        ->removeByAttributesIn($dm, ['systemAccount' => $accountIds['OnlyQueue']]);
                    $dm->getRepository(AdsQueue::class)
                        ->removeByAttributesIn($dm, ['systemAccount' => $accountIds['OnlyQueue']]);
                    $dm->getRepository(KeywordsQueue::class)
                        ->removeByAttributesIn($dm, ['systemAccount' => $accountIds['OnlyQueue']]);
                    $dm->getRepository(ExtensionsQueue::class)
                        ->removeByAttributesIn($dm, ['systemAccount' => $accountIds['OnlyQueue']]);
                    $dm->getRepository(CampaignProcess::class)
                        ->removeByAttributesIn($dm, ['systemAccount' => $accountIds['OnlyQueue']]);
                    $em->getRepository(ProviderEntityName::getForBatchJobBySystem($this->adSystem))
                        ->removeByAttributesIn(['systemAccount' => $accountIds['OnlyQueue']]);
                }
                if (isset($accountIds['AllContent'])) {
                    $this->cleanUpAllContentByAccounts($dm, $accountIds['AllContent']);
                }
            }
        }

        $em->flush();
    }

    /**
     * @param DocumentManager   $dm
     * @param array             $accountIds
     *
     * @throws MongoDBException|ORMException|OptimisticLockException
     */
    public function cleanUpAllContentByAccounts(DocumentManager $dm, array $accountIds = [])
    {
        /** @var EntityManager $em */
        $em = $this->getEntityManager();

        $kcCampaignsInMongo = $dm->getRepository(MongoKcCampaign::class)->getByAttributesIn($dm,
            ['accounts' => $accountIds],
            ['accounts', 'backendId', 'campaigns.id', 'campaigns.systemCampaignId', 'campaigns.systemAccount'],
            true
        );

        if (empty($kcCampaignsInMongo))
            return;

        $backendIds = [];
        /** @var MongoKcCampaign $kcCampaignInMongo */
        foreach ($kcCampaignsInMongo as $kcCampaignInMongo) {
            $backendIds[] = $kcCampaignInMongo->getBackendId();

            $brandTemplateId = $em->getRepository(MysqlKcCampaign::class)
                ->getBrandTemplateIdByBackendId($kcCampaignInMongo->getBackendId());
            $dm->setBrandTemplateId($brandTemplateId);

            $systemCampaignIds = [];
            /** @var Campaign $campaign */
            foreach ($kcCampaignInMongo->getCampaigns() as $campaign) {
                if (in_array($campaign->getSystemAccount(), $accountIds)) {
                    $systemCampaignIds[] = $campaign->getSystemCampaignId();
                    $kcCampaignInMongo->removeCampaign($campaign);
                    CampaignManager::cleanUpAllQueuesByCampaignId($campaign->getId(), $dm);
                }
            }
            foreach ($accountIds as $accountId) {
                $kcCampaignInMongo->removeAccount($accountId);
            }

            $dm->getRepository(MongoKcCampaign::class)
                ->removeContentByAttributesIn($dm, ['systemCampaignId' => array_filter($systemCampaignIds)]);

            $dm->flush();

            CampaignManager::deleteOrSetAmountForKcCampaign($dm, $kcCampaignInMongo->getBackendId());
        }

        $em->getRepository(ProviderEntityName::getForBatchJobBySystem($this->adSystem))
            ->removeByAttributesIn(['systemAccount' => $accountIds]);
        $em->getRepository(ProviderEntityName::getForAccountsBySystem($this->adSystem))
            ->removeByAttributesIn(['id' => $accountIds]);

        /** @var MysqlKcCampaign[] $kcCampaignInMySql */
        $kcCampaignInMySql = $em->getRepository(MySqlKcCampaign::class)
            ->findByAttributesIn(['backendId' => $backendIds]);
        foreach ($kcCampaignInMySql as $kcCampaign) {
            $kcCampaignInMongoByBackendId = $dm->createQueryBuilder(MongoKcCampaign::class)
                ->refresh()
                ->field('backendId')->equals($kcCampaign->getBackendId())
                ->getQuery()->execute();

            if ($kcCampaignInMongoByBackendId)
                continue;

            // remove entity if it does not isset other adSystem
            $dm->setAdSystem(current(array_diff(AdSystemEnum::AD_SYSTEMS, [strtoupper($this->adSystem)])));
            $anotherAdSystemKcCampaignInMongoByBackendId = $dm->createQueryBuilder(MongoKcCampaign::class)
                ->refresh()
                ->field('backendId')->equals($kcCampaign->getBackendId())
                ->getQuery()->execute();

            if (!$anotherAdSystemKcCampaignInMongoByBackendId) {
                $em->getRepository(TimeRange::class)->removeByAttributes(
                    ['campaignBackendId' => $kcCampaign->getBackendId()]);
                $em->remove($kcCampaign);
            }
        }
        $em->flush();
    }

    /**
     *
     */
    public function updateAccountsKeywords()
    {
        ini_set("memory_limit", '3G');

        $em = $this->getEntityManager();

        $entityName = ProviderEntityName::getForAccountsBySystem($this->adSystem);
        /**
         * @var SystemAccountInterface[] $accounts
         */
        $accounts = $em->getRepository($entityName)->findBy(['available' => 1]);

        $activeAccountIds = $this->getActiveAccountIds();

        foreach ($accounts as $account) {
            if (empty($account->getSystemAccountId()) ||
                !in_array($account->getSystemAccountId(), $activeAccountIds)) {
                continue;
            }

            $account->setKeywords($this->getAmountKeywordsByAccount($account));

            $em->flush();
        }
    }
}