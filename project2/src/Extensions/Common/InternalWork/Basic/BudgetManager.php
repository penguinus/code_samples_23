<?php

namespace App\Extensions\Common\InternalWork\Basic;

use Airbrake\Notifier;
use App\Document\Campaign;
use App\Document\CampaignProcess;
use App\Document\ErrorsQueue;
use App\Document\KcCampaign;
use App\Enums\ErrorLevel;
use App\Extensions\Common\ExternalWork\BudgetInterface;
use App\Extensions\Common\ExternalWork\CampaignInterface;
use App\Extensions\Common\InternalWork\Interfaces\BudgetManagerInterface;
use App\Providers\ProviderEntityName;
use Doctrine\ODM\MongoDB\DocumentManager;
use Doctrine\ORM\EntityManagerInterface;
use MongoDB\BSON\ObjectId;
use Psr\Log\LoggerInterface;

/**
 * Class BudgetManager
 * @package App\Extensions\Common\InternalWork\Basic
 */
abstract class BudgetManager implements BudgetManagerInterface
{
    /**
     * @var string
     */
    protected $adSystem;

    /**
     * @var DocumentManager
     */
    private $dm;

    /**
     * @var EntityManagerInterface
     */
    private $em;

    /**
     * BudgetManager constructor.
     * @param string $adSystem
     * @param EntityManagerInterface $em
     * @param DocumentManager $dm
     */
    public function __construct(string $adSystem, EntityManagerInterface $em, DocumentManager $dm)
    {
        $this->adSystem = strtolower($adSystem);

        $dm->setAdSystem($this->adSystem);
        $this->dm = $dm;

        $this->em = $em;
    }

    /**
     * @return EntityManagerInterface
     */
    protected function getEntityManager()
    {
        return $this->em;
    }

    /**
     * @return DocumentManager
     */
    protected function getDocumentManager()
    {
        return $this->dm;
    }

    /**
     * @return BudgetInterface
     */
    abstract protected function getBudgetService() : BudgetInterface;

    /**
     * @return LoggerInterface
     */
    abstract protected function getLogger(): LoggerInterface;

    /**
     * @return CampaignInterface
     */
    abstract protected function getCampaignManager(): CampaignInterface;

    /**
     * @return Notifier
     */
    abstract protected function getAirbrakeNotifier(): Notifier;

    /**
     * @param string $kcCampaignName
     * @return string
     */
    public static function getBudgetName($kcCampaignName)
    {
        return 'Budget-' . $kcCampaignName;
    }

    /**
     * Init or update budget for campaigns
     */
    public function syncBudgets()
    {
        $dm = $this->getDocumentManager();

        $campaignsNeedBudgetId = true;

        while ($campaignsNeedBudgetId) {
            /** @var CampaignProcess $campaign */
            $campaign = $dm->createQueryBuilder('\App\Document\CampaignProcess')
                ->field('systemBudgetId')->exists(false)
                ->field('error')->exists(false)
                ->field('add')->exists(true)
                ->getQuery()
                ->getSingleResult();

            if (empty($campaign)) {
                $kcCampaigns = $dm->getRepository(KcCampaign::class)->findBy(["budgetUpdated" => true]);

                if ($kcCampaigns) {
                    foreach ($kcCampaigns as $kcCampaign) {
                        //Update immediately for all type of system
                        $this->updateBudget($kcCampaign);
                    }
                }

                $campaignsNeedBudgetId = false;
            } else {
                $this->initBudget($campaign);
            }
        }
    }

    /**
     * @param KcCampaign $KcCampaign
     */
    public function updateBudget(KcCampaign $KcCampaign)
    {
        $dm = $this->getDocumentManager();
        $em = $this->getEntityManager();

        $kcCampaignAccounts = [];
        $accounts = [];
        $systemBudgetIds = [];

        if (!empty($KcCampaign)) {
            $checkedCampaignError = $dm->getRepository(ErrorsQueue::class)->findOneBy([
                'backendId' => $KcCampaign->getBackendId(), 'type' => ErrorLevel::BUDGET
            ]);

            if (!is_null($checkedCampaignError)) {
                print date('m/d/Y h:i:s ', time()). PHP_EOL;
                print ($checkedCampaignError->getRawError()). PHP_EOL;

                return;
            }

            /** @var Campaign $hotsCampaign */
            foreach ($KcCampaign->getCampaigns() as $hotsCampaign) {
                if (!empty($hotsCampaign->getSystemBudgetId())) {
                    $kcCampaignAccounts[] = $hotsCampaign->getSystemAccount();
                    $systemBudgetIds[$hotsCampaign->getSystemAccount()] = $hotsCampaign->getSystemBudgetId();
                }
            }
        }

        $kcCampaignAccounts = array_unique($kcCampaignAccounts);
        $systemBudgetIds = array_unique($systemBudgetIds);

        $repositoryName = ProviderEntityName::getForAccountsBySystem($this->adSystem);
        foreach ($kcCampaignAccounts as $kcCampaignAccount) {
            $accounts[] = $em->getRepository($repositoryName)->findOneBy(['id' => $kcCampaignAccount, 'available' => 1]);
        }

        $kcCampaignBudget = $KcCampaign->getBudget();
        foreach ($accounts as $account) {
            $systemBudgetId = $systemBudgetIds[$account->getId()];

            $clientCustomerId = $account->getSystemAccountId();
            $budgetId = $this->getBudgetService()->updateBudget($systemBudgetId, $kcCampaignBudget, $clientCustomerId);

            if ($budgetId != false) {
                $KcCampaign->setBudgetUpdated(false);
                $dm->persist($KcCampaign);

                $this->getLogger()->info("Updated budget. $this->adSystem Budget Id = $budgetId \n");
            } else {
                $this->getAirbrakeNotifier()->notify(new \Exception(
                    '['.ucfirst(strtolower($this->adSystem)).'] Budget can\'t be updated'
                    . '. Backend Id: ' . $KcCampaign->getBackendId()
                    . ', Budget Id: ' . $systemBudgetId
                    . ', Account Id: '. $account->getSystemAccountId()
                    . '. When the process was running "updateBudget()".' . PHP_EOL
                ));

                $errorQueue = new ErrorsQueue();
                $errorQueue->setType(ErrorLevel::BUDGET);
                $errorQueue->setRawError("Budget can't be updated. Budget Id = $systemBudgetId, Account Id = ". $account->getSystemAccountId());
                $errorQueue->setSystemItemId($account->getSystemAccountId());
                $errorQueue->setBackendId($KcCampaign->getBackendId()) ;
                $errorQueue->setError("Budget can't be updated.") ;

                $dm->persist($errorQueue);
            }

            $dm->flush();
        }
    }

    /**
     * @param CampaignProcess $campaign
     */
    protected function initBudget(CampaignProcess $campaign)
    {
        $dm = $this->getDocumentManager();
        $em = $this->getEntityManager();

        $kcCampaign = $em->getRepository('App:KcCampaign')
            ->findOneBy(['backendId' => $campaign->getBackendId()], []);
        $kcCampaignInMongo = $dm->getRepository(KcCampaign::class)
            ->getByAttributesOne($dm, ['backendId' => $campaign->getBackendId()], ['budget']);

        $budgetName = self::getBudgetName($kcCampaign->getName());

        $repositoryName = ProviderEntityName::getForAccountsBySystem($this->adSystem);
        $account = $em->getRepository($repositoryName)->findOneBy(['id' => $campaign->getSystemAccount(), 'available' => 1]);
        $clientCustomerId = $account->getSystemAccountId();

        $kcCampaignBudget = $kcCampaignInMongo->getBudget();

        $budgetService = $this->getBudgetService();

        // Create the shared budget (required).
        if ($clientCustomerId == 0) {
            $attributes = ['error' => 'Customer Id doesn\'t exist.'];
            $where = ['backendId' => $campaign->getBackendId(), 'systemAccount' => $campaign->getSystemAccount()];
            $dm->getRepository(CampaignProcess::class)->updateManyByAttributes($dm, $where, $attributes);
        } elseif (empty($systemBudgetId = $this->checkExistenceBudget(
            $campaign->getBackendId(), $budgetName, $account->getId(), $clientCustomerId))) {

            $budgetId = $budgetService->createBudget($budgetName, $kcCampaignBudget, $clientCustomerId);
            if ($budgetId != false) {
                $this->getLogger()->info("Creating new budget: $budgetName . $this->adSystem Budget Id = $budgetId \n");
                $this->setSystemBudgetIdAllCampaigns(
                    $campaign->getBackendId(), $campaign->getSystemAccount(), $budgetId);
            } else {
                $message = "Budget $budgetName can't be created. \n";

                $attributes = ['error' => $message];
                $where = ['backendId' => $campaign->getBackendId(), 'systemAccount' => $campaign->getSystemAccount()];
                $dm->getRepository(CampaignProcess::class)->updateManyByAttributes($dm, $where, $attributes);
            }
        } else { // Update budget
            $budgetId = $budgetService->updateBudget($systemBudgetId, $kcCampaignBudget, $clientCustomerId);

            if ($budgetId != false) {
                $this->getLogger()->info("Updated budget. {$this->adSystem} Budget Id = $budgetId \n");
                $this->setSystemBudgetIdAllCampaigns(
                    $campaign->getBackendId(), $campaign->getSystemAccount(), $budgetId);
            } else {
                $message = "Budget $budgetId can't be updated. \n";

                $attributes = ['error' => $message];
                $where = ['backendId' => $campaign->getBackendId(), 'systemAccount' => $campaign->getSystemAccount()];
                $dm->getRepository(CampaignProcess::class)->updateManyByAttributes($dm, $where, $attributes);
            }
        }
    }

    /**
     * @param integer $backendId
     * @param string $budgetName
     * @param integer $accountId
     * @param integer $clientCustomerId
     * @return integer|false
     */
    public function checkExistenceBudget(int  $backendId, string  $budgetName, int $accountId, int  $clientCustomerId)
    {
        $dm = $this->getDocumentManager();

        $budgetService = $this->getBudgetService();

        if(empty($systemBudgetId = $budgetService->checkBudgetNamesInAdSystem($budgetName, $clientCustomerId))) {
            return $dm->getRepository(KcCampaign::class)->findBudgetByAccount($dm, $backendId, $accountId);
        } else {
            return $systemBudgetId;
        }
    }

    /**
     * @param int $backendId
     * @param int $systemAccount
     * @param int $budgetId
     * @throws \Doctrine\ODM\MongoDB\MongoDBException
     */
    public function setSystemBudgetIdAllCampaigns(int $backendId, int $systemAccount, int $budgetId)
    {
        $dm = $this->getDocumentManager();

        /** @var KcCampaign $kcCampaignInMongo */
        $kcCampaignInMongo = $dm->createQueryBuilder('\App\Document\KcCampaign')
            ->refresh(true)
            ->field('backendId')->equals($backendId)
            ->getQuery()->getSingleResult();

        /** @var Campaign $campaign */
        foreach ($kcCampaignInMongo->getCampaigns() as $campaign) {
            if ($campaign->getSystemAccount() == $systemAccount) {
                $campaign->setSystemBudgetId($budgetId);
            }
        }
        $dm->flush();

        $dm->createQueryBuilder('\App\Document\CampaignProcess')->updateMany()
            ->field('backendId')->equals($backendId)
            ->field('systemAccount')->equals($systemAccount)
            ->field('systemBudgetId')->set($budgetId)
            ->getQuery()
            ->execute();
    }

    /**
     * @param int $backendId
     * @param string $newKcCampaignName
     * @return bool
     * @throws \Doctrine\ODM\MongoDB\MongoDBException
     */
    public function updateBudgetName(int $backendId, string  $newKcCampaignName): bool
    {
        $dm = $this->getDocumentManager();
        $em = $this->getEntityManager();

        $newKcCampaignName = CampaignManager::getKcCampaignName($newKcCampaignName);

        $attributes = ['backendId' => $backendId];
        $selectFields = ['campaigns.systemAccount', 'campaigns.systemBudgetId', 'campaigns.systemCampaignId'];
        $kcCampaignInMongo = $dm->getRepository(KcCampaign::class)
            ->getByAttributesOne($dm, $attributes, $selectFields, true);

        if (!empty($kcCampaignInMongo)) {
            $budgetIdByAccount = [];
            foreach ($kcCampaignInMongo['campaigns'] as $campaign) {
                if ($campaign['systemCampaignId']) {
                    if ($campaign['systemAccount'] && $campaign['systemBudgetId']) {
                        $budgetIdByAccount[$campaign['systemAccount']] = $campaign['systemBudgetId'];
                    }
                } else {
                    return false;
                }
            }
            unset($kcCampaignInMongo);

            $budgetService = $this->getBudgetService();

            if (!empty($budgetIdByAccount)) {
                $selectFields = ['campaigns.systemAccount', 'campaigns.systemBudgetId', 'campaigns.systemCampaignId'];
                /**@var KcCampaign $kcCampaignInMongo */
                $kcCampaignInMongo = $dm->getRepository(KcCampaign::class)
                    ->getByAttributesOne($dm, $attributes, $selectFields);

                foreach ($budgetIdByAccount as $accountId => $currentBudgetId) {

                    $repositoryName = ProviderEntityName::getForAccountsBySystem($this->adSystem);
                    $account = $em->getRepository($repositoryName)->findOneBy(['id' => $accountId, 'available' => 1]);

                    if (empty($account)) {
                        continue;
                    }

                    $clientCustomerId = $account->getSystemAccountId();

                    $newBudgetName = self::getBudgetName($newKcCampaignName);
                    $systemBudgetId = $budgetService->checkBudgetNamesInAdSystem($newBudgetName, $clientCustomerId);

                    if (empty($systemBudgetId)) {
                        // Update budget name in ad system
                        $resultUpdate = $budgetService
                            ->updateBudgetName($currentBudgetId, $newBudgetName, $clientCustomerId);

                        if (!$resultUpdate) {
                            return false;
                        }
                    } elseif ($systemBudgetId != $currentBudgetId) {
                        //Set the found budget to all companies.
                        $systemCampaignIds = [];
                        /**@var Campaign $campaign */
                        foreach ($kcCampaignInMongo->getCampaigns() as $campaign) {
                            if ($campaign->getSystemAccount() == $accountId) {
                                $campaign->setSystemBudgetId($systemBudgetId);
                                $systemCampaignIds[] = $campaign->getSystemCampaignId();
                            }
                        }

                        $resultUpdate = $this->getCampaignManager()
                            ->updateCampaignsBudget($systemCampaignIds, $systemBudgetId, $clientCustomerId);

                        if ($resultUpdate) {
                            $dm->flush();
                        } else {
                            return false;
                        }
                    }
                }
            }
        }

        return true;
    }
}