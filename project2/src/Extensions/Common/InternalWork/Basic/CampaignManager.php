<?php

namespace App\Extensions\Common\InternalWork\Basic;

use App\Document\{Ad, Adgroup, AdgroupsQueue, AdsQueue, Campaign, CampaignProcess, DevicesBidModifier, ErrorsQueue,
    Extension, ExtensionsQueue, KcCampaign as MongoKcCampaign, Keyword, KeywordsQueue, ZipsQueue};
use Airbrake\Notifier;
use App\Extensions\AdSystem\AdWords\InternalWork\AdWordsAccountManager;
use App\Extensions\AdSystem\Bing\InternalWork\BingAccountManager;
use App\Entity\{BrandKeyword, KcCampaign, ZipcodeTest};
use App\Extensions\Common\{AdSystemEnum, ContentType};
use App\Extensions\Common\ExternalWork\CampaignInterface;
use App\Extensions\Common\InternalWork\Interfaces\CampaignManagerInterface;
use App\Extensions\Common\ServiceEnum;
use App\Interfaces\EntityInterface\SystemAccountInterface;
use App\Providers\{ProviderDocumentName, ProviderEntityName};
use App\Traits\{FlushTrait, SetterTrait};
use Doctrine\DBAL\ConnectionException;
use Doctrine\ODM\MongoDB\{DocumentManager, MongoDBException};
use Doctrine\ORM\{EntityManager, OptimisticLockException, ORMException};
use Doctrine\DBAL\DBALException;
use MongoDB\BSON\ObjectId;
use Psr\Container\ContainerInterface;
use Psr\Log\LoggerInterface;

/**
 * Class CampaignManager
 *
 * @package App\Extensions\Common\InternalWork\Basic
 */
abstract class CampaignManager implements CampaignManagerInterface
{
    use FlushTrait;
    use SetterTrait;

    /** Limit for flush */
    public const LIMIT_FOR_FLUSH = 100;

    /** */
    public const LIMIT_KEYWORDS_IN_QUEUE = 5000000;

    /**
     * @var ContainerInterface
     */
    protected ContainerInterface $container;

    /**
     * @var DocumentManager
     */
    private $dm;

    /**
     * @var EntityManager
     */
    private $em;

    /**
     * @var string
     */
    protected string $adSystem;

    /**
     * CampaignManager constructor.
     * @param string $adSystem
     * @param ContainerInterface $container
     */
    public function __construct(string $adSystem, ContainerInterface $container)
    {
        $this->adSystem = strtolower($adSystem);
        $this->container = $container;

        $dm = $this->get('doctrine_mongodb')->getManager();
        $dm->setAdSystem($this->adSystem);
        $this->dm = $dm;

        $this->em = $this->get('doctrine')->getManager();
    }

    /**
     * @param $id
     * @return mixed
     */
    protected function get($id)
    {
        return $this->container->get($id);
    }

    /**
     * @return EntityManager
     */
    protected function getEntityManager(): EntityManager
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
     * @return mixed
     */
    protected function getLogChanges()
    {
        return $this->get('change_log_manager');
    }

    /**
     * @return LoggerInterface
     */
    abstract protected function getLogger(): LoggerInterface;

    /**
     * @param $request
     * @param $method
     * @return mixed
     */
    protected function getApiData($request, $method)
    {
        return $this->get('kc.hots_api')->getApiData($request, $method);
    }

    /**
     * @return Notifier
     */
    protected function getAirbrakeNotifier(): Notifier
    {
        return $this->get('ami_airbrake.notifier');
    }

    /**
     * @param string $name
     *
     * @return string
     */
    public static function getKcCampaignName(string $name): string
    {
        $campaignName = explode(" ", $name);
        return $campaignName[0];
    }

    /**
     * @return bool
     * @throws MongoDBException|ORMException|OptimisticLockException|DBALException
     */
    public function generateCampaigns(): bool
    {
        $dm = $this->getDocumentManager();

        $kcCampaignsInMongo = $dm->createQueryBuilder(MongoKcCampaign::class)
            ->hydrate(false)
            ->select('backendId')
            ->field('statusCreateCampaigns')->exists(true)
            ->getQuery()->toArray();

        if (!empty($kcCampaignsInMongo)) {

            /** @var MongoKcCampaign $kcCampaign */
            foreach ($kcCampaignsInMongo as $kcCampaign) {
                $this->generateCampaignsByBackendId($kcCampaign['backendId']);
            }
        }

        return true;
    }

    /**
     * @param int $backendId
     *
     * @return bool
     * @throws MongoDBException|ORMException|OptimisticLockException|DBALException|\Exception
     */
    public function generateCampaignsByBackendId(int $backendId): bool
    {
        $em = $this->getEntityManager();
        $dm = $this->getDocumentManager();

        $kcCampaignInMySql = $em->getRepository(KcCampaign::class)->findOneBy(['backendId' => $backendId]);
        $brandTemplate = $kcCampaignInMySql->getBrandTemplate();

        /** @var MongoKcCampaign $kcCampaignInMongo */
        $kcCampaignInMongo = $dm->createQueryBuilder(MongoKcCampaign::class)
            ->field('backendId')->equals($backendId)
            ->field('statusCreateCampaigns')->exists(true)
            ->getQuery()->getSingleResult();

        if (empty($kcCampaignInMongo)) {
            $this->getAirbrakeNotifier()->notify(new \Exception(
                '['. strtolower(ucfirst(($this->adSystem))). ' - ERROR] Unable to find KcCampaign in mongoDB.! '
                . 'Backend Id: '. $backendId. PHP_EOL
            ));
            throw new \Exception("Unable to find KcCampaign in mongoDB.!");
        }

        $product    = $kcCampaignInMySql->getProduct();
        $brand      = $kcCampaignInMySql->getBrand();
        $client     = $kcCampaignInMySql->getClient();

        $request = [
            'requestDate'   => false,
            'productID'     => $product->getBackendId(),
            'brandID'       => $brand->getBackendId(),
            'clientID'      => $client->getBackendId(),
        ];

        $campaigns_json = $this->getApiData($request, 'HOTSPull_CampaignsZips');

        $campaigns_json = array_filter($campaigns_json, function ($item) use ($backendId) {
            return !empty($item[8]) && ((int)$item[0] == $backendId);
        });

        if (!empty($campaigns_json)) {

            $zipcodes   = array_column($campaigns_json, 11);
            $entityName = ProviderEntityName::getForZipcodesBySystem($this->adSystem);

            if (strtoupper($this->adSystem) == AdSystemEnum::ADWORDS) {
                $entityName = ZipcodeTest::getZipcodeTableEntity($kcCampaignInMySql->getTestZipcode(), $entityName);
            }

            $hotsZipcodes = $em->createQueryBuilder()
                ->select('z')
                ->from($entityName, 'z')
                ->where("z.zipcode IN (:zipcodes)")
                ->andWhere("z.deprecated = 0")
                ->getQuery()
                ->setParameter('zipcodes', $zipcodes)
                ->getArrayResult();

            $hotsCandidates = [];
            foreach ($hotsZipcodes as $hotsZipcode) {
                $hotsCandidates[$hotsZipcode['zipcode']][] = $hotsZipcode;
            }

            $em->getRepository(KcCampaign::class)->tryLock($kcCampaignInMySql);

            $brandTemplateKeywords = $em->getRepository(BrandKeyword::class)
                ->getCountKeywordsByBrandTemplate($brandTemplate->getId());

            $systemAccounts = $kcCampaignInMongo->getAccounts();

            $array_of_city = [];
            foreach ($kcCampaignInMongo->getCampaigns() as $campaign) {
                $array_of_city[] = $campaign->getCityId();
            }

            /** @var AdWordsAccountManager|BingAccountManager $accountManager */
            $accountManager = $this->get('service.manager')->getService($this->adSystem, ServiceEnum::ACCOUNT_MANAGER);

            $counterCampaigns = 0;

            foreach ($campaigns_json as $item) {
                if (isset($hotsCandidates[$item[11]])) {
                    foreach ($hotsCandidates[$item[11]] as $hotsZipcode) {
                        if (empty($hotsZipcode['locationId'])) {
                            continue;
                        }
                        if (in_array($hotsZipcode['locationId'], $array_of_city)) {
                            continue;
                        }

                        $account = $accountManager->getAvailableAccount($systemAccounts, $brandTemplateKeywords);
                        if (!$account) {
                            printf("%sNot enough free space in accounts for campaign, Backend Id: %s : City: %s",
                                date('m/d/Y h:i:s ', time()). PHP_EOL,
                                $item[0],
                                $hotsZipcode['city'] . PHP_EOL
                            );

                            continue;
                        }

                        $array_of_city[] = (int)$hotsZipcode['locationId'];
                        $document = new Campaign();
                        $document->setCityId((int)$hotsZipcode['locationId']);
                        $document->setCity($hotsZipcode['city']);
                        $document->setState($hotsZipcode['state']);
                        $document->setZipcode($item[11]);
                        $document->setCountry($hotsZipcode['country']);
                        $document->setSystemAccount($account->getId());
                        $document->setHold($kcCampaignInMongo->getHold());
                        $document->setStatus(false);

                        $kcCampaignInMongo->addCampaign($document);
                        $dm->persist($kcCampaignInMongo);
                        $dm->flush();

                        //$this->logChanges("Campaign created", "", $document->getState(). "/" . $document->getCity());
                        //Guard los elegies temperament //Really? Was a Mexican here?
                        $campaignProcess = new CampaignProcess();
                        $campaignProcess->setBackendId($item[0]);
                        $campaignProcess->setZipcode($item[11]);
                        $campaignProcess->setCity($hotsZipcode['city']);
                        $campaignProcess->setClientId($client->getId());
                        $campaignProcess->setBrandTemplateId($brandTemplate->getId());
                        $campaignProcess->setStatus($document->getStatus());
                        $campaignProcess->setSystemAccount($account->getId());
                        $campaignProcess->setCityId($hotsZipcode['locationId']);
                        $campaignProcess->setCampaignId($document->getId());
                        $campaignProcess->setState($hotsZipcode['state']);
                        $campaignProcess->setAdd(true);

                        $dm->persist($campaignProcess);
                        $dm->flush();

                        if ($this->isHaveKcCampaignBidModifier($kcCampaignInMongo)) {
                            $this->addCampaignWithBidModifier($dm, $item[0], $kcCampaignInMongo, $document->getId(), $account->getId());
                        }

                        $counterCampaigns++;
                    }
                }
            }

            //create Scheduling
            //$this->createTimeRanges($kcCampaignInMySql->getBackendId());

            $kcCampaignInMySql->updateAmountAdwCampaigns($counterCampaigns);
            $kcCampaignInMySql->setTimezone($campaigns_json[array_rand($campaigns_json)][9]);
            $kcCampaignInMySql->setSchedulesUpdate(true);
            $kcCampaignInMongo->updateAmountAdwCampaigns($counterCampaigns);
            $kcCampaignInMongo->setStatusCreateCampaigns(null);

            $dm->persist($kcCampaignInMongo);
            $dm->flush();
            $em->flush();

            $em->getRepository(KcCampaign::class)->releaseLock($kcCampaignInMySql);

            //delete campaigns which doesn't return API
            $this->cleanupCampaigns($zipcodes, $backendId);
        } else {
            $kcCampaignInMongo->setStatusCreateCampaigns(null);
            $dm->flush();
        }

        return true;
    }

    /**
     * @param $dm
     * @param $backendId
     * @param $kcCampaignInMongo
     * @param $campaignId
     * @param $accountId
     */
    private function addCampaignWithBidModifier($dm, $backendId, $kcCampaignInMongo, $campaignId, $accountId)
    {
        $campaignProcess = new CampaignProcess();
        $campaignProcess->setBackendId($backendId);
        $campaignProcess->setCampaignId($campaignId);
        $campaignProcess->setUpdate(true);
        $campaignProcess->setSystemAccount($accountId);

        $deviceBidModifier = new DevicesBidModifier();

        $bidModifier['mobile'] = !empty($kcCampaignInMongo->getDesktopBidModifier())
            ? ($kcCampaignInMongo->getDesktopBidModifier() / 100 + 1) : -1;
        $bidModifier['tablet'] = !empty($kcCampaignInMongo->getMobileBidModifier())
            ? ($kcCampaignInMongo->getMobileBidModifier() / 100 + 1) : -1;
        $bidModifier['desktop'] = !empty($kcCampaignInMongo->getTabletBidModifier())
            ? ($kcCampaignInMongo->getTabletBidModifier() / 100 + 1) : -1;

        if (isset($bidModifier['desktop'])) {
            $deviceBidModifier->setDesktopBidModifier($bidModifier['desktop']);
        }
        if (isset($bidModifier['mobile'])) {
            $deviceBidModifier->setMobileBidModifier($bidModifier['mobile']);
        }
        if (isset($bidModifier['tablet'])) {
            $deviceBidModifier->setTabletBidModifier($bidModifier['tablet']);
        }

        $campaignProcess->setDevicesBidModifier($deviceBidModifier);

        $dm->persist($campaignProcess);
        $dm->flush();
    }

    /**
     * @param $kcCampaignInMongo
     *
     * @return bool
     */
    private function isHaveKcCampaignBidModifier($kcCampaignInMongo): bool
    {
        $flag = false;

        foreach (['getDesktopBidModifier', 'getMobileBidModifier', 'getTabletBidModifier'] as $methodName) {
            if (!empty($kcCampaignInMongo->{$methodName}())) {
                $flag = true;
            }
        }

        return $flag;
    }

    /**
     * @param array $zipcodes
     * @param int $backendId
     * @throws \Exception
     */
    public function cleanupCampaigns(array $zipcodes, int $backendId)
    {
        $em = $this->getEntityManager();
        $dm = $this->getDocumentManager();

        if (empty($zipcodes)) {
            throw new \Exception("Looks like zipcodes list wasn't fetched from KC Api");
        }

        $entityName = ProviderEntityName::getForZipcodesBySystem($this->adSystem);

        if (strtoupper($this->adSystem) == AdSystemEnum::ADWORDS) {
            $kcCampaignInMySql = $em->getRepository(KcCampaign::class)->findOneBy(['backendId' => $backendId]);
            $entityName = ZipcodeTest::getZipcodeTableEntity($kcCampaignInMySql->getTestZipcode(), $entityName);
        }

        // We don't need to filter 'deprecated' location because it will remove old campaigns which are still going
        $hotsZipcodes = $em->createQueryBuilder()
            ->select('z')
            ->from($entityName, 'z')
            ->where("z.zipcode IN (:zipcodes) and z.locationId is not null")
            ->getQuery()
            ->setParameter('zipcodes', $zipcodes)
            ->getArrayResult();

        if (!empty($hotsZipcodes)) {
            $locationIds = [];
            foreach ($hotsZipcodes as $hotsZipcode) {
                $locationIds[] = $hotsZipcode['locationId'];
            }

            $kcCampaigns = $dm->createQueryBuilder(MongoKcCampaign::class)
                ->select('campaigns.systemAccount', 'campaigns.zipcode', 'campaigns.cityId',
                    'campaigns.systemCampaignId', 'campaigns.systemCampaignId', 'campaigns.id')
                ->field('backendId')->equals($backendId)
                ->getQuery()
                ->execute();

            if (!empty($kcCampaigns)) {
                foreach ($kcCampaigns as $kcCampaign) {
                    foreach ($kcCampaign->getCampaigns() as $campaign)
                        if (!in_array($campaign->getCityId(), $locationIds)) {
                            $campaignProcess = new CampaignProcess();
                            $campaignProcess->setBackendId((int)$backendId);
                            $campaignProcess->setSystemAccount($campaign->getSystemAccount());
                            $campaignProcess->setCampaignId($campaign->getId());
                            $campaignProcess->setSystemCampaignId($campaign->getSystemCampaignId());
                            $campaignProcess->setDelete(true);

                            $dm->persist($campaignProcess);
                            $dm->flush();
                        }

                    $dm->detach($kcCampaign);
                    unset($kcCampaign);
                    gc_collect_cycles();
                }
            }
        }
    }

    /**
     * @throws MongoDBException
     */
    public function uploadCampaigns()
    {
        ini_set('default_socket_timeout', 600);

        $dm = $this->getDocumentManager();

        $campaigns = $dm->createQueryBuilder(CampaignProcess::class)
            ->field('add')->exists(true)
            ->field('systemBudgetId')->exists(true)
            ->field('queuesGenerated')->exists(true)
            ->field('error')->exists(false)
            ->limit(50)->skip(0)
            ->getQuery()
            ->execute();

        foreach ($campaigns as $campaign) {
            try {
                $cmp_added = $this->uploadCampaign($campaign->getId());
                if (!$cmp_added) {
                    $message = 'campaign:' . $campaign->getBackendId() . '/' . $campaign->getState()
                        . '/' . $campaign->getCity() . ' NOT ADDED' . PHP_EOL;

                    $this->getAirbrakeNotifier()->notify(new \Exception(
                        "[" . strtolower(ucfirst(($this->adSystem))) . " - ERROR] ". $message. PHP_EOL
                    ));
                }
            } catch (\SoapFault $fault) {
                echo date('m/d/Y h:i:s ', time()). PHP_EOL. $fault->getTraceAsString();
                $message = 'campaign:' . $campaign->getBackendId() . '/' . $campaign->getState()
                    . '/' . $campaign->getCity() . ' ADD ERROR: ' . $fault->getMessage() . '' . PHP_EOL;

                $this->getAirbrakeNotifier()->notify(new \Exception(
                    '['. strtolower(ucfirst(($this->adSystem))). ' - ERROR] '. $message. PHP_EOL
                ));
            }
        }
    }

    /**
     * @param string $id
     * @throws MongoDBException
     */
    protected function uploadCampaign($id): bool
    {
        $dm = $this->getDocumentManager();
        $em = $this->getEntityManager();
        $adSystem = $this->adSystem;

        /** @var CampaignInterface $externalCampaignService */
        $externalCampaignService = $this->get('service.manager')->getService($adSystem, ServiceEnum::CAMPAIGN);

        /**@var CampaignProcess $hotsCampaign */
        $hotsCampaign = $dm->createQueryBuilder(CampaignProcess::class)
            ->field('id')->equals($id)->getQuery()->getSingleResult();

        /**@var KcCampaign $kcCampaign */
        $kcCampaign = $em->getRepository(KcCampaign::class)
            ->findOneBy(['backendId' => $hotsCampaign->getBackendId()]);

        // Get ad System Account where Campaign will be uploaded
        $repositoryName = ProviderEntityName::getForAccountsBySystem($adSystem);
        $account = $em->getRepository($repositoryName)
            ->findOneBy(['id' => $hotsCampaign->getSystemAccount(), 'available' => 1]);
        $clientCustomerId = $account->getSystemAccountId();

        if (empty($account)) {
            $this->getLogger()->error("There is no available accounts for campaign ", [
                'account' => $hotsCampaign->getSystemAccount(),
                'campaignProcess' => (string)$hotsCampaign->getId(),
                'client' => $hotsCampaign->getClientId(),
            ]);

            $this->getAirbrakeNotifier()->notify(new \Exception(
                '['. strtolower(ucfirst(($this->adSystem))). ' - ERROR] Account wasn\'t found for current '
                . 'upload campaign. Account Id: ' . $hotsCampaign->getSystemAccount() . '. Campaign Id: '
                . $hotsCampaign->getId() . '. Backend Id: ' . $hotsCampaign->getBackendId() . PHP_EOL
            ));

            return false;
        }

        $campaignName = $kcCampaign->getName() . '/' . $hotsCampaign->getState() . '/' . $hotsCampaign->getCity();

        // Get name for Campaign
        $campaignName = $this->getCampaignName(new ObjectId($hotsCampaign->getCampaignId()), $campaignName,
            $kcCampaign->getBackendId(), $clientCustomerId);

        // CREATE CAMPAIGN IN AD SYSTEM
        $systemCampaignId = $externalCampaignService->addCampaign($clientCustomerId, $campaignName, $hotsCampaign);
        // Campaign was created success in ad system
        if (is_numeric($systemCampaignId)) {
            $success = $this->setSystemCampaignNameAndIdInAllCollection(
                $hotsCampaign, $systemCampaignId, $campaignName);
            if (!$success) {
                $externalCampaignService->deleteCampaign($systemCampaignId, $clientCustomerId);
            }
        } // Campaign wasn't created in ad system
        elseif ($systemCampaignId == false) {
            $this->getAirbrakeNotifier()->notify(new \Exception(
                '['. strtolower(ucfirst(($this->adSystem))). ' - ERROR] Can\'t create campaign "'
                . $campaignName . '". Account Id: ' . $hotsCampaign->getSystemAccount()
                . '. Backend Id: ' . $hotsCampaign->getBackendId() . PHP_EOL
            ));

            return false;
        }

        return true;
    }

    /**
     * @param int $backendId
     * @param string $newKcCampaignName
     *
     * @return bool
     * @throws MongoDBException
     */
    public function updateCampaignNames(int $backendId, string $newKcCampaignName): bool
    {
        $dm = $this->getDocumentManager();
        $em = $this->getEntityManager();

        $newKcCampaignName = self::getKcCampaignName($newKcCampaignName);
        $attributes = ['backendId' => $backendId];
        $selectFields = ['backendId', 'campaigns.id', 'campaigns.name', 'campaigns.systemCampaignId',
            'campaigns.systemAccount', 'campaigns.state', 'campaigns.city'];

        /** @var MongoKcCampaign $kcCampaignInMongo */
        $kcCampaignInMongo = $dm->getRepository(MongoKcCampaign::class)
            ->getByAttributesOne($dm, $attributes, $selectFields);

        /** @var CampaignInterface $externalCampaignService */
        $externalCampaignService = $this->get('service.manager')->getService($this->adSystem, ServiceEnum::CAMPAIGN);

        $updateKcCampaignName = true;
        if (!empty($kcCampaignInMongo)) {

            /**@var Campaign $campaign */
            foreach ($kcCampaignInMongo->getCampaigns() as $campaign) {

                if ($campaign->getSystemCampaignId()) {
                    $repositoryName = ProviderEntityName::getForAccountsBySystem($this->adSystem);
                    $account = $em->getRepository($repositoryName)->find($campaign->getSystemAccount());
                    $clientCustomerId = $account->getSystemAccountId();

                    $campaignName = $newKcCampaignName . '/' . $campaign->getState() . '/' . $campaign->getCity();

                    if (strcasecmp($campaign->getName(), $campaignName) != 0) {
                        // Check the presence of number at the end
                        if (strpos($campaign->getName(), $campaignName) != false)
                            continue;

                        // Get name for Campaign
                        $campaignName = $this->getCampaignName(
                            new ObjectId($campaign->getId()), $campaignName, $backendId, $clientCustomerId);

                        // Update campaign name in system
                        $result = $externalCampaignService
                            ->updateCampaignName($campaign->getSystemCampaignId(), $campaignName, $clientCustomerId);

                        if ($result) {
                            //Save new campaign name
                            $campaign->setName($campaignName);
                        } else {
                            print date('m/d/Y h:i:s ', time()). PHP_EOL .
                                "Can't update campaign name: {$campaign->getName()} to $campaignName in $this->adSystem" .
                                "BackendId: {$kcCampaignInMongo->getBackendId()}". PHP_EOL;

                            $updateKcCampaignName = false;
                        }
                    }
                } else {
                    $updateKcCampaignName = false;
                }
            }

            $dm->flush();
            $dm->detach($kcCampaignInMongo);
            unset($kcCampaignInMongo);
            gc_collect_cycles();

        }

        return $updateKcCampaignName;
    }

    /**
     * @param array $zipcodes
     * @param int $backendId
     *
     * @return int
     */
    public function estimateCampaigns(array $zipcodes, int $backendId): int
    {
        $em = $this->getEntityManager();
        $dm = $this->getDocumentManager();

        $entityName = ProviderEntityName::getForZipcodesBySystem($this->adSystem);

        $hotsZipcodes = $em->createQueryBuilder()
            ->select('z.locationId')
            ->from($entityName, 'z', 'z.zipcode')
            ->where("z.zipcode IN (:zipcodes) and z.locationId is not null")
            ->andWhere("z.deprecated = 0")
            ->getQuery()
            ->setParameter('zipcodes', $zipcodes)
            ->getArrayResult();

//        $this->getLogger()->debug(__METHOD__ . " found zipcodes: " . count($hotsZipcodes));
        $locationByZipcodes = array_unique(array_column($hotsZipcodes, 'locationId'));

        $em->getConnection()->getConfiguration()->setSQLLogger(null);

        $builder = $dm->createAggregationBuilder(MongoKcCampaign::class);
        $existingLocation = $builder
            ->hydrate(false)
            ->match()
            ->field('backendId')->equals($backendId)
            ->unwind('$campaigns')
            ->project()
            ->excludeFields(['id'])
            ->field('cityId')->abs('$campaigns.cityId')
            ->execute()->toArray();

        $existingLocation = array_column($existingLocation, 'cityId');

        //$this->getLogger()->debug("Method estimateCampaigns peak memory usage " . (memory_get_peak_usage(true) / 1024 / 1024));
        return count(array_diff($locationByZipcodes, $existingLocation));
    }

    /**
     * @param CampaignProcess   $hotsCampaign
     * @param int               $systemCampaignId
     * @param string            $campaignName
     *
     * @return bool
     * @throws MongoDBException
     */
    public function setSystemCampaignNameAndIdInAllCollection($hotsCampaign, $systemCampaignId, $campaignName = ''): bool
    {
        $dm = $this->getDocumentManager();

        // Save google campaign ID and NAME after all settings are uploaded to AdWords
        $kcCampaignInMongo = $dm->createQueryBuilder(MongoKcCampaign::class)
            ->refresh()
            ->select('campaigns.id', 'campaigns.name')
            ->field('backendId')->equals($hotsCampaign->getBackendId())
            ->getQuery()->getSingleResult();

        foreach ($kcCampaignInMongo->getCampaigns() as $campaign) {
            if ($campaign->getId() == $hotsCampaign->getCampaignId()) {
                $campaign->setSystemCampaignId($systemCampaignId);
                if (!empty($campaignName)) {
                    $campaign->setName($campaignName);
                }
                $dm->flush();

                $dm->createQueryBuilder(CampaignProcess::class)->updateMany()
                    ->field('campaignId')->equals(new ObjectId($hotsCampaign->getCampaignId()))
                    ->field('systemCampaignId')->set($systemCampaignId)
                    ->getQuery()
                    ->execute();
                $dm->createQueryBuilder(AdgroupsQueue::class)->updateMany()
                    ->field('campaignId')->equals(new ObjectId($hotsCampaign->getCampaignId()))
                    ->field('systemCampaignId')->set($systemCampaignId)
                    ->getQuery()
                    ->execute();
                $dm->createQueryBuilder(KeywordsQueue::class)->updateMany()
                    ->field('campaignId')->equals(new ObjectId($hotsCampaign->getCampaignId()))
                    ->field('systemCampaignId')->set($systemCampaignId)
                    ->getQuery()
                    ->execute();
                $dm->createQueryBuilder(AdsQueue::class)->updateMany()
                    ->field('campaignId')->equals(new ObjectId($hotsCampaign->getCampaignId()))
                    ->field('systemCampaignId')->set($systemCampaignId)
                    ->getQuery()
                    ->execute();
                $dm->createQueryBuilder(ExtensionsQueue::class)->updateMany()
                    ->field('campaignId')->equals(new ObjectId($hotsCampaign->getCampaignId()))
                    ->field('systemCampaignId')->set($systemCampaignId)
                    ->getQuery()
                    ->execute();
                $dm->createQueryBuilder(CampaignProcess::class)->remove()
                    ->field('id')->equals($hotsCampaign->getId())
                    ->getQuery()
                    ->execute();

                return true;
            }
        }

        return false;
    }

    /**
     * @param int $backendId
     *
     * @return bool
     * @throws ConnectionException|MongoDBException
     */
    public function addKcCampaignInQueueForDelete(int $backendId): bool
    {
        $em = $this->getEntityManager();
        $dm = $this->getDocumentManager();

        $selectFields = ['statusCreateCampaigns', 'campaigns.id', 'campaigns.name',
            'campaigns.systemCampaignId', 'campaigns.systemAccount'];
        $kcCampaignInMongo = $dm->getRepository(MongoKcCampaign::class)
            ->getByAttributesOne($dm, ['backendId' => $backendId], $selectFields, true);

        if (isset($kcCampaignInMongo['statusCreateCampaigns']))
            return false;

        if (isset($kcCampaignInMongo['campaigns'])) {
            $kcCampaign = $em->getRepository(KcCampaign::class)->findOneBy(['backendId' => $backendId]);
            $dm->getRepository(MongoKcCampaign::class)
                ->addKcCampaignToQueueForDeleting($kcCampaignInMongo, $backendId, $dm);

            $retry = 0;
            $done = false;
            while (!$done and $retry < 3) {
                $em->getConnection()->beginTransaction();
                try {
                    $this->getLogChanges()->changeLog("Delete", "KcCampaign", null, null, null, $kcCampaign);
                    $em->getConnection()->commit(); // commit if successfully $done = true;
                    $done = true;
                } catch (\Exception $e) {
                    echo $e->getMessage() . PHP_EOL;
                    $retry++;
                    $em->getConnection()->rollBack();
                }
            }
        }

        return true;
    }

    /**
     * Delete campaigns form ad system and local database
     *
     * @throws MongoDBException|\Exception
     */
    public function deleteCampaigns()
    {
        $em = $this->getEntityManager();
        $dm = $this->getDocumentManager();
        $dm->setAdSystem($this->adSystem);

        $campaignsProcess = $dm->createQueryBuilder(CampaignProcess::class)
            ->field('delete')->exists(true)
            ->field('campaignId')->exists(true)
            ->field('systemAccount')->exists(true)
            ->field('error')->exists(false)
            ->limit(100)
            ->getQuery()
            ->execute();

        if (!empty($campaignsProcess)) {
            $serviceManager = $this->get('service.manager');

            $backendIds = [];
            /** @var CampaignProcess $campaign */
            foreach ($campaignsProcess as $campaign) {
                $backendIds[]       = $campaign->getBackendId();
                $systemCampaignId   = $campaign->getSystemCampaignId();

                // Delete campaign from Ad System
                if (!empty($systemCampaignId)) {
                    /** @var CampaignInterface $externalCampaignService */
                    $externalCampaignService = $serviceManager->getService($this->adSystem, ServiceEnum::CAMPAIGN);

                    $repositoryName = ProviderEntityName::getForAccountsBySystem($this->adSystem);

                    /** @var SystemAccountInterface $account */
                    $account = $em->getRepository($repositoryName)->find($campaign->getSystemAccount());

                    if ($account) {
                        if ($externalCampaignService->deleteCampaign($systemCampaignId, $account->getSystemAccountId())) {
                            //Log result if it successful
                            $this->getLogger()->info("Delete campaign in {$this->adSystem}. Backend Id = {$campaign->getBackendId()}, Campaign Id = $systemCampaignId");

                            // Clean up in mongo Kc Campaign
                            $this->cleanUpCampaignFromDatabase($campaign);
                        }
                    } else {
                        printf("Campaign backend ID: '%s' can not be removed in {$this->adSystem}! Account didn't find. \n",
                            $campaign->getBackendId());

                        $where['systemAccount'] = $campaign->getSystemAccount();
                        $attributes['error']    = 'Customer not found !';

                        $dm->getRepository(CampaignProcess::class)
                            ->updateManyByAttributes($dm, $where, $attributes);

                        foreach (ContentType::CONTENT_TYPES as $CONTENT_TYPE) {
                            $documentName = ProviderDocumentName::getQueueByContentType($CONTENT_TYPE);
                            $dm->getRepository($documentName)->updateManyByAttributes($dm, $where, $attributes);
                        }

                        break;
                    }
                }
            }

            // Clean up Kc Campaign
            $this->cleanUpKcCampaignsFromDatabase(array_unique($backendIds));
        }
    }

    /**
     * @param array int $backendIds
     *
     * @throws MongoDBException
     */
    public function cleanUpKcCampaignsFromDatabase(array $backendIds)
    {
        $em = $this->getEntityManager();
        $dm = $this->getDocumentManager();

        foreach ($backendIds as $backendId) {
            $countCampaigns = self::getTotalCountCampaignsFromAllAdSystems($backendId, $dm);

            if ($countCampaigns == 0) {

                $kcCampaignInMySql = $em->getRepository(KcCampaign::class)->findOneBy(['backendId' => $backendId]);
                $brandTemplateId = $kcCampaignInMySql->getBrandTemplate()->getId();

                $em->createQuery("DELETE App:KcCampaign ca WHERE ca.backendId = :id")
                    ->setParameter('id', $backendId)->execute();

                $em->createQuery("DELETE App:TimeRange tr WHERE tr.campaignBackendId = :id")
                    ->setParameter('id', $backendId)->execute();

                foreach (AdSystemEnum::AD_SYSTEMS as $adSystem) {
                    $dm->setAdSystem($adSystem);

                    // Clean up all queues by Kc Campaign
                    self::cleanUpAllQueuesByBackendId($backendId, $dm);

                    // Clean up all content by Kc Campaign
                    self::cleanUpAllContentByBackendId($backendId, $dm, $brandTemplateId);

                    // Remove kc campaign from main collection
                    $dm->createQueryBuilder(MongoKcCampaign::class)->remove()
                        ->field('backendId')->equals($backendId)
                        ->getQuery()->execute();
                }

            } else {
                self::deleteOrSetAmountForKcCampaign($dm, $backendId);
            }
        }
    }

    /**
     * @param DocumentManager   $dm
     * @param int               $backendId
     *
     * @throws MongoDBException
     */
    public static function deleteOrSetAmountForKcCampaign(DocumentManager $dm, int $backendId)
    {
        /** @var MongoKcCampaign $kcCampaignInMongo */
        $kcCampaignInMongo = $dm->createQueryBuilder(MongoKcCampaign::class)
            ->refresh()
            ->select('backendId', 'campaigns.id')
            ->field('backendId')->equals($backendId)
            ->getQuery()->getSingleResult();

        $countCampaigns = count($kcCampaignInMongo->getCampaigns());

        if ($countCampaigns != 0) {
            $kcCampaignInMongo->setAmountAdwCampaigns($countCampaigns);
        } else {
            $dm->remove($kcCampaignInMongo);
        }

        $dm->flush();
    }

    /**
     * @param CampaignProcess $campaignProcess
     *
     * @throws MongoDBException
     */
    public function cleanUpCampaignFromDatabase(CampaignProcess $campaignProcess)
    {
        $dm = $this->getDocumentManager();
        $em = $this->getEntityManager();

        $kcCampaignInMySql = $em->getRepository(KcCampaign::class)
            ->findOneBy(['backendId' => $campaignProcess->getBackendId()]);
        $brandTemplateId = $kcCampaignInMySql->getBrandTemplate()->getId();
        $dm->setBrandTemplateId($brandTemplateId);

        $repositoryName = ProviderEntityName::getForAccountsBySystem($this->adSystem);
        /** @var SystemAccountInterface $account */
        $account = $em->getRepository($repositoryName)->find($campaignProcess->getSystemAccount());

        if ($account) {
            $kcCampaignInMongo = $dm->createQueryBuilder(MongoKcCampaign::class)
                ->hydrate(false)
                ->select('statusCreateCampaigns')
                ->field('backendId')->equals($campaignProcess->getBackendId())
//                ->field('campaigns.id')->equals(new ObjectId($campaignProcess->getCampaignId()))
                ->getQuery()->getSingleResult();

            if (!isset($kcCampaignInMongo['statusCreateCampaigns'])) {
                // Clean up all queues by campaign
                self::cleanUpAllQueuesByCampaignId($campaignProcess->getCampaignId(), $dm);

                $countCampaigns = $dm->createQueryBuilder(CampaignProcess::class)
                    ->count()
                    ->field('campaignId')->equals(new ObjectId($campaignProcess->getCampaignId()))
                    ->field('add')->exists(true)
                    ->field('error')->exists(false)
                    ->getQuery()->execute();

                // Clean up campaign and related content from mongo
                if ($countCampaigns == 0) {
                    /** @var MongoKcCampaign $kcCampaignInMongo */
                    $kcCampaignInMongo = $dm->createQueryBuilder(MongoKcCampaign::class)
                        ->select('backendId', 'campaigns.id', 'campaigns.systemCampaignId', 'campaigns.zipcode')
                        ->field('backendId')->equals($campaignProcess->getBackendId())
                        ->getQuery()->getSingleResult();

                    /** @var Campaign $campaign */
                    foreach ($kcCampaignInMongo->getCampaigns() as $campaign) {
                        if ((string)$campaign->getId() == (string)$campaignProcess->getCampaignId()) {
                            $dm->createQueryBuilder(ZipsQueue::class)->remove()
                                ->field('backendId')->equals($campaignProcess->getBackendId())
                                ->field('zipcode')->equals($campaign->getZipcode())
                                ->getQuery()->execute();

                            $kcCampaignInMongo->removeCampaign($campaign);
                            $dm->flush();

                            if (!empty($campaign->getSystemCampaignId())) {
                                // Clean up all content by campaign
                                self::cleanUpAllContentBySystemCampaignId(
                                    $campaign->getSystemCampaignId(), $dm, $brandTemplateId
                                );
                            }

                            break;
                        }
                    }

                    $dm->createQueryBuilder(CampaignProcess::class)->remove()
                        ->field('campaignId')->equals(new ObjectId($campaignProcess->getCampaignId()))
                        //->field('add')->exists(true)
                        ->getQuery()->execute();

                    unset($kcCampaignInMongo);
                }
            }

            $dm->flush();
        } else {
            $this->getAirbrakeNotifier()->notify(new \Exception(
                '['. strtolower(ucfirst(($this->adSystem))). ' - ERROR] Campaign "'
                . $campaignProcess->getCampaignId() . '" can not be removed! Account didn\'t find "'
                . $campaignProcess->getSystemAccount() . '". Backend Id: ' . $campaignProcess->getBackendId() . PHP_EOL
            ));
        }
    }

    /**
     * @param int $backendId
     * @param DocumentManager $dm
     * @return int
     */
    public static function getTotalCountCampaignsFromAllAdSystems(int $backendId, DocumentManager $dm): int
    {
        $count = 0;
        foreach (AdSystemEnum::AD_SYSTEMS as $adSystem) {
            $dm->setAdSystem($adSystem);

            $builder = $dm->createAggregationBuilder(MongoKcCampaign::class);
            $countCampaigns = $builder
                ->hydrate(false)
                ->match()
                ->field('backendId')->equals($backendId)
                ->unwind('$campaigns')
                ->count('count')
                ->execute()->toArray();

            $count += $countCampaigns[0]['count'] ?? 0;
        }

        return $count;
    }

    /**
     * @param string $campaignId
     * @param DocumentManager $dm
     * @throws MongoDBException
     */
    public static function cleanUpAllQueuesByCampaignId(string $campaignId, DocumentManager $dm)
    {
        // Except ZipsQueue. You can not remove items from this queue by campaignId
        $dm->createQueryBuilder(ExtensionsQueue::class)->remove()
            ->field('campaignId')->equals(new ObjectId($campaignId))
            ->getQuery()->execute();
        $dm->createQueryBuilder(AdgroupsQueue::class)->remove()
            ->field('campaignId')->equals(new ObjectId($campaignId))
            ->getQuery()->execute();
        $dm->createQueryBuilder(AdsQueue::class)->remove()
            ->field('campaignId')->equals(new ObjectId($campaignId))
            ->getQuery()->execute();
        $dm->createQueryBuilder(KeywordsQueue::class)->remove()
            ->field('campaignId')->equals(new ObjectId($campaignId))
            ->getQuery()->execute();
        $dm->createQueryBuilder(ErrorsQueue::class)->remove()
            ->field('campaignId')->equals(new ObjectId($campaignId))
            ->getQuery()->execute();
    }

    /**
     * @param int               $systemCampaignId
     * @param DocumentManager   $dm
     * @param int               $brandTemplateId
     *
     * @throws MongoDBException
     */
    public static function cleanUpAllContentBySystemCampaignId(int $systemCampaignId, DocumentManager $dm, int $brandTemplateId)
    {
        $dm->setBrandTemplateId($brandTemplateId);

        $dm->createQueryBuilder(Extension::class)->remove()
            ->field('systemCampaignId')->equals($systemCampaignId)
            ->getQuery()->execute();
        $dm->createQueryBuilder(Adgroup::class)->remove()
            ->field('systemCampaignId')->equals($systemCampaignId)
            ->getQuery()->execute();
        $dm->createQueryBuilder(Ad::class)->remove()
            ->field('systemCampaignId')->equals($systemCampaignId)
            ->getQuery()->execute();
        $dm->createQueryBuilder(Keyword::class)->remove()
            ->field('systemCampaignId')->equals($systemCampaignId)
            ->getQuery()->execute();
    }


    /**
     * @param int               $backendId
     * @param DocumentManager   $dm
     *
     * @throws MongoDBException
     */
    public static function cleanUpAllQueuesByBackendId(int $backendId, DocumentManager $dm)
    {
        $dm->createQueryBuilder(ExtensionsQueue::class)->remove()
            ->field('kcCampaignBackendId')->equals($backendId)
            ->getQuery()->execute();
        $dm->createQueryBuilder(AdgroupsQueue::class)->remove()
            ->field('kcCampaignBackendId')->equals($backendId)
            ->getQuery()->execute();
        $dm->createQueryBuilder(AdsQueue::class)->remove()
            ->field('kcCampaignBackendId')->equals($backendId)
            ->getQuery()->execute();
        $dm->createQueryBuilder(KeywordsQueue::class)->remove()
            ->field('kcCampaignBackendId')->equals($backendId)
            ->getQuery()->execute();
        $dm->createQueryBuilder(ErrorsQueue::class)->remove()
            ->field('backendId')->equals($backendId)
            ->getQuery()->execute();

        $dm->createQueryBuilder(ZipsQueue::class)->remove()
            ->field('backendId')->equals($backendId)
            ->getQuery()->execute();
    }

    /**
     * @param int               $backendId
     * @param DocumentManager   $dm
     * @param int               $brandTemplateId
     *
     * @throws MongoDBException
     */
    public static function cleanUpAllContentByBackendId(int $backendId, DocumentManager $dm, int $brandTemplateId)
    {
        $dm->setBrandTemplateId($brandTemplateId);

        $dm->createQueryBuilder(Extension::class)->remove()
            ->field('kcCampaignBackendId')->equals($backendId)
            ->getQuery()->execute();
        $dm->createQueryBuilder(Adgroup::class)->remove()
            ->field('kcCampaignBackendId')->equals($backendId)
            ->getQuery()->execute();
        $dm->createQueryBuilder(Ad::class)->remove()
            ->field('kcCampaignBackendId')->equals($backendId)
            ->getQuery()->execute();
        $dm->createQueryBuilder(Keyword::class)->remove()
            ->field('kcCampaignBackendId')->equals($backendId)
            ->getQuery()->execute();
    }

    /**
     * @param int               $backendId
     * @param string            $adSystem
     * @param DocumentManager   $dm
     *
     * @return bool
     * @throws MongoDBException
     */
    public static function getCommonKcCampaignStatus(int $backendId, string $adSystem, DocumentManager $dm): bool
    {
        $adSystems = AdSystemEnum::AD_SYSTEMS;
        unset($adSystems[array_search(strtoupper($adSystem), $adSystems)]);

        $generalKcCampaignStatus = false;
        foreach ($adSystems as $adSystem) {
            $dm->setAdSystem($adSystem);

            $activated = $dm->createQueryBuilder(MongoKcCampaign::class)
                ->count()
                ->field('backendId')->equals($backendId)
                ->field('statusHots')->equals(true)
                ->getQuery()->execute();

            if ($activated) {
                $generalKcCampaignStatus = true;

                break;
            }
        }

        return $generalKcCampaignStatus;
    }

    /**
     * @param int               $backendId
     * @param string            $adSystem
     * @param DocumentManager   $dm
     * @return bool
     */
    public static function getCommonKcCampaignHoldStatus(int $backendId, string $adSystem, DocumentManager $dm): bool
    {
        $adSystems = AdSystemEnum::AD_SYSTEMS;
        unset($adSystems[array_search(strtoupper($adSystem), $adSystems)]);

        $generalKcCampaignStatus = true;
        foreach ($adSystems as $adSystem) {
            $dm->setAdSystem($adSystem);

            $attributes = ['backendId' => $backendId, 'hold' => true];
            $onHold = $dm->getRepository(MongoKcCampaign::class)
                ->getCountByAttributes($dm, $attributes);

            if (!$onHold) {
                $generalKcCampaignStatus = false;

                break;
            }
        }

        return $generalKcCampaignStatus;
    }

    /**
     * @param int $kcCampaignBackendId
     *
     * @return int
     * @throws MongoDBException|\Exception
     */
    public function countItemsInQueuesByKcCampaign(int $kcCampaignBackendId): int
    {
        $dm = $this->getDocumentManager();

        $count = 0;
        foreach (ContentType::CONTENT_TYPES as $CONTENT_TYPE) {
            $documentName = ProviderDocumentName::getQueueByContentType($CONTENT_TYPE);
            $count += $dm->createQueryBuilder($documentName)
                ->count()
                ->field('kcCampaignBackendId')->equals($kcCampaignBackendId)
                ->field('error')->exists(false)
                ->getQuery()->execute();
        }

        return $count;
    }

    /**
     * Update uploading status
     * @throws MongoDBException
     * @throws \Exception
     */
    public function checkAndUpdateCampaignsStatus()
    {
        $dm = $this->getDocumentManager();

        $backendIds = $dm->getRepository(MongoKcCampaign::class)->getBackendIdsByQueues($dm);

        // Reset statistic to 0 for other kc campaigns (which are not in queues at the moment)
        $where = ['backendId' => $backendIds];
        $attributes = ['statusWaitSync' => null, 'amountItemsInQueue' => 0, 'amountItemsForUpload' => 0];
        $dm->getRepository('App\Document\KcCampaign')->updateManyByAttributesNotIn($dm, $where, $attributes);

        if (!empty($backendIds)) {
            $attributes = ['backendId' => $backendIds];
            $selectFields = ['backendId', 'statusWaitSync', 'amountItemsInQueue'];
            /**@var \App\Document\KcCampaign[] $kcCampaigns */
            $kcCampaigns = $dm->getRepository('App\Document\KcCampaign')
                ->getByAttributesIn($dm, $attributes, $selectFields);

            foreach ($kcCampaigns as $kcCampaign) {
                $count = $this->countItemsInQueuesByKcCampaign($kcCampaign->getBackendId());
                $kcCampaign->setAmountItemsInQueue($count);

                if ($count == 0) {
                    $kcCampaign->setStatusWaitSync(null);
                }
            }

            $dm->flush();
        }
    }

    /**
     * @param                   $backendId
     * @param                   $adSystem
     * @param DocumentManager   $dm
     *
     * @return array
     * @throws \Exception
     */
    public function getCampaignIdsInProcessByBackendId($backendId, $adSystem, DocumentManager $dm): array
    {
        $dm->setAdSystem($adSystem);

        $builder = $dm->createAggregationBuilder(CampaignProcess::class);
        $campaignProcess = $builder->hydrate(false)
            ->match()
            ->field('error')->exists(false)
            ->field('backendId')->equals($backendId)
            ->group()
            ->field('id')->expression(
                $builder->expr()
                    ->field('campaignId')->expression('$campaignId')
                    ->field('add')->expression('$add')
                    ->field('delete')->expression('$delete')
            )
            ->getAggregation()->getIterator()->toArray();

        return array_column($campaignProcess, '_id');
    }

    /**
     * @param                   $backendId
     * @param                   $adSystem
     * @param DocumentManager   $dm
     *
     * @return array
     * @throws \Exception
     */
    public function getCampaignIdsInQueuesByBackendId($backendId, $adSystem, DocumentManager $dm): array
    {
        $dm->setAdSystem($adSystem);

        $queueIds = [];
        foreach ([ContentType::AD, ContentType::KEYWORD, ContentType::EXTENSION] as $contentType) {
            $documentName = ProviderDocumentName::getQueueByContentType($contentType);
            $queue = $dm->createAggregationBuilder($documentName)
                ->hydrate(false)
                ->match()
                ->field('add')->exists(true)
                ->field('error')->exists(false)
                ->field('kcCampaignBackendId')->equals($backendId)
                ->group()
                ->field('id')->expression('$campaignId')
                ->getAggregation()->getIterator()->toArray();

            foreach (array_column($queue, '_id') as $id) {
                $queueIds[] = (string)new ObjectId($id);
            }
        }

        return array_flip(array_unique($queueIds));
    }
}
