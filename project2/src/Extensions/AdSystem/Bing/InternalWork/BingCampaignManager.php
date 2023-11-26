<?php

namespace App\Extensions\AdSystem\Bing\InternalWork;

use App\Document\CampaignProcess;
use App\Document\ErrorsQueue;
use App\Extensions\Common\AdSystemEnum;
use App\Extensions\Common\InternalWork\Basic\CampaignManager;
use MongoDB\BSON\ObjectId;
use Psr\Container\ContainerInterface;
use Psr\Log\LoggerInterface;

/**
 * Class BingCampaignManager
 * @package App\Extensions\AdSystem\Bing\InternalWork
 */
class BingCampaignManager extends CampaignManager
{
    /**
     * @var LoggerInterface
     */
    private $logger;

    /**
     * BingCampaignManager constructor.
     * @param ContainerInterface $container
     * @param LoggerInterface $bingLogger
     */
    public function __construct(ContainerInterface $container, LoggerInterface $bingLogger)
    {
        parent::__construct(AdSystemEnum::BING, $container);

        $this->container = $container;
        $this->logger = $bingLogger;
    }

    /**
     * @return LoggerInterface
     */
    protected function getLogger(): LoggerInterface
    {
        return $this->logger;
    }

    /**
     * @param ObjectId $campaignId
     * @param string $campaignName
     * @param int $kcCampaignBackendId
     * @param int $clientCustomerId
     *
     * @return string $campaignName
     * @throws
     */
    public function getCampaignName(
        ObjectId $campaignId,
        string $campaignName,
        int $kcCampaignBackendId,
        int $clientCustomerId
    ): string {
        $dm = $this->getDocumentManager();

        $findCampaignName = $campaignName;

        // Check Campaign in local database
        $counter = 1;
        $campaignExists = true;
        while ($campaignExists) {
            $builder = $dm->createQueryBuilder('\App\Document\KcCampaign');
            $existsCampaignInMongo = (boolean)$builder
                ->count()
                ->field('backendId')->equals($kcCampaignBackendId)
                ->field('campaigns')->elemMatch(
                    $builder->expr()->field('name')->equals($findCampaignName)
                        ->field('id')->notEqual(new ObjectId($campaignId))
                )
                ->getQuery()->execute();

            if ($existsCampaignInMongo) {
                $counter++;
                $findCampaignName = $campaignName . "-" . (string)$counter;
            } else {
                $campaignExists = false;
            }
        }

        /*
         * Maybe when Bing will make a norm library and it will be able to get a campaign by name.
         * $campaignManager = $this->get('service.manager')->getService(AdSystem::BING, Service::CAMPAIGN);

        while($campaignManager->findCampaignInAdSystem($clientCustomerId, $campaignName)){
            $counter++;
            $campaignName = $campaignName."-".(string)$counter;
        }*/

        return $findCampaignName;
    }

    /**
     * @param CampaignProcess $hotsCampaign
     * @param array $errors
     * @param string $campaignName
     * @throws \Doctrine\ODM\MongoDB\MongoDBException
     */
    public function registerCampaignUploadingErrors(CampaignProcess $hotsCampaign, array $errors, string $campaignName)
    {
        $dm = $this->getDocumentManager();

        foreach ($errors as $error) {
            $exemptionCampaign = new ErrorsQueue;
            $exemptionCampaign->setType("campaign");
            $exemptionCampaign->setCampaignName($campaignName);
            $exemptionCampaign->setCampaignId($hotsCampaign->getId());
            $exemptionCampaign->setBackendId($hotsCampaign->getBackendId());
            $exemptionCampaign->setError($error->Message);
            $exemptionCampaign->setRawError($error->Message);

            $dm->persist($exemptionCampaign);
            $dm->flush();

            $dm->createQueryBuilder('\App\Document\CampaignProcess')->updateOne()
                ->field('id')->equals($hotsCampaign->getId())
                ->field('error')->set($error->Message)
                ->getQuery()
                ->execute();
        }
    }
}