<?php

namespace App\Extensions\AdSystem\AdWords\InternalWork;

use App\Document\CampaignProcess;
use App\Document\ErrorsQueue;
use App\Extensions\AdSystem\AdWords\ExternalWork\AdWordsErrorDetail;
use App\Extensions\Common\AdSystemEnum;
use App\Extensions\Common\InternalWork\Basic\CampaignManager;
use App\Extensions\Common\ServiceEnum;
use MongoDB\BSON\ObjectId;
use Psr\Container\ContainerInterface;
use Psr\Log\LoggerInterface;

/**
 * Class AdWordsCampaignManager
 * @package App\Extensions\AdSystem\AdWords\InternalWork
 */
class AdWordsCampaignManager extends CampaignManager
{
    /**
     * @var LoggerInterface
     */
    private LoggerInterface $logger;

    /** */
    public const KEYWORDS_LIMIT = 3500000;

    /**
     * AdWordsCampaignManager constructor.
     *
     * @param ContainerInterface    $container
     * @param LoggerInterface       $adwordsLogger
     */
    public function __construct(ContainerInterface $container, LoggerInterface $adwordsLogger)
    {
        parent::__construct(AdSystemEnum::ADWORDS, $container);

        $this->container    = $container;
        $this->logger       = $adwordsLogger;
    }

    /**
     * @return mixed
     */
    protected function getLogger(): LoggerInterface
    {
        return $this->logger;
    }

    /**
     * @param ObjectId  $campaignId
     * @param string    $campaignName
     * @param int       $kcCampaignBackendId
     * @param int       $clientCustomerId
     *
     * @return string
     * @throws \Doctrine\ODM\MongoDB\MongoDBException
     */
    public function getCampaignName(
        ObjectId    $campaignId,
        string      $campaignName,
        int         $kcCampaignBackendId,
        int         $clientCustomerId
    ): string {
        $dm = $this->getDocumentManager();

        $findCampaignName = $campaignName;

        // Check Campaign in local database
        $counter = 1;
        $campaignExists = true;
        while ($campaignExists)
        {
            $builder = $dm->createQueryBuilder('\App\Document\KcCampaign');
            $existsCampaignInMongo = (boolean)$builder
                ->count()
                ->field('backendId')->equals($kcCampaignBackendId)
                ->field('campaigns')->elemMatch(
                    $builder->expr()->field('name')->equals($findCampaignName)
                        ->field('id')->notEqual(new ObjectId($campaignId))
                )
                ->getQuery()->execute();


            if($existsCampaignInMongo){
                $counter++;
                $findCampaignName = $campaignName."-".(string)$counter;
            } else {
                $campaignExists = false;
            }
        }

        $campaignManager = $this->get('service.manager')->getService(AdSystemEnum::ADWORDS, ServiceEnum::CAMPAIGN);

        while($campaignManager->findCampaignInAdSystemByName($clientCustomerId, $findCampaignName)){
            $counter++;
            $findCampaignName = $campaignName."-".(string)$counter;
        }

        return $findCampaignName;
    }


    /**
     * @param CampaignProcess   $hotsCampaign
     * @param array             $errors
     * @param string            $campaignName
     *
     * @throws \Doctrine\ODM\MongoDB\MongoDBException
     */
    public function registerCampaignUploadingErrors(CampaignProcess $hotsCampaign , array $errors, string $campaignName)
    {
        $dm = $this->getDocumentManager();

        foreach ($errors as $campaign => $error_message){
            $exemptionCampaign = new ErrorsQueue();
            $exemptionCampaign->setType("campaign");
            $exemptionCampaign->setCampaignName($campaignName);
            $exemptionCampaign->setCampaignId($hotsCampaign->getId());
            $exemptionCampaign->setBackendId($hotsCampaign->getBackendId());
            $exemptionCampaign->setError(AdWordsErrorDetail::errorDetail($error_message));
            $exemptionCampaign->setRawError($error_message);

            $dm->persist($exemptionCampaign);
            $dm->flush();

            $dm->createQueryBuilder('\App\Document\CampaignProcess')->findAndUpdate()
                ->field('id')->equals($hotsCampaign->getId())
                ->field('error')->set($error_message)
                ->getQuery()
                ->execute();
        }
    }
}
