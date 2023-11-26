<?php

namespace App\Extensions\Common\ExternalWork\Core;


use Airbrake\Notifier;
use App\Document\CampaignProcess;
use App\Document\ErrorsQueue;
use App\Document\KcCampaign;
use App\Document\KcCampaign as MongoKcCampaign;
use App\Enums\ErrorLevel;
use App\Extensions\Common\ExternalWork\CampaignInterface;
use App\Extensions\Common\InternalWork\Basic\CampaignManager;
use App\Providers\ProviderEntityName;
use App\Services\SlackSenderService;
use Doctrine\ODM\MongoDB\DocumentManager;
use Doctrine\ODM\MongoDB\MongoDBException;
use Doctrine\ORM\EntityManager;
use Doctrine\ORM\EntityManagerInterface;
use Exception;
use MongoDB\BSON\ObjectId;
use Psr\Container\ContainerExceptionInterface;
use Psr\Container\ContainerInterface;
use Psr\Container\NotFoundExceptionInterface;


/**
 *
 */
abstract class CampaignAbstract implements CampaignInterface
{
    /**
     * @var ContainerInterface
     */
    protected ContainerInterface $container;

    /** @var DocumentManager */
    private DocumentManager $dm;

    /**
     * @param ContainerInterface $container
     */
    public function __construct(ContainerInterface $container)
    {
        $this->container = $container;
    }

    /**
     * @param $id
     * @return mixed
     * @throws ContainerExceptionInterface|NotFoundExceptionInterface
     */
    protected function get($id)
    {
        return $this->container->get($id);
    }

    /**
     * @return string
     */
    abstract protected function getAdSystem(): string;

    /**
     * @return DocumentManager
     * @throws ContainerExceptionInterface|NotFoundExceptionInterface
     */
    protected function getDocumentManager(): DocumentManager
    {
        if (!isset($this->dm)) {
            /** @var DocumentManager $dm */
            $dm = $this->get('doctrine_mongodb');
            $dm->setAdSystem($this->getAdSystem());
            $this->dm = $dm;
        }

        return $this->dm;
    }

    /**
     * @return EntityManagerInterface
     * @throws ContainerExceptionInterface|NotFoundExceptionInterface
     */
    protected function getEntityManager(): EntityManagerInterface
    {
        return $this->container->get('doctrine');
    }

    /**
     * @return SlackSenderService
     * @throws ContainerExceptionInterface|NotFoundExceptionInterface
     */
    protected function getSlackSenderService(): SlackSenderService
    {
        return $this->container->get('kc.slack_sender_service');
    }

    /**
     * @return Notifier
     * @throws ContainerExceptionInterface|NotFoundExceptionInterface
     */
    protected function getAirbrakeNotifier(): Notifier
    {
        return $this->get('ami_airbrake.notifier');
    }

    /**
     * @param string    $errorMessage
     * @param string    $errorDetails
     * @param           $clientCustomerId
     * @param           $systemCampaignId
     * @param array     $campaignDetails
     *
     * @throws MongoDBException|Exception|ContainerExceptionInterface|NotFoundExceptionInterface
     */
    public function registerCampaignErrors(
        string  $errorMessage,
        string  $errorDetails,
                $clientCustomerId,
                $systemCampaignId,
        array   $campaignDetails
    ) {
        $dm = $this->getDocumentManager();

        if ($this->holdCampaign($clientCustomerId, $systemCampaignId,  $campaignDetails)){
            $errorDetails = $errorDetails . " Current campaign was set on hold.";
        } else {
            $errorDetails = $errorDetails . " The campaign WASN'T set on hold.";
            $slackSenderService = $this->getSlackSenderService();
            $slackSenderService->sendCustomizedMessagesToSlack(
                "[ERROR]: $errorDetails. System Campaign Id: $systemCampaignId", 'webhook2');
        }

        $errorInfo = [
            'type' => ErrorLevel::CAMPAIGN,
            'backendId' => $campaignDetails['backendId'],
            'campaignId' => $campaignDetails['id'],
            'campaignName' => $campaignDetails['name'],
            'systemItemId' => $systemCampaignId,
            'error' => $errorDetails,
            'rawError' => $errorMessage,
        ];

        $errorQueue = new ErrorsQueue();
        $errorQueue->fill($errorInfo);
        $dm->persist($errorQueue);
        $dm->flush();

        $this->getAirbrakeNotifier()->notify(new \Exception(
            '['. strtolower(ucfirst($this->getAdSystem())) . ' - ERROR]: '. $errorDetails
                . '. System Campaign Id: '. $systemCampaignId. PHP_EOL
            )
        );
    }

    /**
     * @param int       $cityId
     * @param string    $errorMessage
     * @param string    $errorDetails
     * @param           $clientCustomerId
     * @param           $systemCampaignId
     * @param array     $campaignDetails
     *
     * @throws MongoDBException|Exception|ContainerExceptionInterface|NotFoundExceptionInterface
     */
    public function registerInitLocationErrorsAndDeleteCampaign(
        int     $cityId,
        string  $errorMessage,
        string  $errorDetails,
                $clientCustomerId,
                $systemCampaignId,
        array   $campaignDetails
    ) {
        /** @var EntityManager $em */
        $em = $this->getEntityManager();
        /** @var DocumentManager $dm */
        $dm = $this->getDocumentManager();

        // Mark location as deprecated in db
        $where = ['locationId' => $cityId];
        $attributes = ['deprecated' => true];
        $em->getRepository(ProviderEntityName::getForZipcodesBySystem($this->getAdSystem()))
            ->updateByAttributes($where, $attributes);

        // Remove campaign in system
        $deleted = false;
        $retry = 1;
        while (!$deleted and $retry < 4) {
            sleep(pow($retry, 3));
            $deleted = $this->deleteCampaign($systemCampaignId, $clientCustomerId);
            $retry++;
        }

        if ($deleted) {
            // Clean up all queues by campaign
            CampaignManager::cleanUpAllQueuesByCampaignId($campaignDetails['id'], $dm);

            $dm->createQueryBuilder(CampaignProcess::class)->remove()
                ->field('campaignId')->equals(new ObjectId($campaignDetails['id']))
                ->getQuery()->execute();

            $kcCampaign = $dm->createQueryBuilder(KcCampaign::class);
            $kcCampaign->updateMany()
                ->field('backendId')->equals($campaignDetails['backendId'])
                ->field('amountAdwCampaigns')->inc(-1)
                ->field('campaigns')->pull($kcCampaign->expr()
                    ->field('id')->equals(new ObjectId($campaignDetails['id'])))
                ->getQuery()->execute();

            $errorDetails = $errorDetails . " Current campaign was removed.";
        } else {
            $errorDetails = $errorDetails . " The campaign WASN'T removed after the location initialization error.";

            if ($this->holdCampaign($clientCustomerId, $systemCampaignId,  $campaignDetails)){
                $errorDetails = $errorDetails . " Current campaign was set on hold.";
            } else {
                $errorDetails = $errorDetails . " The campaign WASN'T set on hold.";
                $slackSenderService = $this->getSlackSenderService();
                $slackSenderService->sendCustomizedMessagesToSlack(
                    "[ERROR]: $errorDetails (Location ID: {$cityId}). System Campaign Id: $systemCampaignId",
                    'webhook2'
                );
            }
        }

        $errorDetails = $errorDetails . " (Location ID: {$cityId})";

        $errorInfo = [
            'type' => ErrorLevel::CAMPAIGN,
            'backendId' => $campaignDetails['backendId'],
            'campaignId' => $campaignDetails['id'],
            'campaignName' => $campaignDetails['name'],
            'error' => $errorDetails,
            'rawError' => $errorMessage,
        ];
        $errorQueue = new ErrorsQueue();
        $errorQueue->fill($errorInfo);
        $dm->persist($errorQueue);
        $dm->flush();

        $this->getAirbrakeNotifier()->notify(new \Exception(
            '['. strtolower(ucfirst($this->getAdSystem())) . ' - ERROR]: '. $errorDetails
                . '. System Campaign Id: '. $systemCampaignId. PHP_EOL
            )
        );
    }

    /**
     * @param       $clientCustomerId
     * @param       $systemCampaignId
     * @param array $campaignDetails
     *
     * @return bool
     * @throws MongoDBException|ContainerExceptionInterface|NotFoundExceptionInterface
     */
    private function holdCampaign($clientCustomerId, $systemCampaignId, array $campaignDetails): bool
    {
        $dm = $this->getDocumentManager();

        // Update status campaign in system
        $updated = false;
        $retry = 1;
        while (!$updated and $retry < 4) {
            sleep(pow($retry, 3));
            $updated = $this->changeCampaignStatus($clientCustomerId, [$systemCampaignId], false);
            $retry++;
        }

        if ($updated) {
            $dm->createQueryBuilder(MongoKcCampaign::class)->updateOne()
                ->field('backendId')->equals($campaignDetails['backendId'])
                ->field('campaigns.id')->equals(new ObjectId($campaignDetails['id']))
                ->field('campaigns.$.hold')->set(true)
                ->field('campaigns.$.status')->set(false)
                ->getQuery()->execute();
        }

        return $updated;
    }
}