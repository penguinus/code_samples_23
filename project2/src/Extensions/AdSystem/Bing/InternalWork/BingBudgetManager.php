<?php
/**
 * Created by PhpStorm.
 * User: vadim
 * Date: 27.04.18
 * Time: 15:32
 */

namespace App\Extensions\AdSystem\Bing\InternalWork;

use Airbrake\Notifier;
use App\Extensions\AdSystem\Bing\ExternalWork\BingBudget;
use App\Extensions\AdSystem\Bing\ExternalWork\BingCampaign;
use App\Extensions\Common\AdSystemEnum;
use App\Extensions\Common\ExternalWork\BudgetInterface;
use App\Extensions\Common\ExternalWork\CampaignInterface;
use App\Extensions\Common\InternalWork\Basic\BudgetManager;
use Doctrine\ODM\MongoDB\DocumentManager;
use Doctrine\ORM\EntityManagerInterface;
use Psr\Container\ContainerInterface;
use Psr\Log\LoggerInterface;


/**
 * Class BingBudgetManager
 * @package App\Extensions\AdSystem\Bing\InternalWork
 */
class BingBudgetManager extends BudgetManager
{
    /**
     * @var LoggerInterface
     */
    private LoggerInterface $logger;

    /**
     * @var ContainerInterface
     */
    private ContainerInterface $container;

    /**
     * @var BingBudget
     */
    private BingBudget $budgetService;

    /**
     * @var BingCampaign
     */
    private BingCampaign $campaignService;

    /**
     * BingBudgetManager constructor.
     * @param EntityManagerInterface    $em
     * @param DocumentManager           $dm
     * @param BingBudget                $budgetService
     * @param BingCampaign              $campaignService
     * @param ContainerInterface        $container
     * @param LoggerInterface           $bingLogger
     */
    public function __construct(
        EntityManagerInterface  $em,
        DocumentManager         $dm,
        BingBudget              $budgetService,
        BingCampaign            $campaignService,
        ContainerInterface      $container,
        LoggerInterface         $bingLogger
    ) {
        parent::__construct(AdSystemEnum::BING, $em, $dm);

        $this->budgetService    = $budgetService;
        $this->campaignService  = $campaignService;
        $this->container        = $container;
        $this->logger           = $bingLogger;
    }

    /**
     * @return BudgetInterface
     */
    protected function getBudgetService(): BudgetInterface
    {
        return $this->budgetService;
    }

    /**
     * @return CampaignInterface
     */
    protected function getCampaignManager(): CampaignInterface
    {
        return $this->campaignService;
    }

    /**
     * @return LoggerInterface
     */
    protected function getLogger(): LoggerInterface
    {
        return $this->logger;
    }

    /**
     * @return Notifier
     */
    protected function getAirbrakeNotifier(): Notifier
    {
        return $this->container->get('ami_airbrake.notifier');
    }
}
