<?php

namespace App\Extensions\AdSystem\AdWords\InternalWork;

use Airbrake\Notifier;
use App\Extensions\AdSystem\AdWords\ExternalWork\AdWordsBudget;
use App\Extensions\AdSystem\AdWords\ExternalWork\AdWordsCampaign;
use App\Extensions\Common\AdSystemEnum;
use App\Extensions\Common\ExternalWork\BudgetInterface;
use App\Extensions\Common\ExternalWork\CampaignInterface;
use App\Extensions\Common\InternalWork\Basic\BudgetManager;
use Doctrine\ODM\MongoDB\DocumentManager;
use Doctrine\ORM\EntityManagerInterface;
use Psr\Container\ContainerInterface;
use Psr\Log\LoggerInterface;

/**
 * Class AdWordsBudgetManager
 * @package App\Extensions\AdSystem\AdWords\InternalWork
 */
class AdWordsBudgetManager extends BudgetManager
{
    /**
     * @var LoggerInterface
     */
    private LoggerInterface $logger;

    /**
     * @var AdWordsBudget
     */
    private AdWordsBudget $budgetService;

    /**
     * @var ContainerInterface
     */
    private ContainerInterface $container;

    /**
     * @var AdWordsCampaign
     */
    private AdWordsCampaign $campaignService;

    /**
     * AdWordsBudgetManager constructor.
     *
     * @param EntityManagerInterface    $em
     * @param DocumentManager           $dm
     * @param AdWordsBudget             $budgetService
     * @param AdWordsCampaign           $campaignService
     * @param ContainerInterface        $container
     * @param LoggerInterface           $adwordsLogger
     */
    public function __construct(
        EntityManagerInterface  $em,
        DocumentManager         $dm,
        AdWordsBudget           $budgetService,
        AdWordsCampaign         $campaignService,
        ContainerInterface      $container,
        LoggerInterface         $adwordsLogger
    ) {
        parent::__construct(AdSystemEnum::ADWORDS, $em, $dm);

        $this->budgetService    = $budgetService;
        $this->campaignService  = $campaignService;
        $this->container        = $container;
        $this->logger           = $adwordsLogger;
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
