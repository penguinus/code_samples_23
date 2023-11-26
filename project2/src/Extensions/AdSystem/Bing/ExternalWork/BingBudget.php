<?php

namespace App\Extensions\AdSystem\Bing\ExternalWork;

use Airbrake\Notifier;
use App\Extensions\AdSystem\Bing\ExternalWork\Auth\BingServiceManager;
use App\Extensions\Common\AdSystemEnum;
use App\Extensions\Common\ExternalWork\BudgetInterface;
use Microsoft\BingAds\V13\CampaignManagement\AddBudgetsRequest;
use Microsoft\BingAds\V13\CampaignManagement\Budget;
use Microsoft\BingAds\V13\CampaignManagement\BudgetLimitType;
use Microsoft\BingAds\V13\CampaignManagement\UpdateBudgetsRequest;
use Psr\Container\ContainerInterface;
use Psr\Log\LoggerInterface;
use SoapFault;

/**
 * Class BingBudget
 * @package App\Extensions\AdSystem\Bing\ExternalWork
 */
class BingBudget implements BudgetInterface
{
    /**
     * @var BingServiceManager
     */
    private BingServiceManager $serviceManager;

    /**
     * @var LoggerInterface
     */
    private LoggerInterface $logger;

    /**
     * @var ContainerInterface
     */
    private ContainerInterface $container;

    /**
     * BingBudget constructor.
     * @param BingServiceManager    $bingServiceManager
     * @param LoggerInterface       $bingLogger
     * @param ContainerInterface    $container
     */
    public function __construct(
        BingServiceManager  $bingServiceManager,
        LoggerInterface     $bingLogger,
        ContainerInterface  $container)
    {
        $this->serviceManager   = $bingServiceManager;
        $this->logger           = $bingLogger;
        $this->container        = $container;
    }

    /**
     * @return BingServiceManager
     */
    protected function getBingServicesManager(): BingServiceManager
    {
        return $this->serviceManager;
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

    /**
     * @param string    $budgetName
     * @param float     $kcCampaignBudget
     * @param int       $clientCustomerId
     *
     * @return int|bool
     */
    public function createBudget(string $budgetName, float $kcCampaignBudget, int $clientCustomerId)
    {
        $campaignService = $this->getBingServicesManager()->getCampaignManagementService($clientCustomerId);

        $budget = new Budget();
        $budget->Amount = $kcCampaignBudget;
        $budget->BudgetType = BudgetLimitType::DailyBudgetAccelerated;
        $budget->Name = $budgetName;

        $request = new AddBudgetsRequest();
        $request->Budgets = [$budget];

        $response = $campaignService->GetService()->AddBudgets($request);

        if (!empty($response->BudgetIds->long[0])) {
            return $response->BudgetIds->long[0];
        } elseif (!empty($response->PartialErrors->BatchError[0])) {
            $budgetName = $budgetName . "-2";
            $budget->Name = $budgetName;

            $request = new AddBudgetsRequest();
            $request->Budgets = [$budget];

            $response = $campaignService->GetService()->AddBudgets($request);

            if (!empty($response->BudgetIds->long[0])) {
                return $response->BudgetIds->long[0];
            } elseif (!empty($response->PartialErrors->BatchError[0])) {
                $this->getAirbrakeNotifier()->notify(new \Exception(
                    '[Bing] Budget '. $budgetName .' can\'t be created.'
                    . $response->PartialErrors->BatchError[0]->Message
                    . '. "Customer Id: "'. $clientCustomerId. PHP_EOL
                ));

                return false;
            } else {
                $this->getAirbrakeNotifier()->notify(new \Exception(
                    '[Bing] Budget '. $budgetName .' can\'t be created. "Customer Id: "'. $clientCustomerId. PHP_EOL
                ));

                return false;
            }

        } else {
            $this->getAirbrakeNotifier()->notify(new \Exception(
                '[Bing] Budget '. $budgetName .' can\'t be created. "Customer Id: "'. $clientCustomerId. PHP_EOL
            ));

            return false;
        }
    }

    /**
     * @param string $kcCampaignName
     * @return string
     */
    public function getBudgetName($kcCampaignName)
    {
        return "";
    }

    /**
     * @param int   $systemBudgetId
     * @param float $kcCampaignBudget
     * @param int   $clientCustomerId
     *
     * @return int|bool
     */
    public function updateBudget(int $systemBudgetId, float $kcCampaignBudget, int $clientCustomerId)
    {
        $campaignService = $this->getBingServicesManager()->getCampaignManagementService($clientCustomerId);

        $budget = new Budget();
        $budget->Id = $systemBudgetId;
        $budget->Amount = $kcCampaignBudget;

        $request = new UpdateBudgetsRequest();
        $request->Budgets = [$budget];

        try {
            // Make the mutate request.
            $response = $campaignService->GetService()->UpdateBudgets($request);
        } catch (SoapFault $e) {
            $this->getAirbrakeNotifier()->notify(new \Exception(
                '[Bing] SoapFault was thrown with message "Cannot update budget - ' . $systemBudgetId
                . '" ' . $e->getMessage() . '. Customer ID: ' . $clientCustomerId
                . '. Request: ' . $campaignService->GetService()->__getLastRequest()
                . '. Response: '. $campaignService->GetService()->__getLastResponse(). PHP_EOL
            ));

            return false;
        }

        if (!empty($response->PartialErrors->BatchError[0])) {
            $this->getAirbrakeNotifier()->notify(new \Exception(
                    '[Bing] Can\'t update budget: ' . $systemBudgetId . '. '
                    . $response->PartialErrors->BatchError[0]->Message
                    . '. Customer Id: ' . $clientCustomerId. PHP_EOL
            ));

            return false;

        }

        return $systemBudgetId;
    }

    /**
     * @param int       $systemBudgetId
     * @param string    $newBudgetName
     * @param int       $clientCustomerId
     *
     * @return int|bool
     */
    public function updateBudgetName(int $systemBudgetId, string $newBudgetName, int $clientCustomerId)
    {
        $campaignService = $this->getBingServicesManager()->getCampaignManagementService($clientCustomerId);

        $budget = new Budget();
        $budget->Id = $systemBudgetId;
        $budget->Name = $newBudgetName;

        $request = new UpdateBudgetsRequest();
        $request->Budgets = [$budget];

        try {
            // Make the mutate request.
            $response = $campaignService->GetService()->UpdateBudgets($request);
        } catch (SoapFault $e) {
            $this->getAirbrakeNotifier()->notify(new \Exception(
                '[Bing] SoapFault was thrown with message "Cannot update budget - ' . $systemBudgetId
                . '" ' . $e->getMessage() . '. Customer ID: ' . $clientCustomerId
                . '. Request: ' . $campaignService->GetService()->__getLastRequest()
                . '. Response: '. $campaignService->GetService()->__getLastResponse(). PHP_EOL
            ));

            return false;
        }

        if (!empty($response->PartialErrors->BatchError[0])) {
            $this->getAirbrakeNotifier()->notify(new \Exception(
                '[Bing] Can\'t update budget: ' . $systemBudgetId . '. '
                . $response->PartialErrors->BatchError[0]->Message
                . '. Customer Id: '. $clientCustomerId. PHP_EOL
            ));

            return false;
        }

        return $systemBudgetId;
    }

    /**
     * @param int $clientCustomerId
     * @param string $budgetName
     *
     * @return false
     */
    public function checkBudgetNamesInAdSystem(string $budgetName, int $clientCustomerId): bool
    {
        /** Maybe when Bing will make a norm library and it will be able to get a budget by name. */
        return false;
    }
}