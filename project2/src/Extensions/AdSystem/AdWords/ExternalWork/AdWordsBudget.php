<?php

namespace App\Extensions\AdSystem\AdWords\ExternalWork;

use Airbrake\Notifier;
use App\Extensions\AdSystem\AdWords\ExternalWork\Auth\AdWordsServiceManager;
use App\Extensions\Common\ExternalWork\BudgetInterface;
use Google\Ads\GoogleAds\Util\FieldMasks;
use Google\Ads\GoogleAds\Util\V13\ResourceNames;
use Google\Ads\GoogleAds\V13\Enums\BudgetDeliveryMethodEnum\BudgetDeliveryMethod;
use Google\Ads\GoogleAds\V13\Resources\CampaignBudget;
use Google\Ads\GoogleAds\V13\Services\{CampaignBudgetOperation, CampaignBudgetServiceClient, GoogleAdsRow};
use Google\ApiCore\ApiException;
use Google\ApiCore\ValidationException;
use Psr\Container\ContainerInterface;
use Psr\Log\LoggerInterface;

/**
 * Class AdWordsBudget
 *
 * @package App\Extensions\AdSystem\AdWords\ExternalWork
 */
class AdWordsBudget implements BudgetInterface
{
    /**
     * @var AdWordsServiceManager
     */
    private AdWordsServiceManager $serviceManager;

    /**
     * @var LoggerInterface
     */
    private LoggerInterface $logger;

    /**
     * @var ContainerInterface
     */
    private ContainerInterface $container;

    /**
     * GoogleBudget constructor.
     * @param AdWordsServiceManager $serviceManager
     * @param LoggerInterface       $googleLogger
     * @param ContainerInterface    $container
     */
    public function __construct(
        AdWordsServiceManager   $serviceManager,
        LoggerInterface         $googleLogger,
        ContainerInterface      $container
    ) {
        $this->serviceManager   = $serviceManager;
        $this->logger           = $googleLogger;
        $this->container        = $container;
    }

    /**
     * @return AdWordsServiceManager
     */
    protected function getGoogleServiceManager(): AdWordsServiceManager
    {
        return $this->serviceManager;
    }

    /**
     * @return LoggerInterface
     */
    public function getLogger(): LoggerInterface
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
     * @param int       $customerId
     *
     * @return false | int
     */
    public function createBudget(string $budgetName, float $kcCampaignBudget, int $customerId)
    {
        // Create a Campaign Budget
        $campaignBudget = new CampaignBudget();
        $campaignBudget->setName($budgetName);
        $campaignBudget->setDeliveryMethod(BudgetDeliveryMethod::STANDARD);
        $campaignBudget->setAmountMicros((int)($kcCampaignBudget * 1000000));

        /**
         * Constructs a campaign budget operation.
         * @see https://developers.google.com/google-ads/api/reference/rpc/v9/CampaignBudgetOperation
         */
        $campaignBudgetOperation = new CampaignBudgetOperation();
        $campaignBudgetOperation->setCreate($campaignBudget);

        try {
            return $this->getAddedBudget($campaignBudgetOperation, $customerId);

        } catch (\Exception $e) {
            if (!strpos(AdWordsErrorDetail::errorDetail($e->getMessage()), "internal error")) {
                $this->getAirbrakeNotifier()->notify(new \Exception(
                    '[Google] Budget ' . $budgetName . ' can\'t be created. ' . $e->getMessage()
                    . '. Customer Id: ' . $customerId . PHP_EOL
                ));
            }

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
     * @param int       $systemBudgetId
     * @param float     $kcCampaignBudget
     * @param int       $customerId
     *
     * @return int|bool
     */
    public function updateBudget(int $systemBudgetId, float $kcCampaignBudget, int $customerId)
    {
        // Create a Campaign Budget
        $campaignBudget = new CampaignBudget();
        $campaignBudget->setResourceName(ResourceNames::forCampaignBudget($customerId, $systemBudgetId));
        $campaignBudget->setDeliveryMethod(BudgetDeliveryMethod::STANDARD);
        $campaignBudget->setAmountMicros((int)($kcCampaignBudget * 1000000));

        /**
         * Constructs a campaign budget operation.
         * @see https://developers.google.com/google-ads/api/reference/rpc/v9/CampaignBudgetOperation
         */
        $campaignBudgetOperation = new CampaignBudgetOperation();
        $campaignBudgetOperation->setUpdate($campaignBudget);
        $campaignBudgetOperation->setUpdateMask(FieldMasks::allSetFieldsOf($campaignBudget));

        try {
            return $this->getAddedBudget($campaignBudgetOperation, $customerId);

        } catch (\Exception $e) {
            if (!strpos(AdWordsErrorDetail::errorDetail($e->getMessage()), "internal error")) {
                $this->getAirbrakeNotifier()->notify(new \Exception(
                    '[Google] Budget Id: ' . $systemBudgetId . ', can\'t be update. '
                    . $e->getMessage() . '. Customer Id: ' . $customerId . PHP_EOL
                ));
            }

            return false;
        }
    }

    /**
     * @param int       $systemBudgetId
     * @param string    $newBudgetName
     * @param int       $customerId
     *
     * @return int|bool
     */
    public function updateBudgetName(int $systemBudgetId, string $newBudgetName, int $customerId)
    {
        // Create a Campaign Budget
        $campaignBudget = new CampaignBudget();
        $campaignBudget->setResourceName(ResourceNames::forCampaignBudget($customerId, $systemBudgetId));
        $campaignBudget->setName($newBudgetName);

        /**
         * Constructs a campaign budget operation.
         * @see https://developers.google.com/google-ads/api/reference/rpc/v9/CampaignBudgetOperation
         */
        $campaignBudgetOperation = new CampaignBudgetOperation();
        $campaignBudgetOperation->setUpdate($campaignBudget);
        $campaignBudgetOperation->setUpdateMask(FieldMasks::allSetFieldsOf($campaignBudget));

        try {
            return $this->getAddedBudget($campaignBudgetOperation, $customerId);

        } catch (\Exception $e) {
            if (!strpos(AdWordsErrorDetail::errorDetail($e->getMessage()), "internal error")) {
                $this->getAirbrakeNotifier()->notify(new \Exception(
                    '[Google] Budget Id: ' . $systemBudgetId . ', can\'t be update budget name. '
                    . $e->getMessage() . '. Customer Id: ' . $customerId . PHP_EOL
                ));
            }

            return false;
        }
    }

    /**
     * @param string    $budgetName
     * @param int       $customerId
     *
     * @return bool|int
     * @throws ApiException | ValidationException
     */
    public function checkBudgetNamesInAdSystem(string $budgetName, int $customerId)
    {
        /**
         * Possible statuses of a Budget (UNSPECIFIED, UNKNOWN, ENABLED, REMOVED)
         * @see https://developers.google.com/google-ads/api/reference/rpc/v9/BudgetStatusEnum.BudgetStatus?hl=en
         */
        $query = /** @lang text */
            "SELECT campaign_budget.id, campaign_budget.name
             FROM campaign_budget 
             WHERE campaign_budget.status IN ('ENABLED')
             ORDER BY campaign_budget.id ASC";

        $googleAdsServiceClient = $this->getGoogleServiceManager()->getGoogleAdsServiceClient();
        $budgets = $googleAdsServiceClient->search(
            $customerId,
            $query,
            ['pageSize' => AdWordsServiceManager::PAGE_SIZE]
        );

        $googleBudgetId = null;

        // Iterates over all rows in all messages and prints the requested field values for
        // the campaign budgets in each row.
        foreach ($budgets->iterateAllElements() as $googleAdsRow) {
            /** @var GoogleAdsRow $googleAdsRow */
            if (strcmp($budgetName, $googleAdsRow->getCampaignBudget()->getName()) == 0) {
                $googleBudgetId = $googleAdsRow->getCampaignBudget()->getId();
            }
        }

        return $googleBudgetId ?: false;
    }

    /**
     * @param CampaignBudgetOperation   $campaignBudgetOperation
     * @param int                       $customerId
     *
     * @return int
     * @throws \Exception
     */
    private function getAddedBudget(CampaignBudgetOperation $campaignBudgetOperation, int $customerId): int
    {
        $campaignBudget = null;

        try {
            // Issues a mutate request to create the budget.
            $campaignBudgetServiceClient = $this->getGoogleServiceManager()->getCampaignBudgetServiceClient();

            $response = $campaignBudgetServiceClient->mutateCampaignBudgets(
                $customerId,
                [$campaignBudgetOperation]
            );

            if (!empty($response->getResults()[0])) {
                $campaignBudget = CampaignBudgetServiceClient::parseName($response->getResults()[0]->getResourceName());
                $campaignBudget = (int)$campaignBudget['campaign_budget_id'];
            } else {
                $this->getAirbrakeNotifier()->notify(new \Exception(
                '[Google] Unknown problem, Google Ads API return empty response for process '
                    . '"getAddedBudget". Budget ' . $campaignBudgetOperation->getCreate()->getName()
                    . ' can\'t create. Customer Id: '. $customerId. PHP_EOL
                ));
            }

        } catch (ApiException $apiException) {
            foreach ($apiException->getMetadata() as $metadatum) {
                foreach ($metadatum['errors'] as $error) {
                    $errorMessage = null;
                    if ($campaignBudgetOperation->hasCreate()) {
                        $errorMessage = '[Google] ApiException was thrown with message - ' . $error['message'] . '. Budget '
                            . $campaignBudgetOperation->getCreate()->getName(). ' can\'t create. Customer Id: '
                            . $customerId. PHP_EOL;
                    } elseif ($campaignBudgetOperation->hasUpdate()) {
                        $errorMessage = '[Google] ApiException was thrown with message - ' . $error['message']
                            . '. Budget ResourceName "' . $campaignBudgetOperation->getUpdate()->getResourceName()
                            . '" can\'t update. Customer Id: ' . $customerId. PHP_EOL;
                    }

                    if ($errorMessage != null) {
                        $this->getAirbrakeNotifier()->notify(new \Exception($errorMessage));
                    }

                }
            }
        }

        return $campaignBudget ?: false;
    }
}