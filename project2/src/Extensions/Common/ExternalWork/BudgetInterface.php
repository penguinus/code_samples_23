<?php

namespace App\Extensions\Common\ExternalWork;

/**
 * Interface BudgetInterface
 * @package App\Extensions\Common\ExternalWork
 */
interface BudgetInterface
{
    /**
     * @param string $budgetName
     * @param float $kcCampaignBudget
     * @param int $clientCustomerId
     */
    public function createBudget(string $budgetName, float $kcCampaignBudget, int $clientCustomerId);

    /**
     * @param $campaign
     * @return string
     */
    public function getBudgetName($campaign);

    /**
     * @param int $systemBudgetId
     * @param float $kcCampaignBudget
     * @param int $clientCustomerId
     */
    public function updateBudget(int $systemBudgetId, float $kcCampaignBudget, int $clientCustomerId);

    /**
     * @param int $systemBudgetId
     * @param string $newBudgetName
     * @param int $clientCustomerId
     *
     * @return int|bool
     */
    public function updateBudgetName(int $systemBudgetId, string $newBudgetName, int $clientCustomerId);


    /**
     * @param int $clientCustomerId
     * @param string $budgetName
     * @return int|bool
     */
    public function checkBudgetNamesInAdSystem(string $budgetName, int $clientCustomerId);
}