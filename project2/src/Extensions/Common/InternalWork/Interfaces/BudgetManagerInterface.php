<?php

namespace App\Extensions\Common\InternalWork\Interfaces;


/**
 * Interface BudgetManagerInterface
 * @package App\Extensions\Common\InternalWork\Interfaces
 */
interface BudgetManagerInterface
{
    /** sync Budget (create or update) in ad system*/
    public function syncBudgets();

    /**
     * @param int $backendId
     * @param int $systemAccount
     * @param int $budgetId
     */
    public function setSystemBudgetIdAllCampaigns(int $backendId, int $systemAccount, int $budgetId);

    /**
     * @param int $backendId
     * @param string $newKcCampaignName
     * @return bool
     */
    public function updateBudgetName(int $backendId, string  $newKcCampaignName): bool;
}
