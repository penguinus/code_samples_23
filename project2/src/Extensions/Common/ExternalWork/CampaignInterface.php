<?php

namespace App\Extensions\Common\ExternalWork;

use App\Document\CampaignProcess;

/**
 * Interface CampaignInterface
 * @package App\Extensions\Common\ExternalWork
 */
interface CampaignInterface
{
    /**
     * @param int $clientCustomerId
     * @param string $campaignName
     * @param CampaignProcess $hotsCampaign
     * @return int|false
     */
    public function addCampaign(int $clientCustomerId, string $campaignName, CampaignProcess $hotsCampaign): ?int;

    /**
     * @param int   $clientCustomerId
     * @param int   $systemCampaignId
     * @param int   $locationId
     * @param array $campaignDetails
     *
     * @return mixed
     */
    public function initLocation(int $clientCustomerId, int $systemCampaignId, int $locationId, array $campaignDetails): bool;

    /**
     * @param int       $systemCampaignId
     * @param string    $newCampaignName
     * @param int       $clientCustomerId
     * @return bool
     */
    public function updateCampaignName(int $systemCampaignId, string $newCampaignName, int $clientCustomerId): bool;

    /**
     * @param array $systemCampaignIds
     * @param int   $systemBudgetId
     * @param int   $clientCustomerId
     * @return bool
     */
    public function updateCampaignsBudget(array $systemCampaignIds, int $systemBudgetId, int $clientCustomerId): bool;

    /**
     * @param int       $clientCustomerId
     * @param string    $campaignName
     *
     * @return int|null
     */
    public function findCampaignInAdSystemByName(int $clientCustomerId, string $campaignName): ?int;

    /**
     * @param int   $clientCustomerId
     * @param array $systemCampaignIds
     * @param bool  $status
     *
     * @return bool
     */
    public function changeCampaignStatus(int $clientCustomerId, array $systemCampaignIds, bool $status): bool;

    /**
     * @param int $systemCampaignId
     * @param int $clientCustomerId
     *
     * @return bool
     */
    public function deleteCampaign(int $systemCampaignId, int $clientCustomerId): bool;
}
