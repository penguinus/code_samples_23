<?php

namespace App\Extensions\Common\InternalWork\Interfaces;

use App\Document\CampaignProcess;
use MongoDB\BSON\ObjectId;

/**
 * Interface CampaignManagerInterface
 * @package App\Extensions\Common\InternalWork\Interfaces
 */
interface CampaignManagerInterface
{
    /**
     * @return boolean
     */
    public function generateCampaigns();

    /**
     * Uploading campaigns to ad system
     */
    public function uploadCampaigns();

    /**
     * @param int $backendId
     * @return bool
     */
    public function addKcCampaignInQueueForDelete(int $backendId): bool;

    /**
     * Delete campaigns form ad system and local database
     */
    public function deleteCampaigns();

    /**
     * @param array $zipcodes
     * @param int $backendId
     */
    public function cleanupCampaigns(array $zipcodes, int $backendId);

    /**
     * @param ObjectId $campaignId
     * @param string $campaignName
     * @param int $kcCampaignBackendId
     * @param int $clientCustomerId
     *
     * @return string $campaignName
     */
    public function getCampaignName(
        ObjectId $campaignId,
        string $campaignName,
        int $kcCampaignBackendId,
        int $clientCustomerId
    ): string;

    /**
     * @param int $backendId
     * @param string $newKcCampaignName
     * @return bool
     */
    public function updateCampaignNames(int $backendId, string $newKcCampaignName): bool;

    /**
     * @param CampaignProcess $hotsCampaign
     * @param integer $systemCampaignId
     * @param string $campaignName
     */
    public function setSystemCampaignNameAndIdInAllCollection($hotsCampaign, $systemCampaignId, $campaignName);

    /**
     * @param CampaignProcess $hotsCampaign
     * @param array $errors
     * @param string $campaignName
     */
    public function registerCampaignUploadingErrors(CampaignProcess $hotsCampaign , array $errors, string $campaignName);

    /**
     * @param array $zipcodes
     * @param int $backendId
     * @return int
     */
    public function estimateCampaigns(array $zipcodes, int $backendId): int;

    /**
     * @param array int $backendIds
     */
    public function cleanUpKcCampaignsFromDatabase(array $backendIds);

    /**
     * @param CampaignProcess $campaignProcess
     */
    public function cleanUpCampaignFromDatabase(CampaignProcess $campaignProcess);
}
