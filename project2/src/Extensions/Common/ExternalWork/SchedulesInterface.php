<?php

namespace App\Extensions\Common\ExternalWork;

/**
 * Interface SchedulesInterface
 *
 * @package App\Extensions\Common\ExternalWork
 */
interface SchedulesInterface
{
    /**
     * @param int   $clientCustomerId
     * @param int   $systemCampaignId
     * @param array $schedulesData
     *
     * @return bool
     */
    public function createSchedules(int $clientCustomerId, int $systemCampaignId, array $schedulesData): bool;

    /**
     * @param int $systemCampaignId
     * @param int $clientCustomerId
     *
     * @return array
     */
    public function getUploadedSchedules(int $systemCampaignId, int $clientCustomerId): array;

    /**
     * @param int   $clientCustomerId
     * @param int   $systemCampaignId
     * @param int[] $scheduleIds
     *
     * @return bool
     */
    public function deleteSchedules(int $clientCustomerId, int $systemCampaignId, array $scheduleIds): bool;

}
