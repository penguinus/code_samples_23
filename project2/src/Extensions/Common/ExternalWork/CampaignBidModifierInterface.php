<?php

namespace App\Extensions\Common\ExternalWork;

/**
 * Interface CampaignBidModifierInterface
 * @package App\Extensions\Common\ExternalWork
 */
interface CampaignBidModifierInterface
{
    /**
     * @param int|string    $clientCustomerId
     * @param int|string    $systemCampaignId
     * @param string        $typeDevice
     * @param float         $deviceBidModifier
     *
     * @return mixed
     */
    public function makeDeviceBidModifierOperation($clientCustomerId, $systemCampaignId, string $typeDevice, float $deviceBidModifier);

    /**
     * @param int|null $clientCustomerId
     * @param array     $operations
     * @return mixed
     */
    public function uploadCriterionOperations(array $operations, ?int $clientCustomerId = null);
}