<?php

namespace App\Extensions\Common\InternalWork\Interfaces;

use App\Document\KcCampaign;

/**
 * Interface CampaignBidModifiersManagerInterface
 * @package App\Extensions\Common\InternalWork\Interfaces
 */
interface BidModifiersManagerInterface
{
    /**
     * @param KcCampaign $kcCampaignInMongo
     * @param array $bidModifiers
     * @return bool
     */
    public function syncBidModifiers(KcCampaign $kcCampaignInMongo, array $bidModifiers): bool;

    /**
     */
    public function uploadBidModifiers();
}
