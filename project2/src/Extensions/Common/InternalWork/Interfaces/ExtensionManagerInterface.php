<?php

namespace App\Extensions\Common\InternalWork\Interfaces;

use App\Entity\KcCampaign;

/**
 * Interface ExtensionManagerInterface
 * @package App\Extensions\Common\InternalWork\Interfaces
 */
interface ExtensionManagerInterface
{
    /**
     * @param array $item
     * @param KcCampaign $kcCampaignInMySql
     * @return mixed
     */
    public function checkNeedUpdateAndUpdate(
        array $item,
        KcCampaign $kcCampaignInMySql
    );

    /**
     * @return mixed|void
     */
    public function syncExtensionsWithAdSystem();
}