<?php

namespace App\Extensions\Common\ExternalWork;


/**
 * Interface AdInterface
 * @package App\Extensions\Common\ExternalWork
 */
interface AdInterface
{
    /**
     * @param $url
     * @param $backendId
     * @param $adgroupId
     * @param $channelId
     * @param $cityId
     * @return mixed|string
     */
    public static function applyParamsForUrl($url, $backendId, $adgroupId, $channelId, $cityId);

}