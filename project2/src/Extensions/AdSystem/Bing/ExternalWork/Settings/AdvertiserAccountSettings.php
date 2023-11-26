<?php

namespace App\Extensions\AdSystem\Bing\ExternalWork\Settings;

use Microsoft\BingAds\V13\CustomerManagement\TimeZoneType;

/**
 * Class AdvertiserAccountSettings
 * @package KC\DataBundle\Extensions\AdSystem\Bing\ExternalWork\Settings
 */
class AdvertiserAccountSettings
{
    /** @var string */
    const CURRENCY_CODE = 'USD';

    /** @var string */
    const TIME_ZONE = TimeZoneType::EasternTimeUSCanada;
}