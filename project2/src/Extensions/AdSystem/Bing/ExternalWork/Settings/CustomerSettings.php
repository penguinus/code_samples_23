<?php

namespace App\Extensions\AdSystem\Bing\ExternalWork\Settings;

use Microsoft\BingAds\V13\CustomerManagement\Industry;
use Microsoft\BingAds\V13\CustomerManagement\LanguageType;

/**
 * Class CustomerSettings
 * @package KC\DataBundle\Extensions\AdSystem\Bing\ExternalWork\Settings
 */
class CustomerSettings
{
    /** @var string */
    const INDUSTRY = Industry::Services;

    /** @var string */
    const MARKET_COUNTRY = "US";

    /** @var string */
    const MARKET_LANGUAGE = LanguageType::English;
}