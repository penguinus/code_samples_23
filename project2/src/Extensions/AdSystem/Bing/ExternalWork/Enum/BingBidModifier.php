<?php

namespace App\Extensions\AdSystem\Bing\ExternalWork\Enum;

/**
 * Class BingBidModifier
 * @package App\Extensions\AdSystem\Bing\ExternalWork\Enum
 */
class BingBidModifier
{
    const COMPUTERS = "Computers";

    const SMARTPHONES = "Smartphones";

    const TABLETS = "Tablets";

    const ALL = "All";

    /**
     * Possible case-sensitive values
     */
    const DEVICE_TYPES = [
        self::COMPUTERS,
        self::SMARTPHONES,
        self::TABLETS,
    ];
}