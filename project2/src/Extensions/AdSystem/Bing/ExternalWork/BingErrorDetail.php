<?php

namespace App\Extensions\AdSystem\Bing\ExternalWork;

use App\Extensions\Common\ExternalWork\ErrorDetailInterface;

/**
 * Class BingErrorDetail
 * @package KC\DataBundle\Extensions\AdSystem\Bing\ExternalWork
 */
class BingErrorDetail implements ErrorDetailInterface
{
    /**
     * @param int $errorCode
     * @return string
     */
    public static function errorDetail($errorCode): string
    {
        $errorCode = (int)$errorCode;

        if ($errorCode >= 0 && $errorCode < 500)
            return self::errorFrom0To500($errorCode);
        elseif ($errorCode >= 500 && $errorCode < 1000)
            return self::errorFrom500To1000($errorCode);
        elseif ($errorCode >= 1000 && $errorCode < 1500)
            return self::errorFrom1000To1500($errorCode);
        elseif ($errorCode >= 5000 && $errorCode < 5500)
            return self::errorFrom1000To1500($errorCode);
        else
            return "";
    }

    /**
     * @param $errorCode
     * @return string
     */
    public static function errorFrom0To500($errorCode): string
    {
        switch ($errorCode) {
            case 0:
                return "An unidentified error has occurred. You may obtain additional information about 
                    this error by contacting Bing Ads Support.";
            default:
                return "";
        }
    }

    /**
     * @param $errorCode
     * @return string
     */
    public static function errorFrom500To1000($errorCode): string
    {
        switch ($errorCode) {
            default:
                return "";
        }
    }

    /**
     * @param $errorCode
     * @return string
     */
    public static function errorFrom1000To1500($errorCode): string
    {
        switch ($errorCode) {
            case 1201:
                return "The ad group ID is not valid.";
            case 1214:
                return "An attempt was made to create a duplicate of an ad group that already exists.";
            case 1308:
                return "The ad identifier is not valid.";
            case 1325:
                return "The Text property of the text ad exceeds the maximum length.";
            case 1516:
                return "One or more of the bid values exceeds the budget that you specified for the campaign.\n
                 Please verify that the bid values do not exceed the daily budget and 
                 does not exceed the calculated monthly budget";
            default:
                return "";
        }
    }

    /**
     * @param $errorCode
     * @return string
     */
    public static function errorFrom5000To5500($errorCode): string
    {
        switch ($errorCode) {
            case 5016:
                return "The title part2 is invalid.";
            default:
                return "";
        }
    }
}