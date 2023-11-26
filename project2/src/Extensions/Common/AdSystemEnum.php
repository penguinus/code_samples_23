<?php


namespace App\Extensions\Common;


/**
 * Class AdSystemEnum
 * @package App\Extensions\Common
 */
final class AdSystemEnum
{
    public const ADWORDS = 'ADWORDS';

    public const BING = 'BING';

    public const AD_SYSTEMS = ['ADWORDS', 'BING'];

    public const AD_SYSTEMS_LOWER = ['adwords', 'bing'];

    public const ALL = self::AD_SYSTEMS;
}