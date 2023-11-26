<?php

namespace App\Extensions\Common\ExternalWork;


/**
 * Interface ErrorDetailInterface
 * @package KC\DataBundle\Extensions\Common\ExternalWork
 */
interface ErrorDetailInterface
{
    /**
     * @param string|int $error
     * @return string
    */
    public static function errorDetail($error): string;
}
