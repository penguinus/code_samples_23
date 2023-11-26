<?php

namespace App\Extensions\AdSystem\Bing\ExternalWork\Exception;


class RefreshTokenExpiredException extends \Exception
{
    public function errorMessage()
    {
        //error message
        $errorMsg = $this->getMessage().' Bing Authentication Refresh Token has Expired!';

        return $errorMsg;
    }
}