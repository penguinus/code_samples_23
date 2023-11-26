<?php

namespace App\Extensions\Common\InternalWork\Interfaces;

use App\Interfaces\EntityInterface\SystemAccountInterface;

/**
 * Interface AccountManagerInterface
 * @package App\Extensions\Common\InternalWork\Interfaces
 */
interface AccountManagerInterface
{
    /**
     * @param int[] $accounts
     * @param integer $brandTemplateKeywords
     *
     * @return SystemAccountInterface|null
     */
    public function getAvailableAccount(array $accounts, int $brandTemplateKeywords);
}
