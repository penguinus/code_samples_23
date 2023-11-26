<?php

namespace App\Extensions\Common\ExternalWork;


/**
 * Interface KeywordInterface
 *
 * @package App\Extensions\Common\ExternalWork
 */
interface KeywordInterface
{
    /**
     * @param array $parentIds
     * @param int   $accountId
     * @param int   $backendId
     * @param int   $brandTemplateId
     * @param bool  $campaignCriterion
     *
     * @return bool
     */
    public function findByParentIdsAndRemove(
        array   $parentIds,
        int     $accountId,
        int     $backendId,
        int     $brandTemplateId,
        bool    $campaignCriterion = false
    ): bool;


}