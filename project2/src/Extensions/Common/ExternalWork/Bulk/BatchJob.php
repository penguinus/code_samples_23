<?php

namespace App\Extensions\Common\ExternalWork\Bulk;


/**
 * Class BatchJob
 * @package App\Extensions\Common\ExternalWork\Bulk
 */
abstract class BatchJob
{
    /**
     *
     */
    public const ACTION_ADD = 'ADD';
    /**
     *
     */
    public const ACTION_UPDATE = 'UPDATE';
    /**
     *
     */
    public const ACTION_REMOVE = 'REMOVE';

    /**
     *
     */
    public const OPERAND_TYPE_ADGROUP = 'AdGroup';
    /**
     *
     */
    public const OPERAND_TYPE_KEYWORD = 'Keyword';
    /**
     *
     */
    public const OPERAND_TYPE_AD = 'Ad';
    /**
     *
     */
    public const OPERAND_TYPE_EXTENSION = 'Extension';

    /**
     *
     */
    public const STATUS_PENDING_RESULT = 'PendingResult';
    /**
     *
     */
    public const STATUS_COMPLETE = 'Complete';
    /**
     *
     */
    public const STATUS_PENDING_CANCELLATION = 'PendingCancellation';
    /**
     *
     */
    public const STATUS_CANCELED = 'Canceled';
    /**
     *
     */
    public const STATUS_ERROR = 'Error';
}