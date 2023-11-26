<?php

namespace App\Extensions\Common;

/**
 * Class TemplateType
 * @package App\Extensions\Common
 */
final class TemplateType
{
    /**
     *
     */
    const PRODUCT = "Product";

    /**
     *
     */
    const BRAND = "Brand";

    /**
     *
     */
    const AD = "Ad";

    /**
     *
     */
    const TEMPLATE_TYPES = [
        self::PRODUCT,
        self::BRAND,
        self::AD,
    ];
}