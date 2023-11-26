<?php

namespace App\Extensions\Common;

/**
 * Class ContentType
 * @package KC\DataBundle\Extensions\Common
 */
final class ContentType
{
    /**
     *
     */
    const ADGROUP = "Adgroup";

    /**
     *
     */
    const KEYWORD = "Keyword";

    /**
     *
     */
    const AD = "Ad";

    /**
     *
     */
    const EXTENSION = "Extension";

    /**
     *
     */
    const CONTENT_TYPES = array(
        self::EXTENSION,
        self::ADGROUP,
        self::AD,
        self::KEYWORD
    );

    const BRAND_TEMPLATE_CONTENT_TYPES = [
        self::EXTENSION,
        self::ADGROUP,
        self::KEYWORD
    ];

    /**
     * list entity for product
     */
    const PRODUCT_TEMPLATE_CONTENT_TYPES = [
        self::EXTENSION,
        self::ADGROUP,
        self::KEYWORD
    ];

}