<?php

namespace App\Extensions\Common\ExternalWork\Core;

use App\Extensions\Common\ExternalWork\AdInterface;

/**
 * Class AdAbstract
 *
 * @package App\Extensions\Common\ExternalWork\Core
 */
abstract class AdAbstract implements AdInterface
{

    public const QUEUE_HEADLINE = 'headline';

    public const QUEUE_DESCRIPTION = 'desc';

    public const QUEUE_PATH = 'path';

    public const RSA_HEADLINES_LIMIT = 15;

    public const RSA_DESCRIPTIONS_LIMIT = 4;

    public const RSA_HEADLINE_LENGTH_LIMIT = 30;

    public const RSA_DESCRIPTION_LENGTH_LIMIT = 90;

    /**
     * @return mixed
     */
    abstract public static function getRSAHeadLinePinFieldsMap(): array;

    /**
     * @return mixed
     */
    abstract public static function getRSAQueueDescriptionPinFieldsMap(): array;

    /**
     * @param string $field
     *
     * @return int
     * @throws \Exception
     */
    public function getRSAFieldsLimit(string $field): int
    {
        switch ($field) {
            case self::QUEUE_HEADLINE:
                return static::RSA_HEADLINES_LIMIT;
            case self::QUEUE_DESCRIPTION:
                return static::RSA_DESCRIPTIONS_LIMIT;
            default:
                throw new \Exception("Unknown field name.");
        }
    }

    /**
     * @param string $field
     *
     * @return int
     * @throws \Exception
     */
    public static function getRSAFieldLengthLimit(string $field): int
    {
        switch ($field) {
            case strpos($field, self::QUEUE_HEADLINE) !== false:
                return static::RSA_HEADLINE_LENGTH_LIMIT;
            case strpos($field, self::QUEUE_DESCRIPTION) !== false:
                return static::RSA_DESCRIPTION_LENGTH_LIMIT;
            default:
                throw new \Exception("Unknown field name.");
        }
    }

    /**
     * @param string $field
     *
     * @return array
     * @throws \Exception
     */
    public static function getRSAQueuePinFieldsMap(string $field): array
    {
        switch ($field) {
            case self::QUEUE_HEADLINE:
                return static::getRSAHeadLinePinFieldsMap();
            case self::QUEUE_DESCRIPTION:
                return static::getRSAQueueDescriptionPinFieldsMap();
            default:
                throw new \Exception("Unknown field name.");
        }
    }

    /**
     * @param array         $queueAd
     * @param array         $placeholders
     * @param array|null    $abbreviation
     *
     * @return array
     * @throws \Exception
     */
    protected function processingRSAFields(array $queueAd, array $placeholders, ?array $abbreviation = null): array
    {
        $headlines = $this->makeRSAFields(self::QUEUE_HEADLINE, $queueAd, $placeholders, $abbreviation);

        $descriptions = $this->makeRSAFields(self::QUEUE_DESCRIPTION, $queueAd, $placeholders, $abbreviation);

        $typesAdRSA['headlines'] = $headlines;
        $typesAdRSA['descriptions'] = $descriptions;

        return $typesAdRSA;
    }

    /**
     * @param string        $field
     * @param array         $queueEntity
     * @param array         $placeholders
     * @param array|null    $abbreviation
     *
     * @return array
     * @throws \Exception
     */
    private function makeRSAFields(
        string  $field,
        array   $queueEntity,
        array   $placeholders,
        ?array  $abbreviation = null
    ): array
    {
        $fields = [];
        foreach (range(0, self::getRSAFieldsLimit($field) - 1) as $value) {
            $queueField = "{$field}{$value}";

            if (!key_exists($queueField, $queueEntity) || empty($queueEntity[$queueField]))
                continue;

            $lengthLimit = self::getRSAFieldLengthLimit($field);

            // replacements field in which there are placeholders
            $queueEntity[$queueField] = self::placeholdersReplacement(
                $queueEntity[$queueField], $placeholders, $abbreviation, $lengthLimit);

            $filedAssetLink = [];
            $filedAssetLink['text'] = $queueEntity[$queueField];

            foreach (self::getRSAQueuePinFieldsMap($field) as $queueItemField => $pinnedInSystem)
                if (in_array($value, $queueEntity[$queueItemField])) {
                    $filedAssetLink['pinnedField'] = $pinnedInSystem;
                    break;
                }


            $fields[] = $filedAssetLink;
        }

        return $fields;
    }

    /**
     * $placeholders contains placeholders for replacements in brand, offer, ad group, city, state
     * where key in array $placeholders is defines the replacement field in the headline
     *
     * @param string        $fieldValue
     * @param array         $placeholders
     * @param array|null    $abbreviation
     * @param int           $lengthLimit
     *
     * @return string|string[]
     */
    public static function placeholdersReplacement(
        string  $fieldValue,
        array   $placeholders,
        ?array  $abbreviation = null,
        int     $lengthLimit = 30
    ) {
        if (!preg_match('/\[city\]|\[brand\]|\[adgroup\]|\[offer\]/', stripcslashes($fieldValue)))
            return $fieldValue;

        foreach (['brand', 'adgroup', 'offer'] as $type) {
            if (strpos($fieldValue, '[' . $type . ']') !== false) {
                $placeholderValue = $placeholders[$type] ?? '';
                $fieldValue = str_replace('[' . $type . ']', $placeholderValue, $fieldValue);

                if (strlen($fieldValue) > $lengthLimit)
                    $fieldValue = str_replace($placeholderValue, '', $fieldValue);
            }
        }

        $cleanValueLength = strlen(str_replace("[city]", "", $fieldValue));
        $cityStates = self::getCityStates($placeholders, $abbreviation);

        $replaceValue = '';
        foreach ($cityStates as $cityState) {
            if ($cleanValueLength + strlen($cityState) <= $lengthLimit) {
                $replaceValue = $cityState;

                break;
            }
        }

        return str_replace("[city]", $replaceValue, $fieldValue);
    }


    /**
     * @param array         $placeholders
     * @param array|null    $abbreviation
     *
     * @return \Generator
     */
    private static function getCityStates(array $placeholders, ?array $abbreviation = null): \Generator
    {
        // Order city-state should from the biggest string length to least.
        $cityStates[] = $placeholders['city'] . ', ' . $placeholders['state'];
        $cityStates[] = $placeholders['city'] . ' ' . $placeholders['state'];
        $cityStates[] = $placeholders['city'];

        if (!empty($abbreviation)) {
            foreach ($abbreviation as $key => $abbr)
                if (strripos($key, 'abbr') !== false) {
                    $cityStates[] = $abbr . ' ' . $placeholders['state'];
                    $cityStates[] = $abbr;
                }

            usort($cityStates, function ($a, $b) {
                return strlen($a) < strlen($b);
            });
        }

        $cityStates[] = $placeholders['state'];

        foreach ($cityStates as $cityState)
            yield $cityState;
    }

    /**
     * @param               $path
     * @param               $city
     * @param array|null    $abbreviation
     *
     * @return string|string[]
     */
    public static function pathPlaceholdersReplacement($path, $city, ?array $abbreviation = null)
    {
        if (!(strpos($path, '[city]') === false)) {
            if (strlen($city) <= 15) {
                $path = $city;
            } elseif (!empty($abbreviation['destinationurl']) && strlen($abbreviation['destinationurl']) <= 15) {
                $path = $abbreviation['destinationurl'];
            } else {
                $path = $abbreviation['abbr9'];
            }
        }

        return str_replace([' ', '.'], '_', $path);
    }

    /**
     * @param $url
     * @param $backendId
     * @param $adgroupId
     * @param $channelId
     * @param $cityId
     *
     * @return string
     */
    public static function applyParamsForUrl($url, $backendId, $adgroupId, $channelId, $cityId): string
    {
        /*
           ctid = client ID, it comes from KC backend
           agid = ad group ID (comes when you create the adgroup)
           chid = channel ID: entered at the creation of the brand
           cyid = Ad System City ID, geocode, lookup from KC database from a ZIP Code
       */

        if (empty($url))
            return '';

        $destUrl = str_replace('?ctid={1}&agid={2}&chid={3}&cyid={4}', '', $url .
            '?ctid=' . $backendId .
            '&agid=' . $adgroupId .
            '&chid=' . $channelId .
            '&cyid=' . $cityId);

        if (substr($destUrl, 0, 7) != 'http://' && substr($destUrl, 0, 8) != 'https://')
            $destUrl = 'http://' . $destUrl;

        $destUrl = str_replace(" ", "_", $destUrl);

        return $destUrl;
    }

}