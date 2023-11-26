<?php

namespace App\Extensions\AdSystem\AdWords\InternalWork;

use App\Extensions\Common\AdSystemEnum;
use App\Extensions\Common\InternalWork\Basic\SchedulesManager;
use Doctrine\ODM\MongoDB\DocumentManager;
use Doctrine\ORM\EntityManagerInterface;
use Google\Ads\GoogleAds\V13\Enums\DayOfWeekEnum\DayOfWeek;
use Google\Ads\GoogleAds\V13\Enums\MinuteOfHourEnum\MinuteOfHour;
use Monolog\Logger;
use Psr\Container\ContainerInterface;
use Psr\Log\LoggerInterface;

/**
 * Class AdWordsSchedulesManager
 *
 * @package App\Extensions\AdSystem\AdWords\InternalWork
 */
class AdWordsSchedulesManager extends SchedulesManager
{
    /**
     * AdWordsSchedulesManager constructor.
     *
     * @param ContainerInterface        $container
     * @param EntityManagerInterface    $em
     * @param DocumentManager           $dm
     */
    public function __construct(ContainerInterface $container, EntityManagerInterface $em, DocumentManager $dm)
    {
        parent::__construct(AdSystemEnum::ADWORDS, $container, $em, $dm);
    }

    /**
     * @return array
     */
    public static function getSubscribedServices(): array
    {
        return parent::getSubscribedServices() + [
                'monolog.logger.adwords_sync' => LoggerInterface::class
            ];
    }

    /**
     * @return Logger
     */
    protected function getLogger(): Logger
    {
        return $this->get('monolog.logger.adwords_sync');
    }

    /**
     * @return array
     */
    public function getDayNamesForSchedule(): array
    {
        $dayNames = [];
        $dayNames[0] = '';
        $dayNames[1] = DayOfWeek::MONDAY;
        $dayNames[2] = DayOfWeek::TUESDAY;
        $dayNames[3] = DayOfWeek::WEDNESDAY;
        $dayNames[4] = DayOfWeek::THURSDAY;
        $dayNames[5] = DayOfWeek::FRIDAY;
        $dayNames[6] = DayOfWeek::SATURDAY;
        $dayNames[7] = DayOfWeek::SUNDAY;

        return $dayNames;
    }

    /**
     * @return array
     */
    public function getMinuteNamesForSchedule(): array
    {
        return [
            '00' => MinuteOfHour::ZERO,
            '15' => MinuteOfHour::FIFTEEN,
            '30' => MinuteOfHour::THIRTY,
            '45' => MinuteOfHour::FORTY_FIVE,
            '59' => MinuteOfHour::FORTY_FIVE
        ];
    }

    /**
     * @param int           $hour
     * @param string|null   $timeZone
     *
     * @return int
     */
    public function getHourAfterModifyByTimeZone(int $hour, ?string $timeZone = null): int
    {
        if (is_null($timeZone)) {
            return $hour;
        }

        switch ($timeZone) {
            case 'CDT':
                return $hour + 1;
            case 'MDT':
                return $hour + 2;
            case 'AKDT':
                return $hour + 4;
            case 'MST':
            case 'PDT':
                return $hour + 3;
            case 'ADT':
                return $hour - 1;
            default:
                return $hour;
        }
    }
}