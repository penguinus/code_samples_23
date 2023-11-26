<?php

namespace App\Extensions\AdSystem\Bing\InternalWork;

use App\Extensions\Common\AdSystemEnum;
use App\Extensions\Common\InternalWork\Basic\SchedulesManager;
use Doctrine\ODM\MongoDB\DocumentManager;
use Doctrine\ORM\EntityManagerInterface;
use Microsoft\BingAds\V13\CampaignManagement\Day;
use Microsoft\BingAds\V13\CampaignManagement\Minute;
use Monolog\Logger;
use Psr\Container\ContainerInterface;
use Psr\Log\LoggerInterface;

/**
 * Class BingSchedulesManager
 *
 * @package App\Extensions\AdSystem\Bing\InternalWork
 */
class BingSchedulesManager extends SchedulesManager
{
    /**
     * BingSchedulesManager constructor.
     *
     * @param ContainerInterface        $container
     * @param EntityManagerInterface    $em
     * @param DocumentManager           $dm
     */
    public function __construct(ContainerInterface $container, EntityManagerInterface $em, DocumentManager $dm)
    {
        parent::__construct(AdSystemEnum::BING, $container, $em, $dm);
    }

    /**
     * @return array
     */
    public static function getSubscribedServices(): array
    {
        return parent::getSubscribedServices() + [
            'monolog.logger.bing_sync' => LoggerInterface::class
        ];
    }

    /**
     * @return Logger
     */
    protected function getLogger(): Logger
    {
        return $this->get('monolog.logger.bing_sync');
    }

    /**
     * @return array
     */
    public function getDayNamesForSchedule(): array
    {
        $dayNames = [];
        $dayNames[0] = '';
        $dayNames[1] = Day::Monday;
        $dayNames[2] = Day::Tuesday;
        $dayNames[3] = Day::Wednesday;
        $dayNames[4] = Day::Thursday;
        $dayNames[5] = Day::Friday;
        $dayNames[6] = Day::Saturday;
        $dayNames[7] = Day::Sunday;

        return $dayNames;
    }


    /**
     * @return array
     */
    public function getMinuteNamesForSchedule(): array
    {
        return [
            '00' => Minute::Zero,
            '15' => Minute::Fifteen,
            '30' => Minute::Thirty,
            '45' => Minute::FortyFive,
            '59' => Minute::FortyFive
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
        return $hour;
    }
}