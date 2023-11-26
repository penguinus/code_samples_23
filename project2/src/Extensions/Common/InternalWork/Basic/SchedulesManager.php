<?php

namespace App\Extensions\Common\InternalWork\Basic;

use App\Document\CampaignProcess;
use App\Document\KcCampaign as MongoKcCampaign;
use App\Entity\KcCampaign as MysqlKcCampaign;
use App\Entity\TimeRange;
use App\Extensions\Common\ExternalWork\SchedulesInterface;
use App\Extensions\Common\InternalWork\Interfaces\SchedulesManagerInterface;
use App\Extensions\Common\ServiceEnum;
use App\Extensions\ServiceManager;
use App\Providers\ProviderEntityName;
use Doctrine\ODM\MongoDB\DocumentManager;
use Doctrine\ORM\EntityManagerInterface;
use Monolog\Logger;
use Psr\Container\ContainerInterface;
use Symfony\Contracts\Service\ServiceSubscriberInterface;

/**
 * Class SchedulesManager
 *
 * @package App\Extensions\Common\InternalWork\Basic
 */
abstract class SchedulesManager implements SchedulesManagerInterface, ServiceSubscriberInterface
{
    /**
     * @var array
     */
    private $_schedules = [];

    /**
     * @var ContainerInterface
     */
    private $container;

    /**
     * @var string
     */
    protected $adSystem;

    /**
     * @var DocumentManager
     */
    private $dm;

    /**
     * @var EntityManagerInterface
     */
    private $em;

    /**
     * SchedulesManager constructor.
     * @param string                    $adSystem
     * @param ContainerInterface        $container
     * @param EntityManagerInterface    $em
     * @param DocumentManager           $dm
     */
    public function __construct(
        string                  $adSystem,
        ContainerInterface      $container,
        EntityManagerInterface  $em,
        DocumentManager         $dm
    ) {

        $this->container    = $container;
        $this->adSystem     = strtolower($adSystem);

        $this->em = $em;

        $dm->setAdSystem($this->adSystem);
        $this->dm = $dm;
    }

    /**
     * @return array
     */
    public static function getSubscribedServices()
    {
        return [
            'service.manager' => ServiceManager::class
        ];
    }

    /**
     * @param string $id
     *
     * @return mixed|object
     */
    protected function get($id)
    {
        return $this->container->get($id);
    }

    /**
     * @return EntityManagerInterface
     */
    protected function getEntityManager(): EntityManagerInterface
    {
        return $this->em;
    }

    /**
     * @return DocumentManager
     */
    protected function getDocumentManager(): DocumentManager
    {
        return $this->dm;
    }

    /**
     * @return array
     */
    abstract public function getDayNamesForSchedule(): array;

    /**
     * @return array
     */
    abstract public function getMinuteNamesForSchedule(): array;

    /**
     * @param int           $hour
     * @param string|null   $timeZone
     *
     * @return int
     */
    abstract public function getHourAfterModifyByTimeZone(int $hour, ?string $timeZone = null): int;


    /**
     * @return Logger
     */
    abstract protected function getLogger(): Logger;

    /**
     * @param EntityManagerInterface    $em
     * @param int                       $backendId
     * @param array                     $days
     */
    public static function createSchedules(EntityManagerInterface $em, int $backendId, array $days)
    {
        $savedDays = $em->getRepository(TimeRange::class)->findBy(['campaignBackendId' => $backendId]);

        if (empty($savedDays) && isset($days)) {

            foreach ($days as $item) {
                $day = new TimeRange();
                $day->setWeekDay($item['day']);
                $day->setStartTime($item['from']);
                $day->setEndTime($item['to']);
                $day->setCampaignBackendId($backendId);

                $em->persist($day);
                $em->flush();
            }
        }
    }

    /**
     * Upload schedules to ad system
     */
    public function uploadSchedules()
    {
        /** @var \Doctrine\ORM\EntityManager $em */
        $em = $this->getEntityManager();
        $dm = $this->getDocumentManager();

        gc_enable();

        $backendIds = $dm->createQueryBuilder(MongoKcCampaign::class)
            ->hydrate(false)
            ->select('backendId')
            ->field('schedulesUpdate')->equals(true)
            ->field('campaigns')->exists(true)
            ->getQuery()->toArray();

        $backendIds = array_column($backendIds, 'backendId');

        foreach ($backendIds as $backendId) {

            if (!$this->kcCampaignUploaded($backendId)) {
                continue;
            }

            $kcCampaignInMongo = $dm->createQueryBuilder(MongoKcCampaign::class)
                ->hydrate(false)
                ->select('campaigns.id', 'campaigns.systemCampaignId', 'campaigns.systemAccount')
                ->field('backendId')->equals($backendId)
                ->getQuery()->getSingleResult();

            if (isset($kcCampaignInMongo['campaigns'])) {
                $campaigns = $kcCampaignInMongo['campaigns'];

                $timezone = $em->getRepository(MysqlKcCampaign::class)
                    ->getTimeZoneByBackendId($backendId);

                $schedule = $this->getScheduleNew($backendId, $timezone);

                foreach ($campaigns as $campaign) {
                    if (isset($campaign['systemCampaignId'])) {
                        $this->uploadSchedule($campaign, $schedule, $this->adSystem);
                    }
                }

                $dm->createQueryBuilder(MongoKcCampaign::class)
                    ->updateMany()
                    ->field('backendId')->equals($backendId)
                    ->field('schedulesUpdate')->unsetField()->exists(true)
                    ->getQuery()->execute();

            }
        }
    }

    /**
     * @param $backendId
     * @return bool
     * @throws \Doctrine\ODM\MongoDB\MongoDBException
     */
    protected function kcCampaignUploaded($backendId): bool
    {
        $dm = $this->getDocumentManager();

        $campaignUploadingYet = (bool)$dm->createQueryBuilder(CampaignProcess::class)
            ->count()
            ->field('backendId')->equals($backendId)
            ->field('add')->exists(true)
            ->field('error')->exists(false)
            ->getQuery()->execute();

        return $campaignUploadingYet ? false : true;
    }

    /**
     * @param int       $backendId
     * @param string    $timezone
     *
     * @return array
     */
    public function getScheduleNew(int $backendId, ?string $timezone = null)
    {
        $em = $this->getEntityManager();

        /** @var TimeRange[] $days */
        $days = $em->getRepository(TimeRange::class)->findBy(['campaignBackendId' => $backendId], ['weekDay' => 'ASC']);

        $dayNames       = $this->getDayNamesForSchedule();
        $minuteNames    = $this->getMinuteNamesForSchedule();

        foreach ($days as $key => $day) {
            $startTimeHour      = $this->getHour($day->getStartTime(), false);
            $endTimeHour        = $this->getHour($day->getEndTime(), true);
            $startTimeHour      = $this->getHourAfterModifyByTimeZone($startTimeHour, $timezone);
            $endTimeHour        = $this->getHourAfterModifyByTimeZone($endTimeHour, $timezone);
            $startTimeMinute    = $this->getMinute($day->getStartTime());
            $endTimeMinute      = $this->getMinute($day->getEndTime());

            if ($this->checkNeedNewDayOrNot($startTimeHour, $endTimeHour)) {
                if ($startTimeHour < 24) {
                    //Разрыв графика с переносом на след. день
                    $dayCurrent = $day->getWeekDay();

                    $this->_schedules[] = [
                        'dayOfWeek'         => $dayNames[$dayCurrent],
                        'startTimeHour'     => $startTimeHour,
                        'startTimeMinute'   => $minuteNames[$startTimeMinute],
                        'endTimeHour'       => 24,
                        'endTimeMinute'     => $minuteNames[$endTimeMinute],
                        'startValue'        => $startTimeHour * 100 + (int)$startTimeMinute,
                        'endValue'          => 24 * 100 + (int)$endTimeMinute,
                    ];

                    $dayCurrent = $dayCurrent + 1;
                    if ($dayCurrent == 8) $dayCurrent = 1;

                    $endTimeHour = $this->getDiffHour($endTimeHour);

                    $this->_schedules[] = [
                        'dayOfWeek'         => $dayNames[$dayCurrent],
                        'startTimeHour'     => '00',
                        'startTimeMinute'   => $minuteNames[$startTimeMinute],
                        'endTimeHour'       => $endTimeHour,
                        'endTimeMinute'     => $minuteNames[$endTimeMinute],
                        'startValue'        => 00 * 100 + (int)$startTimeMinute,
                        'endValue'          => $endTimeHour * 100 + (int)$endTimeMinute,
                    ];
                } elseif ($startTimeHour >= 24 && $endTimeHour > 24) {
                    // перенос на след. день

                    $startTimeHour  = $this->getDiffHour($startTimeHour);
                    $endTimeHour    = $this->getDiffHour($endTimeHour);

                    $dayCurrent = $day->getWeekDay();
                    $dayCurrent = $dayCurrent + 1;

                    if ($dayCurrent == 8) $dayCurrent = 1;

                    $this->_schedules[] = [
                        'dayOfWeek'         => $dayNames[$dayCurrent],
                        'startTimeHour'     => $startTimeHour,
                        'startTimeMinute'   => $minuteNames[$startTimeMinute],
                        'endTimeHour'       => $endTimeHour,
                        'endTimeMinute'     => $minuteNames[$endTimeMinute],
                        'startValue'        => $startTimeHour * 100 + (int)$startTimeMinute,
                        'endValue'          => $endTimeHour * 100 + (int)$endTimeMinute,
                    ];
                }

            } elseif ($this->checkYesterDay($startTimeHour, $endTimeHour)) {
                if ($startTimeHour < 0 && $endTimeHour <= 0) {
                    $diffStartHour      = 24 - (-1 * $startTimeHour);
                    $diffEndTimeHour    = 24 - (-1 * $endTimeHour);
                    $endTimeHour        = $endTimeHour == 0 ? 24 : $diffEndTimeHour;
                    $dayCurrent         = $day->getWeekDay();
                    $dayCurrent         = $dayCurrent - 1;

                    if ($dayCurrent == 0) $dayCurrent = 7;

                    $this->_schedules[] = [
                        'dayOfWeek'         => $dayNames[$dayCurrent],
                        'startTimeHour'     => $diffStartHour,
                        'startTimeMinute'   => $minuteNames[$startTimeMinute],
                        'endTimeHour'       => $endTimeHour,
                        'endTimeMinute'     => $minuteNames[$endTimeMinute],
                        'startValue'        => $diffStartHour * 100 + (int)$startTimeMinute,
                        'endValue'          => $endTimeHour * 100 + (int)$endTimeMinute,
                    ];

                } else {
                    $dayCurrent = $day->getWeekDay();
                    $dayCurrent = $dayCurrent - 1;

                    if ($dayCurrent == 0) $dayCurrent = 7;

                    $diffStartHour = 24 - (-1 * $startTimeHour);

                    $this->_schedules[] = [
                        'dayOfWeek'         => $dayNames[$dayCurrent],
                        'startTimeHour'     => $diffStartHour,
                        'startTimeMinute'   => $minuteNames[$startTimeMinute],
                        'endTimeHour'       => 24,
                        'endTimeMinute'     => $minuteNames[$endTimeMinute],
                        'startValue'        => $diffStartHour * 100 + (int)$startTimeMinute,
                        'endValue'          => 24 * 100 + (int)$endTimeMinute,
                    ];

                    $diffStartHour = "00";

                    $this->_schedules[] = [
                        'dayOfWeek'         => $dayNames[$day->getWeekDay()],
                        'startTimeHour'     => $diffStartHour,
                        'startTimeMinute'   => $minuteNames[$startTimeMinute],
                        'endTimeHour'       => $endTimeHour,
                        'endTimeMinute'     => $minuteNames[$endTimeMinute],
                        'startValue'        => $diffStartHour * 100 + (int)$startTimeMinute,
                        'endValue'          => $endTimeHour * 100 + (int)$endTimeMinute,
                    ];
                }

            } else {

                if ($startTimeHour < 24 && $endTimeHour <= 24) {
                    $endTimeHour    = $endTimeHour === "00" ? 24 : $endTimeHour;
                    $startTimeHour  = $startTimeHour === "00" ? 00 : $startTimeHour;

                    $this->_schedules[] = [
                        'dayOfWeek'         => $dayNames[$day->getWeekDay()],
                        'startTimeHour'     => $startTimeHour,
                        'startTimeMinute'   => $minuteNames[$startTimeMinute],
                        'endTimeHour'       => $endTimeHour,
                        'endTimeMinute'     => $minuteNames[$endTimeMinute],
                        'startValue'        => $startTimeHour * 100 + (int)$startTimeMinute,
                        'endValue'          => $endTimeHour * 100 + (int)$endTimeMinute,
                    ];
                }
            }
        }

        return $this->_schedules;
    }

    /**
     * @param $hour
     * @return int
     */
    private function getDiffHour($hour)
    {
        return ($hour >= 24) ? $hour - 24 : $hour;
    }

    /**
     * @param $startTimeHour
     * @param $endTimeHour
     * @return bool
     */
    private function checkYesterDay($startTimeHour, $endTimeHour)
    {
        if ($startTimeHour < 0) {
            return true;
        }

        return false;
    }

    /**
     * @param $startTimeHour
     * @param $endTimeHour
     * @return bool
     */
    private function checkNeedNewDayOrNot($startTimeHour, $endTimeHour)
    {
        if ($startTimeHour >= 24 && $endTimeHour > 24) {
            return true;
        } elseif (($startTimeHour > 0 && $startTimeHour <= 24) && $endTimeHour <= 24) {
            return false;
        } elseif (($startTimeHour > 0 && $startTimeHour < 24) && $endTimeHour > 24) {
            return true;
        }

        return false;
    }

    /**
     * @param $hour
     * @return false|string
     */
    private function getHour($hour, $isFinish)
    {
        if ($hour === '12:00 AM' && $isFinish) {
            return 24;
        }

        return date('H', strtotime($hour));
    }

    /**
     * @param $minute
     * @return false|string
     */
    private function getMinute($minute)
    {
        return substr($minute, 3, 2);
    }

    /**
     * @param array     $campaign
     * @param array     $schedules
     * @param string    $adSystem
     */
    protected function uploadSchedule(array $campaign, array $schedules, string $adSystem)
    {
        $em = $this->getEntityManager();

        $account = $campaign['systemAccount'];
        $repositoryName = ProviderEntityName::getForAccountsBySystem($adSystem);

        $account = $em->getRepository($repositoryName)->createQueryBuilder('g')
            ->select('g.systemAccountId')->where("g.id = $account")
            ->getQuery()->getSingleResult();

        $clientCustomerId = $account['systemAccountId'];
        $systemCampaignId = $campaign['systemCampaignId'];

        $serviceManager = $this->get('service.manager');
        /** @var SchedulesInterface $systemSchedulesManager */
        $systemSchedulesManager = $serviceManager->getService($adSystem, ServiceEnum::SCHEDULES);

        // Get Uploaded Time Ranges from System
        $uploadedSchedules = $systemSchedulesManager->getUploadedSchedules($systemCampaignId, $clientCustomerId);

        $currentSchedules = [];
        foreach ($schedules as $scheduleData) {
            $currentSchedules[]
                = $scheduleData['dayOfWeek']
                . intval($scheduleData['startTimeHour'])
                . $scheduleData['startTimeMinute']
                . intval($scheduleData['endTimeHour'])
                . $scheduleData['endTimeMinute']
            ;
        }

        if (array_diff($currentSchedules, $uploadedSchedules) || array_diff($uploadedSchedules, $currentSchedules)) {
            $this->getLogger()->info("Syncing schedule for campaign", [(string)$campaign['_id']]);

            // Remove old schedules
            $systemSchedulesManager->deleteSchedules($clientCustomerId, $systemCampaignId, array_keys($uploadedSchedules));

            // Add new schedules
            $systemSchedulesManager->createSchedules($clientCustomerId, $systemCampaignId, $schedules);
        }
    }

    /**
     * @param int $backendId
     * @param string $timezone
     *
     * @return array $schedules
     */
    public function getSchedule(int $backendId, string $timezone): array
    {
        $em = $this->getEntityManager();

        /** @var TimeRange[] $days */
        $days = $em->getRepository('DataBundle:TimeRange')
            ->findBy(array('campaignBackendId' => $backendId), array('weekDay' => 'ASC'));

        $dayNames       = $this->getDayNamesForSchedule();
        $minuteNames    = $this->getMinuteNamesForSchedule();

        $schedules = [];
        foreach ($days as $day) {
            $startTime          = strtotime($day->getStartTime());
            $endTime            = strtotime($day->getEndTime());
            $startTimeHour      = date('H', $startTime);
            $endTimeHour        = date('H', $endTime);
            $startTimeMinute    = substr($day->getStartTime(), 3, 2);   //date('i', $startTime);
            $endTimeMinute      = substr($day->getEndTime(), 3, 2);     //date('i', $endTime);

            $startTimeHour  = $this->getHourAfterModifyByTimeZone($startTimeHour, $timezone);
            $endTimeHour    = $this->getHourAfterModifyByTimeZone($endTimeHour, $timezone);

            $otherDay               = false;
            $otherDayNum            = 0;
            $otherDayStartHour      = 0;
            $otherDayEndHour        = 0;
            $otherDayStartMinute    = '00';
            $otherDayEndMinute      = $endTimeMinute;

            if ($startTimeHour > 24 || ($startTimeHour == 24 && $startTimeMinute != '00')) {
                $dif                = $startTimeHour - 24;
                $otherDayStartHour  = $dif;
                $startTimeHour      = 0;
                $startTimeMinute    = '00';
                $otherDayNum        = $day->getWeekDay() + 1;

                if ($otherDayNum == 8) $otherDayNum = 1;

                $otherDay = true;
            }

            if ($endTimeHour > 24 || ($endTimeHour == 24 && $endTimeMinute != '00')) {
                $dif                = $endTimeHour - 24;
                $otherDayEndHour    = $dif;
                $endTimeHour        = 24;
                $endTimeMinute      = '00';
                $otherDayNum        = $day->getWeekDay() + 1;

                if ($otherDayNum == 8) $otherDayNum = 1;

                $otherDay = true;
            }

            if ($startTimeHour == $endTimeHour && $startTimeMinute == $endTimeMinute) {

                $endTimeHour    = 24;
                $endTimeMinute  = '00';

                if (!(($startTimeHour == 0 && $startTimeMinute == '00'))) {
                    $otherDay               = true;
                    $otherDayStartHour      = 0;
                    $otherDayStartMinute    = '00';
                    $otherDayEndHour        = $endTimeHour;
                    $otherDayEndMinute      = $endTimeMinute;
                    $otherDayNum            = $day->getWeekDay() + 1;

                    if ($otherDayNum == 8) $otherDayNum = 1;
                }
            }

            if ($startTimeHour > $endTimeHour) {
                $otherDayStartHour      = 0;
                $otherDayStartMinute    = '00';
                $otherDayEndHour        = $endTimeHour;
                $otherDayEndMinute      = $endTimeMinute;

                $otherDayStartHour  = 0;
                $endTimeHour        = 24;
                $endTimeMinute      = '00';
                $otherDayNum        = $day->getWeekDay() + 1;

                if ($otherDayNum == 8) $otherDayNum = 1;

                $otherDay = true;
            }

            if ($startTimeHour < 0) {
                $otherDayStartHour  = 24 + $startTimeHour;
                $startTimeHour      = 0;
                $startTimeMinute    = '00';
                $otherDayNum        = $day->getWeekDay() - 1;

                if ($otherDayNum == 0) $otherDayNum = 7;

                $otherDay = true;

                if ($endTimeHour < 0) {
                    $otherDayEndHour    = 24 + $endTimeHour;
                    $endTimeHour        = 0;
                    $endTimeMinute      = '00';
                } else {
                    $otherDayEndHour    = 24;
                    $otherDayEndMinute  = '00';
                }
            }

            $schedules[] = [
                'dayOfWeek'         => $dayNames[$day->getWeekDay()],
                'startTimeHour'     => $startTimeHour,
                'startTimeMinute'   => $minuteNames[$startTimeMinute],
                'endTimeHour'       => $endTimeHour,
                'endTimeMinute'     => $minuteNames[$endTimeMinute],
                'startValue'        => $startTimeHour * 100 + (int)$startTimeMinute,
                'endValue'          => $endTimeHour * 100 + (int)$endTimeMinute,
            ];

            if ($otherDay) {
                $schedules[] = [
                    'dayOfWeek'         => $dayNames[$otherDayNum],
                    'startTimeHour'     => $otherDayStartHour,
                    'startTimeMinute'   => $minuteNames[$otherDayStartMinute],
                    'endTimeHour'       => $otherDayEndHour,
                    'endTimeMinute'     => $minuteNames[$otherDayEndMinute],
                    'startValue'        => $startTimeHour * 100 + (int)$startTimeMinute,
                    'endValue'          => $endTimeHour * 100 + (int)$endTimeMinute,
                ];
            }
        }

        $notReady = true;
        while ($notReady) {
            $notReady = false;

            foreach ($schedules as $key1 => $schedul1) {
                foreach ($schedules as $key2 => $schedul2) {
                    if (($key1 != $key2) && ($schedul1['dayOfWeek'] == $schedul2['dayOfWeek'])) {
                        if (($schedul1['startValue'] >= $schedul2['startValue']) && ($schedul1['startValue'] <= $schedul2['endValue'])) {
                            $schedules[] = [
                                'dayOfWeek' => $schedul2['dayOfWeek'],
                                'startTimeHour' => $schedul2['startTimeHour'],
                                'startTimeMinute' => $schedul2['startTimeMinute'],
                                'endTimeHour' => $schedul1['endValue'] > $schedul2['endValue'] ? $schedul1['endTimeHour'] : $schedul2['endTimeHour'],
                                'endTimeMinute' => $schedul1['endValue'] > $schedul2['endValue'] ? $schedul1['endTimeMinute'] : $schedul2['endTimeMinute'],
                                'startValue' => $schedul2['startValue'],
                                'endValue' => $schedul1['endValue'] > $schedul2['endValue'] ? $schedul1['endValue'] : $schedul2['endValue'],
                            ];

                            unset($schedules[$key1]);
                            unset($schedules[$key2]);

                            $notReady = true;

                            break 2;
                        }
                    }
                }
            }
        }

        return $schedules;
    }
}