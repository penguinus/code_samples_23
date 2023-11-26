<?php

namespace App\Extensions\Common\InternalWork\Interfaces;



use Doctrine\ORM\EntityManagerInterface;

interface SchedulesManagerInterface
{
    /**
     * @param EntityManagerInterface $em
     * @param int $backendId
     * @param array $days
     */
    public static function createSchedules(EntityManagerInterface $em, int $backendId, array $days);

     /**
     * Upload schedules to ad system
     */
    public function uploadSchedules();

    /**
     * @param int  $backendId
     * @param string $timezone
     *
     * @return array $schedules
     */
    public function getSchedule(int $backendId, string $timezone): array;

}
