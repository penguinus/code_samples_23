<?php

namespace App\Extensions;

use Psr\Container\ContainerInterface;
use Symfony\Component\DependencyInjection\ParameterBag\ContainerBagInterface;

/**
 * Class ServiceManager
 * @package App\Extensions
 */
final class ServiceManager
{
    /**
     * @var ContainerBagInterface
     */
    protected $container;

    /**
     * ServiceManager constructor.
     * @param $container
     */
    public function __construct(ContainerInterface $container)
    {
        $this->container = $container;
    }

    /**
     * @param $id
     * @return mixed
     */
    protected function get($id)
    {
        return $this->container->get($id);
    }

    /**
     * @param $adSystem
     * @param $service
     * @return mixed
     */
    public function getService($adSystem, $service)
    {
        $serviceName = $this->getServiceName($adSystem, $service);
        
        return $this->get($serviceName);
    }

    /**
     * @param $adSystem
     * @param $service
     * @return string
     */
    public function getServiceName($adSystem, $service)
    {
        return strtolower($adSystem).".".strtolower($service);
    }
}