<?php

namespace App\Extensions\Common\ExternalWork;


/**
 * Interface AccountInterface
 * @package App\Extensions\Common\ExternalWork
 */
interface AccountInterface
{
    /**
     * @param $clientCustomerId
     * @return int
     */
    public function getAmountKeywords($clientCustomerId): int;

    /**
     * @param string $name
     * @return mixed
     */
    public function create(string $name);

    /**
     * @param string $name
     * @param $systemAccountId
     * @return mixed
     */
    public function update(string $name, $systemAccountId);

    /**
     * @param $id
     * @return mixed
     */
    public function delete($id);

    /**
     * @return array
     */
    public function getAccounts(): array;
}