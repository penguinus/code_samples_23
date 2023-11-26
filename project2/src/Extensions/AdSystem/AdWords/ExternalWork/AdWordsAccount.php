<?php

namespace App\Extensions\AdSystem\AdWords\ExternalWork;

use Airbrake\Notifier;
use App\Extensions\AdSystem\AdWords\ExternalWork\Auth\AdWordsServiceManager;
use App\Extensions\Common\ExternalWork\AccountInterface;
use Google\Ads\GoogleAds\V13\Enums\CustomerStatusEnum\CustomerStatus;
use Google\Ads\GoogleAds\V13\Resources\Customer;
use Google\Ads\GoogleAds\V13\Services\{CustomerServiceClient, GoogleAdsRow};
use Google\ApiCore\ApiException;
use Google\ApiCore\ValidationException;
use Psr\Container\ContainerInterface;
use Psr\Log\LoggerInterface;


/**
 * Class AdWordsAccount
 *
 * @package App\Extensions\AdSystem\AdWords\ExternalWork
 */
class AdWordsAccount implements AccountInterface
{
    /**
     * @var AdWordsServiceManager
     */
    protected AdWordsServiceManager $serviceManager;

    /**
     * @var LoggerInterface
     */
    protected LoggerInterface $logger;

    /**
     * @var ContainerInterface
     */
    protected ContainerInterface $container;

    /**
     * AdWordsAccount constructor.
     *
     * @param ContainerInterface    $container
     * @param AdWordsServiceManager $serviceManager
     * @param LoggerInterface       $logger
     */
    public function __construct(
        ContainerInterface $container,
        AdWordsServiceManager $serviceManager,
        LoggerInterface $logger
    ) {
        $this->container        = $container;
        $this->serviceManager   = $serviceManager;
        $this->logger           = $logger;
    }

    /**
     * @return AdWordsServiceManager
     */
    protected function getGoogleServiceManager(): AdWordsServiceManager
    {
        return $this->serviceManager;
    }

    /**
     * @return LoggerInterface
     */
    public function getLogger() : LoggerInterface
    {
        return $this->logger;
    }

    /**
     * @return Notifier
     */
    protected function getAirbrakeNotifier(): Notifier
    {
        return $this->container->get('ami_airbrake.notifier');
    }

    /**
     * @param $value
     *
     * @return string
     */
    private function getTimezoneName($value): string
    {
        $result = '';
        switch ($value) {
            case -9:
                $result = 'America/Anchorage';
                break;
            case -4:
                $result = 'America/Puerto_Rico';
                break;
            case -6:
                $result = 'America/Chicago';
                break;
            case 10:
                $result = 'Pacific/Guam';
                break;
            case -5:
                $result = 'America/New_York';
                break;
            case -10:
                $result = 'Pacific/Honolulu';
                break;
            case -7:
                $result = 'America/Dawson_Creek';
                break;
            case -8:
                $result = 'America/Los_Angeles';
                break;
            case -11:
                $result = 'Pacific/Midway';
                break;
        }
        return $result;
    }

    /**
     * @param string $name
     *
     * @return mixed
     * @throws ValidationException
     */
    public function create(string $name): ?int
    {
        $newCustomer = new Customer();
        $newCustomer->setDescriptiveName($name);
        // For a list of valid currency codes and time zones see this documentation:
        // https://developers.google.com/google-ads/api/reference/data/codes-formats.
        $newCustomer->setCurrencyCode('USD');
        $newCustomer->setTimeZone($this->getTimezoneName(-5));
        // The below values are optional. For more information about URL
        // options see: https://support.google.com/google-ads/answer/6305348.
        // $customer->setTrackingUrlTemplate('{lpurl}?device={device}');
        // $customer->setFinalUrlSuffix('keyword={keyword}&matchtype={matchtype}&adgroupid={adgroupid}');

        $customerId = null;
        try {
            // Issues a mutate request to create an account
            $customerServiceClient = $this->getGoogleServiceManager()->getCustomerServiceClient();
            $customerClientResponse = $customerServiceClient->createCustomerClient(
                $this->getGoogleServiceManager()->getClientBuilder()->getLoginCustomerId(),
                $newCustomer
            );

            $customerId = CustomerServiceClient::parseName($customerClientResponse->getResourceName())['customer_id'];

        } catch (ApiException $apiException) {
            foreach ($apiException->getMetadata() as $metadatum) {
                foreach ($metadatum['errors'] as $error) {
                    if (!strpos(AdWordsErrorDetail::errorDetail($error["message"]), "internal error")) {
                        $this->getAirbrakeNotifier()->notify(new \Exception(
                            '[Google] ApiException was thrown with message - '
                            . $error["message"] .', when the process was running "create new customer" '. PHP_EOL
                        ));
                    }
                }
            }
        }

        return $customerId ?: false;
    }

    /**
     * @param string    $name
     * @param           $systemAccountId
     *
     * @throws \Exception
     */
    public function update(string $name, $systemAccountId)
    {
        throw new \Exception('Google account isn\'t aggregator');
    }

    /**
     * Note: The REMOVE operator is not supported.
     *
     * @param $id
     *
     * @throws \Exception
     */
    public function delete($id)
    {
        throw new \Exception('Google account isn\'t aggregator');
    }

    /**
     * @param $clientCustomerId
     *
     * @return int
     * @throws ValidationException
     */
    public function getAmountKeywords($clientCustomerId): int
    {
        $googleAdsServiceClient = $this->getGoogleServiceManager()->getGoogleAdsServiceClient();

        $amountKeywords = 0;
        try {
            // Creates a query that retrieves keywords.
            $query = /** @lang text */
                "SELECT ad_group_criterion.criterion_id 
                FROM ad_group_criterion 
                WHERE ad_group_criterion.type = KEYWORD 
                AND ad_group_criterion.status != REMOVED";

            // Issues a search request by specifying page size.
            $adGroupCriterionResponse = $googleAdsServiceClient->search(
                $clientCustomerId,
                $query,
                ['returnTotalResultsCount' => true]
            );

            if (!empty($adGroupCriterionResponse->iterateAllElements())) {
                // total number of results in the response.
                $amountKeywords = $adGroupCriterionResponse->getPage()->getResponseObject()->getTotalResultsCount();
            }

        } catch (ApiException $apiException) {
            foreach ($apiException->getMetadata() as $metadatum) {
                foreach ($metadatum['errors'] as $error) {
                    if (!strpos(AdWordsErrorDetail::errorDetail($error["message"]), "internal error")) {
                        $this->getAirbrakeNotifier()->notify(new \Exception(
                            '[Google] ApiException was thrown with message - '
                            . $error["message"] . ' Customer ID: ' . $clientCustomerId
                            . ', when the process was running "getAmountKeywords" ' . PHP_EOL
                        ));
                    }
                }
            }
        }

        return $amountKeywords;
    }

    /**
     * @return array
     * @throws ValidationException
     */
    public function getAccounts(): array
    {
        // Creates the Google Ads Service client.
        $googleAdsServiceClient = $this->getGoogleServiceManager()->getGoogleAdsServiceClient();

        // Creates a query that retrieves all child accounts of the manager specified in search
        $query = /** @lang text */
            "SELECT customer_client.client_customer, customer_client.id, customer_client.level, customer_client.manager, 
            customer_client.descriptive_name, customer_client.test_account, customer_client.status 
            FROM customer_client 
            WHERE customer_client.level <= 1";

        $customers = [];
        try {
            // Issues a search request by specifying page size.
            $customerClientResponse = $googleAdsServiceClient->search(
                $this->getGoogleServiceManager()->getClientBuilder()->getLoginCustomerId(),
                $query,
                ['pageSize' => $this->getGoogleServiceManager()::PAGE_SIZE]
            );

            // Iterates over all elements to get all customer clients under the specified customer's
            // hierarchy.
            /** @var GoogleAdsRow $googleAdsRow */
            foreach ($customerClientResponse->iterateAllElements() as $googleAdsRow) {
                if ($googleAdsRow->getCustomerClient()->getManager()) {
                    continue;
                }

                $customers[$googleAdsRow->getCustomerClient()->getStatus() == CustomerStatus::ENABLED
                || $googleAdsRow->getCustomerClient()->getTestAccount() ? 'active' : 'inactive'][] = [
                    'customerId'    => $googleAdsRow->getCustomerClient()->getId(),
                    'name'          => $googleAdsRow->getCustomerClient()->getDescriptiveName(),
                    'testAccount'   => $googleAdsRow->getCustomerClient()->getTestAccount(),
                ];
            }

        } catch (ApiException $apiException) {
            foreach ($apiException->getMetadata() as $metadatum) {
                foreach ($metadatum['errors'] as $error) {
                    if (!strpos(AdWordsErrorDetail::errorDetail($error["message"]), "internal error")) {
                        $this->getAirbrakeNotifier()->notify(new \Exception(
                            '[Google] ApiException was thrown with message - '
                            . $error["message"] . ', when the process was running "getAccounts" ' . PHP_EOL
                        ));
                    }
                }
            }
        }

        return $customers;
    }
}