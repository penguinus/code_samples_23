<?php

namespace App\Extensions\AdSystem\Bing\ExternalWork;

use Airbrake\Notifier;
use App\Extensions\AdSystem\Bing\ExternalWork\Auth\BingServiceManager;
use App\Extensions\AdSystem\Bing\ExternalWork\Settings\AccountLifeCycleStatus;
use App\Extensions\AdSystem\Bing\ExternalWork\Settings\AddressSettings;
use App\Extensions\AdSystem\Bing\ExternalWork\Settings\AdvertiserAccountSettings;
use App\Extensions\AdSystem\Bing\ExternalWork\Settings\CustomerSettings;
use App\Extensions\Common\ExternalWork\AccountInterface;
use Microsoft\BingAds\V13\CustomerManagement\AddAccountRequest;
use Microsoft\BingAds\V13\CustomerManagement\Address;
use Microsoft\BingAds\V13\CustomerManagement\AdvertiserAccount;
use Microsoft\BingAds\V13\CustomerManagement\Customer;
use Microsoft\BingAds\V13\CustomerManagement\DeleteAccountRequest;
use Microsoft\BingAds\V13\CustomerManagement\GetAccountRequest;
use Microsoft\BingAds\V13\CustomerManagement\GetAccountsInfoRequest;
use Microsoft\BingAds\V13\CustomerManagement\GetUserRequest;
use Microsoft\BingAds\V13\CustomerManagement\LanguageType;
use Microsoft\BingAds\V13\CustomerManagement\SignupCustomerRequest;
use Microsoft\BingAds\V13\CustomerManagement\UpdateAccountRequest;
use Psr\Container\ContainerInterface;

/**
 * Class BingAccount
 * @package App\Extensions\AdSystem\BingAccount\ExternalWork
 */
class BingAccount implements AccountInterface
{
    protected $container;

    /**
     * BingAccount constructor.
     * @param ContainerInterface $container
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
     * @return Notifier
     */
    protected function getAirbrakeNotifier(): Notifier
    {
        return $this->container->get('ami_airbrake.notifier');
    }

    /**
     * @param string $name
     * @return int
     * @throws \Exception
     */
    public function create(string $name): int
    {
        $customerId = null;
        $bingServiceManager = $this->get('bing.service_manager');
        $managedCustomerService = $bingServiceManager->getCustomerManagementService($bingServiceManager->getCustomerId());

        try {
            $request = new GetUserRequest();
            $request->UserId = null;
            $getUserResponse = $managedCustomerService->GetService()->GetUser($request);

            // In example, we get all managers from User and check RoleId for everyone,
            // but in prod we know exactly manager with this RoleId for User
            $roleIds = [];
            foreach ($getUserResponse->CustomerRoles->CustomerRole as $customerRole) {
                $roleIds[] = $customerRole->RoleId;
//                if ($customerRole->RoleId == 33) {
//                    $customerId = $customerRole->CustomerId;
//                }
            }

            if (!(in_array(33, $roleIds))) {
                $this->getAirbrakeNotifier()->notify(new \Exception(
                        '[Bing] SoapFault was thrown with message - '
                        . 'Only a user with the aggregator role can sign up new customers. '
                        . 'When the process was running "create new customer" '. PHP_EOL
                    )
                );
            }

            $customer = new Customer();
            // The primary business segment of the customer, for example, automotive, food, or entertainment.
            $customer->Industry = CustomerSettings::INDUSTRY;
            // The primary country where the customer operates.
            $customer->MarketCountry = CustomerSettings::MARKET_COUNTRY;
            // The primary language that the customer uses.
            $customer->MarketLanguage = CustomerSettings::MARKET_LANGUAGE;
            // The name of the customer.
            $customer->Name = $name;

            // The location where your business is legally registered.
            // The business address is used to determine your tax requirements.
            $businessAddress = new Address();
            $businessAddress->City = AddressSettings::CITY;
            $businessAddress->Line1 = AddressSettings::LINE1;
            $businessAddress->PostalCode = AddressSettings::POSTAL_CODE;
            $businessAddress->CountryCode = AddressSettings::COUNTRY_CODE;
            $businessAddress->BusinessName = 'Socius Marketing';
            $businessAddress->StateOrProvince = AddressSettings::STATE_OR_PROVINCE;

            $account = new AdvertiserAccount();
            $account->BusinessAddress = $businessAddress;

            // The type of currency that is used to settle the account.
            // The service uses the currency information for billing purposes.
            $account->CurrencyCode = AdvertiserAccountSettings::CURRENCY_CODE;

            // The name of the account.
            $account->Name = $name;

            $account->ParentCustomerId = $bingServiceManager->getCustomerId();

            // The TaxInformation is optional. If specified, The tax information must be valid
            // in the country that you specified in the BusinessAddress element. Without tax information
            // or exemption certificate, taxes might apply based on your business location.
            $account->TaxInformation = null;

            // The default time-zone for campaigns in this account.
            $account->TimeZone = AdvertiserAccountSettings::TIME_ZONE;
            $account->Language = LanguageType::English;
            $addAccountRequest = new AddAccountRequest();
            $addAccountRequest->Account = $account;

            $accountResponse = $managedCustomerService->GetService()->AddAccount($addAccountRequest);
            $customerId = (int)$accountResponse->AccountId;

//            $request = new SignupCustomerRequest();
//
//            $request->Customer = $customer;
//            $request->Account = $account;
//            $request->ParentCustomerId = $bingServiceManager->getCustomerId();
//
//            // Signup a new customer and account for the reseller.
//            $result = $managedCustomerService->GetService()->SignupCustomer($request);

//            $customerId = (int)$result->AccountId;
        } catch (\SoapFault $e) {
            $errorMessage = 'something went wrong';
            if (isset($e->detail->ApiFault->OperationErrors->OperationError->Message)) {
                $errorMessage = $e->detail->ApiFault->OperationErrors->OperationError->Message;
            }

            $this->get('change_log_manager')->changeLog("Create", "Account", "Bing", null, $errorMessage, null);

            $this->getAirbrakeNotifier()->notify(new \Exception(
                '[Bing] SoapFault was thrown with message - '
                    . $errorMessage . '. When the process was running "create new customer" '. PHP_EOL
                )
            );
        } catch (\Exception $e) {
            $this->getAirbrakeNotifier()->notify(new \Exception(
                '[Bing] Exception was thrown with message - '
                    . $e->getMessage() . '. When the process was running "create new customer" '. PHP_EOL
                )
            );
        }

        return $customerId ?: false;
    }

    /**
     * @param string $name
     * @param $systemAccountId
     * @throws \Exception
     */
    public function update(string $name, $systemAccountId)
    {
        try {
            $bingServiceManager = $this->get('bing.service_manager');
            $managedCustomerService = $bingServiceManager->getCustomerManagementService($bingServiceManager->getCustomerId());

            $request = new GetAccountRequest();
            $request->AccountId = $systemAccountId;
            $account = $managedCustomerService->GetService()->GetAccount($request);

            if ($account->Account->AccountLifeCycleStatus ==  AccountLifeCycleStatus::INACTIVE) {
                $this->getAirbrakeNotifier()->notify(new \Exception(
                    '[Bing] SoapFault was thrown with message - '
                    . 'Remote account cannot be update. '
                    . 'When the process was running "update customer" '. PHP_EOL
                ));
            }

            $advertiseAccount = new AdvertiserAccount();
            $advertiseAccount->Id = $systemAccountId;
            $advertiseAccount->Name = $name;
            $advertiseAccount->TimeStamp = $account->Account->TimeStamp;

            $request = new UpdateAccountRequest();
            $request->Account = $advertiseAccount;

            // Signup a new customer and account for the reseller.
            $managedCustomerService->GetService()->UpdateAccount($request);
        } catch (\SoapFault $e) {
            $errorMessage = 'something went wrong';
            if(isset($e->detail->ApiFault->OperationErrors->OperationError->Message)) {
                $errorMessage = $e->detail->ApiFault->OperationErrors->OperationError->Message;
            }

            $this->get('change_log_manager')->changeLog("Update", "Account", "Bing", null, $errorMessage, null);

            $this->getAirbrakeNotifier()->notify(new \Exception(
                '[Bing] SoapFault was thrown with message - '
                . $errorMessage . '. When the process was running "update customer" '. PHP_EOL
            ));
        } catch (\Exception $e) {
            $this->getAirbrakeNotifier()->notify(new \Exception(
                '[Bing] Exception was thrown with message - '
                . $e->getMessage() . '. When the process was running "update customer" '. PHP_EOL
            ));
        }
    }

    /**
     * @param $id
     * @throws \Exception
     */
    public function delete($id)
    {
        try {
            $bingServiceManager = $this->get('bing.service_manager');
            $managedCustomerService = $bingServiceManager->getCustomerManagementService($bingServiceManager->getCustomerId());

            $request = new GetAccountRequest();
            $request->AccountId = $id;
            $account = $managedCustomerService->GetService()->GetAccount($request);

            if ($account->Account->AccountLifeCycleStatus ==  AccountLifeCycleStatus::INACTIVE) {
                $this->getAirbrakeNotifier()->notify(new \Exception(
                    '[Bing] SoapFault was thrown with message - '
                    . 'Remote account cannot be delete. '
                    . 'When the process was running "delete customer" '. PHP_EOL
                ));
            }

            $request = new DeleteAccountRequest();
            $request->AccountId = $id;
            $request->TimeStamp = $account->Account->TimeStamp;
            $managedCustomerService->GetService()->DeleteAccount($request);
        } catch (\SoapFault $e) {
            $errorMessage = 'something went wrong';
            if(isset($e->detail->ApiFault->OperationErrors->OperationError->Message)) {
                $errorMessage = $e->detail->ApiFault->OperationErrors->OperationError->Message;
            }

            $this->get('change_log_manager')->changeLog("Delete", "Account", "Bing", null, $errorMessage, null);

            $this->getAirbrakeNotifier()->notify(new \Exception(
                '[Bing] SoapFault was thrown with message - '
                . $errorMessage . '. When the process was running "delete customer" '. PHP_EOL
            ));
        } catch (\Exception $e) {
            $this->getAirbrakeNotifier()->notify(new \Exception(
                '[Bing] Exception was thrown with message - '
                . $e->getMessage() . '. When the process was running "delete customer" '. PHP_EOL
            ));
        }
    }

    /**
     *
     */
    public function updateAccounts()
    {
        $array = $this->getAccounts();
        $em = $this->get('doctrine')->getManager();

        foreach ($array['AccountsInfo']->AccountInfo as $account) {
            $entity = $em->getRepository('DataBundle:BingAccount')->findOneBy(['systemAccountId' => $account->Id]);

            if (is_null($entity)) {
                $entity = new \App\Entity\BingAccount();
                $keywords = $this->getAmountKeywords($entity->getSystemAccountId());
                $entity->fill(['systemAccountId' => $account->Id, 'name' => $account->Name, 'keywords' => $keywords]);
                $em->persist($entity);
                continue;
            }

            $entity->fill(['name' => $account->Name]);
            $em->persist($entity);
        }
        $em->flush();
    }

    /**
     * @return array
     */
    public function getAccounts(): array
    {
        $bingServiceManager = $this->get('bing.service_manager');
        $managedCustomerService = $bingServiceManager->getCustomerManagementService();

        $request = new GetAccountsInfoRequest();
        $request->CustomerId = $bingServiceManager->getCustomerId();

        return (array)($managedCustomerService->GetService()->GetAccountsInfo($request));
    }

    /**
     * @param $clientCustomerId
     * @return int
     */
    public function getAmountKeywords($clientCustomerId): int
    {
        //TODO REMOVE THAT
        return 0;
//        $managedCustomerService = $this->get('bing.service_manager')->getCustomerManagementService(AuthConfigDev::CustomerId);
//        $campaignManagementService = $this->get('bing.service_manager')->getCampaignManagementService($account->getSystemAccountId());
//
//        $GetCampaignsByAccountIdRequest = new GetCampaignsByAccountIdRequest();
//        $GetCampaignsByAccountIdRequest->AccountId = $account->getSystemAccountId();
//        $GetCampaignsByAccountIdRequest->CampaignType = 'Search';
//
//        try {
//            $camplaingsIds = $campaignManagementService->GetService()->GetCampaignsByAccountId($GetCampaignsByAccountIdRequest);
//
//            if (property_exists($camplaingsIds->Campaigns, 'Campaign')) {
//                $camplaingsIds = $camplaingsIds->Campaigns->Campaign;
//
//                foreach ($camplaingsIds as $company) {
//                    $GetAdGroupsByCampaignIdRequest = new GetAdGroupsByCampaignIdRequest();
//                    $GetAdGroupsByCampaignIdRequest->CampaignId = $company->Id;
//
//                    $adsG = $campaignManagementService->GetService()->GetAdGroupsByCampaignId($GetAdGroupsByCampaignIdRequest);
//
//                    $array = (array)$adsG;
//                    $AdGroups = (array)$array['AdGroups'];
//                    if ($AdGroups) {
//                        $GetKeywordsByAdGroupIdRequest = new GetKeywordsByAdGroupIdRequest();
//                        $GetKeywordsByAdGroupIdRequest->AdGroupId = $AdGroups['AdGroup'][0]->Id;
//
//                        $keyWords = $campaignManagementService->GetService()->GetKeywordsByAdGroupId($GetKeywordsByAdGroupIdRequest);
//
//                        return count((array)$keyWords->Keywords);
//                    }
//
//                    return 0;
//                }
//            }
//
//            return 0;
//        } catch (SoapFault $e) {
//            print "\nLast SOAP request/response:\n";
//            printf("Fault Code: %s\nFault String: %s\n", $e->faultcode, $e->faultstring);
//            print $managedCustomerService->GetWsdl() . "\n";
//            print $managedCustomerService->GetService()->__getLastRequest() . "\n";
//            print $managedCustomerService->GetService()->__getLastResponse() . "\n";
//
//            if (isset($e->detail->AdApiFaultDetail)) {
//                $managedCustomerService->GetService()->OutputAdApiFaultDetail($e->detail->AdApiFaultDetail);
//
//            } elseif (isset($e->detail->ApiFault)) {
////                $managedCustomerService->GetService()->OutputApiFault($e->detail->ApiFault);
//            }
//        } catch (Exception $e) {
//            // Ignore fault exceptions that we already caught.
//            if ($e->getPrevious()) {
//
//            } else {
//                print $e->getCode() . " " . $e->getMessage() . "\n\n";
//                print $e->getTraceAsString() . "\n\n";
//            }
//        }
    }
}