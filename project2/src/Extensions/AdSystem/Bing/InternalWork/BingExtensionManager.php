<?php

namespace App\Extensions\AdSystem\Bing\InternalWork;

use Airbrake\Notifier;
use App\Document\{ErrorsQueue, Extension, ExtensionsQueue};
use App\Entity\BrandExtension as EntityExtension;
use App\Extensions\AdSystem\Bing\ExternalWork\Auth\BingServiceManager;
use App\Extensions\AdSystem\Bing\Traits\BingExtensionTrait;
use App\Extensions\Common\AdSystemEnum;
use App\Extensions\Common\InternalWork\Basic\ExtensionManager;
use App\Providers\{ProviderCampaignName, ProviderEntityName};
use App\Services\Sync\SyncChangesWithCampaigns;
use Doctrine\ODM\MongoDB\DocumentManager;
use Microsoft\BingAds\V13\CampaignManagement\{AddAdExtensionsRequest, AdExtensionIdToEntityIdAssociation,
    AdExtensionsTypeFilter, AssociationType, DeleteAdExtensionsRequest, GetAdExtensionsByIdsRequest,
    SetAdExtensionsAssociationsRequest};
use MongoDB\BSON\ObjectId;
use Psr\Container\ContainerInterface;
use Symfony\Component\Cache\Adapter\AdapterInterface;;

/**
 * Class BingExtensionManager
 *
 * @package App\Extensions\AdSystem\Bing\InternalWork
 */
class BingExtensionManager extends ExtensionManager
{
    use BingExtensionTrait;

    const REQUEST_OPERATIONS_LIMIT = 100;

    const LINK_QUERY_ITEMS_LIMIT = 10000;

    /**
     * @var AdapterInterface
     */
    protected AdapterInterface $cacheService;

    /**
     * @var
     */
    private $_managedCustomerService;

    /**
     * @param ContainerInterface        $container
     * @param DocumentManager           $dm
     * @param SyncChangesWithCampaigns  $syncChangesWithCampaigns
     */
    public function __construct(
        ContainerInterface          $container,
        DocumentManager             $dm,
        SyncChangesWithCampaigns    $syncChangesWithCampaigns,
        AdapterInterface            $cacheService
    ) {
        parent::__construct(AdSystemEnum::BING, $container, $dm, $syncChangesWithCampaigns);

        $this->cacheService = $cacheService;
    }

    /**
     * @return mixed
     */
    protected function getBingServiceManager()
    {
        return $this->get('bing.service_manager');
    }

    /**
     * @return Notifier
     */
    protected function getAirbrakeNotifier(): Notifier
    {
        return $this->get('ami_airbrake.notifier');
    }

    /**
     * @return AdapterInterface
     */
    protected function getCacheService(): AdapterInterface
    {
        return $this->cacheService;
    }

    /**
     * @param $category
     * @return bool|mixed
     */
    public function getFilterByCategory($category)
    {
        $array = $this->getMethodsListForExtensions();

        return key_exists($category, $array) ? $array[$category] : false;
    }

    /**
     * @return array
     */
    protected function getMethodsListForExtensions()
    {
        return [
            EntityExtension::SITELINK       => AdExtensionsTypeFilter::SitelinkAdExtension,
            EntityExtension::PHONE          => AdExtensionsTypeFilter::CallAdExtension,
            EntityExtension::CALLOUT        => AdExtensionsTypeFilter::CalloutAdExtension,
            EntityExtension::STRUCTURED     => AdExtensionsTypeFilter::StructuredSnippetAdExtension,
            EntityExtension::PROMOTIONAL    => AdExtensionsTypeFilter::PromotionAdExtension,
        ];
    }

    /**
     * @return mixed|void
     */
    public function syncExtensionsWithAdSystem()
    {
        // Don't change the order of calling methods
        $this->deleteCampaignExtensions();
        $this->uploadNewExtensions();
        $this->linkUploadedExtensionsWithCampaigns();
    }

    /**
     * @param int $extensionId
     * @param int $extensionCategory
     * @param int $accountId
     * @return mixed
     */
    private function getEntityByCampaignIdAndExtensionId(int $extensionId, int $extensionCategory, int $accountId)
    {
        $request = new GetAdExtensionsByIdsRequest();
        $request->AccountId = $accountId;
        $request->AdExtensionIds = [$extensionId];
        $request->AdExtensionType = $this->getFilterByCategory($extensionCategory);

        try {
            $result = $this->_managedCustomerService->GetService()->GetAdExtensionsByIds($request);

            return $result->AdExtensions->AdExtension[0];
        } catch (\SoapFault $sf) {
            print "\nLast SOAP request/response:\n";
            printf("Fault Code: %s\nFault String: %s\n", $sf->faultcode, $sf->faultstring);
            print $this->_managedCustomerService->GetWsdl() . "\n";
            print $this->_managedCustomerService->GetService()->__getLastRequest() . "\n";
            print $this->_managedCustomerService->GetService()->__getLastResponse() . "\n";

            if (isset($sf->detail->AdApiFaultDetail)) {
                $this->_managedCustomerService->GetService()->OutputAdApiFaultDetail($sf->detail->AdApiFaultDetail);
            }
        } catch (\Exception $sf) {
            if (!$sf->getPrevious()) {
                print $sf->getCode() . " " . $sf->getMessage() . "\n\n";
                print $sf->getTraceAsString() . "\n\n";
            }
        }
    }

    public function deleteCampaignExtensions()
    {
        $entityName = ProviderEntityName::getForAccountsBySystem($this->adSystem);

        $extensions = $this->dm->getRepository(ExtensionsQueue::class)->getListForDelete($this->dm);

        $data = $this->getRemovableExtensionsGroupedData($extensions);
        foreach ($data as $backendId => $accounts) {
            foreach ($accounts as $accountId => $extensions) {
                foreach ($extensions as $extensionId => $datum) {
                    /** @var ExtensionsQueue $extension */
                    $extension = $datum['extension'];
                    $campaigns = $datum['campaigns'];

                    $this->dm->setBrandTemplateId($extension->getBrandTemplateId());

                    /** @var \App\Entity\BingAccount $account */
                    $account = $this->getEntityManager()->getRepository($entityName)->findOneBy(['id' => $extension->getSystemAccount(), 'available' => 1]);
                    if (empty($account)) {
                        $this->getAirbrakeNotifier()->notify(new \Exception(
                            '[Bing] ERROR: Account with id = ' . $extension->getSystemAccount()
                            . ' not found.'. PHP_EOL
                        ));

                        continue;
                    }

                    $this->_managedCustomerService = $this->getBingServiceManager()->getCampaignManagementService($account->getSystemAccountId());

                    $exists = true;
                    $response = $this->getEntityByCampaignIdAndExtensionId($extensionId, $extension->getCategory(), $account->getSystemAccountId());
                    if ((bool)$response) {
                        $extensionInSystem = $response;
                    } else {
                        $exists = false;
                    }

                    $this->dm->getRepository(Extension::class)
                        ->createQueryBuilder()
                        ->remove()
                        ->field('systemExtensionId')->equals($extensionId)
                        ->field('systemCampaignId')->in($campaigns)
                        ->getQuery()
                        ->execute();
                    $this->dm->getRepository(ExtensionsQueue::class)
                        ->createQueryBuilder()
                        ->remove()
                        ->field('systemExtensionId')->equals($extensionId)
                        ->field('systemCampaignId')->in($campaigns)
                        ->getQuery()
                        ->execute();

                    if ($exists && ($extensionInSystem->Status != 'REMOVED')) {
                        $deleteAdExtensionsRequest = new DeleteAdExtensionsRequest();
                        $deleteAdExtensionsRequest->AccountId = $account->getSystemAccountId();
                        $deleteAdExtensionsRequest->AdExtensionIds = [$extensionInSystem->Id];

                        $this->_managedCustomerService->getService()->DeleteAdExtensions($deleteAdExtensionsRequest);
                    }
                }
            }
        }
    }

    public function uploadNewExtensions()
    {
        $extensions = $this->getExtensionsForUpload();

        $entityName = ProviderEntityName::getForAccountsBySystem($this->adSystem);

        $sortedExts = [];
        foreach ($extensions as $e) {
            $sortedExts[$e->getSystemAccount()][$e->getKcCampaignBackendId()][] = $e;
        }

        foreach ($sortedExts as $accountId => $extsByKcCampaign) {

            /** @var \App\Entity\BingAccount $account */
            $account = $this->getEntityManager()->getRepository($entityName)->findOneBy(["id" => $accountId]);
            if (empty($account)) {
                printf("ERROR: Account with id=%d not found.\n", $accountId);
                continue;
            }

            $this->_managedCustomerService = $this->getBingServiceManager()
                ->getCampaignManagementService($account->getSystemAccountId());

            $extsForUpload = [];

            /**
             * @var int $backendId
             * @var  ExtensionsQueue[] $exts
             */
            foreach ($extsByKcCampaign as $backendId => $exts) {
                $this->dm->setBrandTemplateId($exts[array_rand($exts)]->getBrandTemplateId());

                $exts = $this->checkAndDeleteIfAlreadyUploaded($exts, $backendId, $accountId);

                /** @var ExtensionsQueue $extension */
                foreach ($exts as $ext) {
                    $extsForUpload[] = $ext;

                    if (count($extsForUpload) >= self::REQUEST_OPERATIONS_LIMIT) {
                        $this->uploadExtensions($extsForUpload, $account);
                        $extsForUpload = [];
                    }
                }
            }

            if (!empty($extsForUpload)) {
                $this->uploadExtensions($extsForUpload, $account);
            }
        }
    }

    private function uploadExtensions($exts, $account)
    {
        $extensionsRequest = $this->getExtensionsRequest($exts, $account);

        try {
            $result = $this->_managedCustomerService->GetService()->AddAdExtensions($extensionsRequest);

            // Process error result items
            $this->processNestedPartialErrors($result->NestedPartialErrors, $exts);

            # Process success result items
            $this->processAddExtensionsSuccess($result->AdExtensionIdentities, $exts);

        } catch (\SoapFault $sf) {
            if (isset($sf->detail->ApiFaultDetail->OperationErrors->OperationError)) {
                $operationError = $sf->detail->ApiFaultDetail->OperationErrors->OperationError;

                $this->processFatalApiError($exts, $operationError);
            }
        }
    }

    private function getExtensionsRequest(array $extensions, \App\Entity\BingAccount $account)
    {
        $systemExtensions = [];
        foreach ($extensions as $ext) {
            try {
                $systemExtensions[] = $this->makeSystemExtension($ext);
            } catch (\Exception $e) {
                print $e->getMessage();
            }
        }

        $request = new AddAdExtensionsRequest();
        $request->AccountId = $account->getSystemAccountId();
        $request->AdExtensions = $systemExtensions;

        return $request;
    }

    private function processAddExtensionsSuccess($adExtensionIdentities, $extensions)
    {
        if (isset($adExtensionIdentities->AdExtensionIdentity)) {
            for ($index = 0; $index < count($adExtensionIdentities->AdExtensionIdentity); $index++) {

                # The list corresponds directly to the list of extensions specified in the request.
                if (!empty($adExtensionIdentities->AdExtensionIdentity[$index])
                    && isset($adExtensionIdentities->AdExtensionIdentity[$index]->Id)) {
                    /** @var ExtensionsQueue $ext */
                    $ext = $extensions[$index];
                    $ext->setSystemExtensionId($adExtensionIdentities->AdExtensionIdentity[$index]->Id);

                    $where = [
                        'teId' => $ext->getTeId(),
                        'kcCampaignBackendId' => $ext->getKcCampaignBackendId(),
                        'systemAccount' => $ext->getSystemAccount()
                    ];
                    $attributes = ['systemExtensionId' => $ext->getSystemExtensionId()];

                    $this->dm->getRepository(ExtensionsQueue::class)
                        ->updateManyByAttributes($this->dm, $where, $attributes);
                }
            }
        }
    }

    public function linkUploadedExtensionsWithCampaigns()
    {
        /** @var ExtensionsQueue[] $extensions */
        $extensions = $this->dm->createQueryBuilder(ExtensionsQueue::class)
            ->field('systemCampaignId')->exists(true)
            ->field('systemExtensionId')->exists(true)
            ->field('add')->exists(true)
            ->field('error')->exists(false)
            ->limit(self::LINK_QUERY_ITEMS_LIMIT)
            ->getQuery()
            ->execute();

        $entityName = ProviderEntityName::getForAccountsBySystem($this->adSystem);

        $sortedExts = [];
        foreach ($extensions as $e) {
            $sortedExts[$e->getSystemAccount()][$e->getKcCampaignBackendId()][] = $e;
        }

        foreach ($sortedExts as $accountId => $extsByKcCampaign) {

            /** @var \App\Entity\BingAccount $account */
            $account = $this->getEntityManager()->getRepository($entityName)->findOneBy(["id" => $accountId]);
            if (empty($account)) {
                printf("ERROR: Account with id=%d not found.\n", $accountId);
                continue;
            }

            $this->_managedCustomerService = $this->getBingServiceManager()
                ->getCampaignManagementService($account->getSystemAccountId());

            $extsForUpload = [];

            /**
             * @var int $backendId
             * @var  ExtensionsQueue[] $exts
             */
            foreach ($extsByKcCampaign as $backendId => $exts) {
                $this->dm->setBrandTemplateId($exts[array_rand($exts)]->getBrandTemplateId());

                /** @var ExtensionsQueue $extension */
                foreach ($exts as $ext) {
                    $extsForUpload[] = $ext;

                    if (count($extsForUpload) >= self::REQUEST_OPERATIONS_LIMIT) {
                        $this->linkExtensions($extsForUpload, $account);
                        $extsForUpload = [];
                    }
                }
            }

            if (!empty($extsForUpload)) {
                $this->linkExtensions($extsForUpload, $account);
            }
        }
    }

    private function linkExtensions($exts, $account)
    {
        # Add an association between extensions and campaigns
        $associations = $this->getExtensionEntityAssociation($exts);

        $associationsRequest = $this->getAssociationsRequest($associations, $account);

        try {
            $result = $this->_managedCustomerService->getService()->SetAdExtensionsAssociations($associationsRequest);

            # Process error result items
            $this->processPartialErrors($result->PartialErrors, $exts);

            # Process success result items
            $this->processExtensionsAssociationsResult($result->PartialErrors, $exts);
        } catch (\SoapFault $sf) {
            if (isset($sf->detail->ApiFaultDetail->OperationErrors->OperationError)) {
                $operationError = $sf->detail->ApiFaultDetail->OperationErrors->OperationError;

                $this->processFatalApiError($exts, $operationError);
            }
        }
    }

    private function getExtensionEntityAssociation(array $extensions)
    {
        $associations = [];
        /** @var ExtensionsQueue[] $extensions */
        foreach ($extensions as $extension) {
            $association = new AdExtensionIdToEntityIdAssociation();
            $association->AdExtensionId = $extension->getSystemExtensionId();
            $association->EntityId = $extension->getSystemCampaignId();
            $associations[] = $association;
        }

        return $associations;
    }

    private function getAssociationsRequest(array $associations, \App\Entity\BingAccount $account)
    {
        $request = new SetAdExtensionsAssociationsRequest();
        $request->AccountId = $account->getSystemAccountId();
        $request->AssociationType = AssociationType::Campaign;
        $request->AdExtensionIdToEntityIdAssociations = $associations;

        return $request;
    }

    private function processExtensionsAssociationsResult($partialErrors, $extensions)
    {
        if (isset($partialErrors->BatchError)) {

            foreach ($partialErrors->BatchError as $batchError) {
                unset($extensions[$batchError->Index]);
            }
        }

        $ids = [];
        /** @var ExtensionsQueue[] $extensions */
        foreach ($extensions as $ext) {
            $newExt = new Extension;
            $newExt->setCategory($ext->getCategory());
            $newExt->setTeId($ext->getTeId());
            $newExt->setSystemExtensionId($ext->getSystemExtensionId());
            $newExt->setSystemCampaignId($ext->getSystemCampaignId());
            $newExt->setSystemAccount($ext->getSystemAccount());
            $newExt->setKcCampaignBackendId($ext->getKcCampaignBackendId());
            $this->dm->persist($newExt);

            $ids[] = $ext->getId();
        }

        $this->dm->flush();

        $this->dm->getRepository(ExtensionsQueue::class)->removeByAttributesIn($this->dm, ['id' => $ids]);
    }

    private function processNestedPartialErrors($nestedPartialErrors, array $extensions)
    {
        # The list of errors do not correspond directly to the list of items in the request. Use item->Index
        if (count((array)$nestedPartialErrors) != 0 || isset($nestedPartialErrors->BatchErrorCollection)) {
            $this->processBatchErrorCollection($nestedPartialErrors->BatchErrorCollection, $extensions);
        }
    }

    private function processPartialErrors($partialErrors, array $extensions)
    {
        # The list of errors do not correspond directly to the list of items in the request. Use item->Index
        if (isset($partialErrors->BatchError)) {
            $this->processBatchErrorCollection($partialErrors->BatchError, $extensions);
        }
    }

    private function processBatchErrorCollection($batchErrorCollection, array $extensions)
    {
        $indexes = [];
        foreach ($batchErrorCollection as $batchError) {

            # Bing can return a few similar errors for the single extension.
            # Code error the same for all errors but each error has minor additional info which not important for us.
            # For example for call extension if phone number isn't correct bing return about 50 errors
            # for the same issue for each PublisherCountry code (CountryCode).
            if(isset($indexes[$batchError->Index]))
                continue;

            /** @var ExtensionsQueue $extension */
            $extension = $extensions[$batchError->Index];

            $campaignName = ProviderCampaignName::getCampaignName($this->dm, $this->getCacheService(), $extension->getCampaignId());

            $extensionTypeName = $this->getMethodsListForExtensions()[$extension->getCategory()];

            $errorMessage = $batchError->Message;
            if (!empty($batchError->DisapprovedText)) {
                $errorMessage = $errorMessage . " DisapprovedText: '" . $batchError->DisapprovedText . "'";
            }

            $errorsQueue = new ErrorsQueue;
            $errorsQueue->setType('extension');
            $errorsQueue->setErrorElementId(new ObjectId($extension->getId()));
            $errorsQueue->setRawError($errorMessage);
            $errorsQueue->setBackendId($extension->getKcCampaignBackendId());
            $errorsQueue->setError($batchError->ErrorCode);
            $errorsQueue->setCampaignName($campaignName);
            $errorsQueue->setHeadline('' . $extensionTypeName . ' - ' . $extension->getPhone() .
                $extension->getCallout() . $extension->getDesc1() . '  ' . $extension->getDesc2());
            $errorsQueue->setTeId($extension->getTeId());

            $this->dm->persist($errorsQueue);

            $where = [
                'teId' => $extension->getTeId(),
                'kcCampaignBackendId' => $extension->getKcCampaignBackendId()
            ];
            $attributes = ['error' => $errorMessage];
            $this->dm->getRepository(ExtensionsQueue::class)
                ->updateManyByAttributes($this->dm, $where, $attributes);

            $indexes[$batchError->Index] = $batchError->Index;

            $this->getAirbrakeNotifier()->notify(new \Exception(
                '[Bing - Error] '. $errorMessage .', when the process was running "UploadExtensions" '. PHP_EOL
            ));
        }

        $this->dm->flush();
    }

    private function processFatalApiError(array $extensions, $operationError)
    {
        /** @var ExtensionsQueue $extension */
        foreach ($extensions as $extension) {
            $campaignName = ProviderCampaignName::getCampaignName($this->dm, $this->getCacheService(), $extension->getCampaignId());

            $extensionTypeName = $this->getMethodsListForExtensions()[$extension->getCategory()];

            $errorsQueue = new ErrorsQueue;
            $errorsQueue->setType('extension');
            $errorsQueue->setErrorElementId(new ObjectId($extension->getId()));
            $errorsQueue->setRawError($operationError->Message);
            $errorsQueue->setBackendId($extension->getKcCampaignBackendId());
            $errorsQueue->setError($operationError->ErrorCode);
            $errorsQueue->setCampaignName($campaignName);
            $errorsQueue->setHeadline('' . $extensionTypeName . ' - ' . $extension->getPhone() . $extension->getCallout() . $extension->getDesc1() . '  ' . $extension->getDesc2());
            $errorsQueue->setTeId($extension->getTeId());

            $extension->setErrorCode($operationError->Code);
            $extension->setError($operationError->Message);

            $this->getAirbrakeNotifier()->notify(new \Exception(
                '[Bing - Error] ' . $operationError->Message
                . ', when the process was running "UploadExtensions" '. PHP_EOL
            ));

            $this->dm->persist($errorsQueue);
            $this->dm->persist($extension);
        }

        $this->dm->flush();
    }
}
