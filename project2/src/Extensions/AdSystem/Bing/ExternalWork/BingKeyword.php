<?php

namespace App\Extensions\AdSystem\Bing\ExternalWork;

use App\Document\KeywordsQueue;
use App\Entity\BingAccount;
use App\Extensions\AdSystem\Bing\ExternalWork\Auth\BingServiceManager;
use App\Extensions\Common\AdSystemEnum;
use App\Extensions\Common\ExternalWork\KeywordInterface;
use App\Providers\ProviderEntityName;
use Doctrine\ODM\MongoDB\{DocumentManager, MongoDBException};
use Doctrine\ORM\EntityManagerInterface;
use Psr\Container\{ContainerExceptionInterface, ContainerInterface, NotFoundExceptionInterface};
use Microsoft\BingAds\V13\CampaignManagement\DeleteKeywordsRequest;
use Microsoft\BingAds\V13\CampaignManagement\DeleteNegativeKeywordsFromEntitiesRequest;
use Microsoft\BingAds\V13\CampaignManagement\EntityNegativeKeyword;
use Microsoft\BingAds\V13\CampaignManagement\GetKeywordsByAdGroupIdRequest;
use Microsoft\BingAds\V13\CampaignManagement\GetNegativeKeywordsByEntityIdsRequest;
use SoapFault;

/**
 * @Class BingKeyword
 */
class BingKeyword implements KeywordInterface
{
    /**
     * @var string
     */
    protected string $adSystem = AdSystemEnum::BING;

    /**
     * @var ContainerInterface
     */
    protected ContainerInterface $container;

    /**
     * @param ContainerInterface $container
     */
    public function __construct(ContainerInterface $container)
    {
        $this->container = $container;
    }

    /**
     * @return BingServiceManager
     * @throws ContainerExceptionInterface|NotFoundExceptionInterface
     */
    protected function getBingServiceManager(): BingServiceManager
    {
        return $this->container->get('bing.service_manager');
    }

    /**
     * @return DocumentManager
     * @throws ContainerExceptionInterface|NotFoundExceptionInterface
     */
    protected function getDocumentManager(): DocumentManager
    {
        if (!isset($this->dm)) {
            /** @var DocumentManager $dm */
            $dm = $this->container->get('doctrine_mongodb')->getManager();
            $dm->setAdSystem($this->adSystem);

            $this->dm = $dm;

            return $dm;
        } else {
            return $this->dm;
        }
    }

    /**
     * @return EntityManagerInterface
     * @throws ContainerExceptionInterface|NotFoundExceptionInterface
     */
    protected function getEntityManager(): EntityManagerInterface
    {
        return $this->container->get('doctrine')->getManager();
    }

    /**
     * @param $parentIds
     * @param $accountId
     * @param $backendId
     * @param $brandTemplateId
     * @param $campaignCriterion
     *
     * @return bool
     * @throws ContainerExceptionInterface|NotFoundExceptionInterface|MongoDBException
     */
    public function findByParentIdsAndRemove(
        $parentIds,
        $accountId,
        $backendId,
        $brandTemplateId,
        $campaignCriterion = false
    ): bool {
        $em = $this->getEntityManager();
        $dm = $this->getDocumentManager();

        /** @var BingAccount $account */
        $account = $em->getRepository(ProviderEntityName::getForAccountsBySystem($this->adSystem))->find($accountId);

        if ($campaignCriterion) {
            $this->findByCampaignIdsAndRemove($account, $brandTemplateId, $backendId, $parentIds);
        } else {
            $this->findByAdgroupIdsAndRemove($account, $brandTemplateId, $backendId, $parentIds);
        }

        $where      = ['kcCampaignBackendId' => $backendId];
        $attributes = ['error' => null];

        $dm->getRepository(KeywordsQueue::class)->updateManyByAttributes($dm, $where, $attributes);

        return true;
    }

    /**
     * @param BingAccount   $account
     * @param int           $brandTemplateId
     * @param int           $backendId
     * @param array         $systemCampaignIds
     * @throws ContainerExceptionInterface|NotFoundExceptionInterface|MongoDBException
     */
    private function findByCampaignIdsAndRemove(
        BingAccount  $account,
        int             $brandTemplateId,
        int             $backendId,
        array           $systemCampaignIds
    ) {
        $dm = $this->getDocumentManager();

        $counterOperations = 0;
        foreach ($systemCampaignIds as $systemCampaignId) {
            $request = new GetNegativeKeywordsByEntityIdsRequest();
            $request->EntityIds = [$systemCampaignId];
            $request->EntityType = "Campaign";
            $request->ParentEntityId = $account->getSystemAccountId();

            $campaignService = $this->getBingServiceManager()->getCampaignManagementService($account->getSystemAccountId());

            try {
                $entityNegativeKeywords = $campaignService->GetService()->GetNegativeKeywordsByEntityIds($request);
            } catch (SoapFault $e) {
                print "\nLast SOAP request/response:\n";
                printf("Fault Code: %s\nFault String: %s\n", $e->faultcode, $e->faultstring);

                if (!$entityNegativeKeywords = $campaignService->GetService()->GetNegativeKeywordsByEntityIds($request)) {
                    printf("Skip search NegativeKeywordIds by CampaignId: %s\n", $systemCampaignId);

                    continue;
                }
            }

            if (!property_exists($entityNegativeKeywords->EntityNegativeKeywords, 'EntityNegativeKeyword')) {
                continue;
            }

            foreach ($entityNegativeKeywords->EntityNegativeKeywords->EntityNegativeKeyword as $entityNegativeKeyword) {
                if (!property_exists($entityNegativeKeyword->NegativeKeywords, 'NegativeKeyword')) {
                    continue;
                }

                foreach ($entityNegativeKeyword->NegativeKeywords->NegativeKeyword as $negativeKeyword) {
                    $fieldsValue = [
                        'systemCampaignId' => $systemCampaignId,
                        'systemKeywordId' => $negativeKeyword->Id,
                        'negative' => true,
                    ];

                    $keywordForRemove = $this->makeItemForRemove($account->getId(), $brandTemplateId, $backendId, $fieldsValue);
                    $dm->persist($keywordForRemove);

                    $counterOperations++;
                }

                if ($counterOperations % 500 == 0) {
                    $dm->flush();
                    $dm->clear();

                    printf("Count: %d\r\r\r\r\r\r\r\r\r\r\r\r\r\r\r\r", $counterOperations);
                }
            }

            $dm->flush();
            $dm->clear();

        }

        print("Count: ". $counterOperations. PHP_EOL);
    }

    /**
     * @param BingAccount   $account
     * @param int           $brandTemplateId
     * @param int           $backendId
     * @param array         $parentIds
     * @throws ContainerExceptionInterface|NotFoundExceptionInterface|MongoDBException
     */
    private function findByAdgroupIdsAndRemove(
        BingAccount     $account,
        int             $brandTemplateId,
        int             $backendId,
        array           $parentIds

    ) {
        $dm = $this->getDocumentManager();

        $counterOperations = 0;
        foreach ($parentIds as $systemCampaignId => $systemAdgroupIds) {
            foreach ($systemAdgroupIds as $systemAdgroupId) {
                $keywordsByAdGroupIdRequest = new GetKeywordsByAdGroupIdRequest();
                $keywordsByAdGroupIdRequest->AdGroupId = $systemAdgroupId;

                $campaignService = $this->getBingServiceManager()->getCampaignManagementService($account->getSystemAccountId());

                try {
                    $systemKeywordsByAdGroupIdRequest = $campaignService->GetService()->GetKeywordsByAdGroupId($keywordsByAdGroupIdRequest);
                } catch (SoapFault $e) {
                    print "\nLast SOAP request/response:\n";
                    printf("Fault Code: %s\nFault String: %s\n", $e->faultcode, $e->faultstring);

                    if (!$systemKeywordsByAdGroupIdRequest = $campaignService->GetService()->GetKeywordsByAdGroupId($keywordsByAdGroupIdRequest)) {
                        printf("Skip Bing search AdIds by AdGroupId: %s\n", $systemAdgroupId);

                        continue;
                    }
                }

                if (!property_exists($systemKeywordsByAdGroupIdRequest->Keywords, 'Keyword')) {
                    continue;
                }

                foreach ($systemKeywordsByAdGroupIdRequest->Keywords->Keyword as $keyword) {
                    $fieldsValue = [
                        'systemAdgroupId' => $systemAdgroupId,
                        'systemCampaignId' => $systemCampaignId,
                        'systemKeywordId' => $keyword->Id,
                        'negative' => false,
                    ];

                    $keywordForRemove = $this->makeItemForRemove($account->getId(), $brandTemplateId, $backendId, $fieldsValue);
                    $dm->persist($keywordForRemove);

                    $counterOperations++;
                    if ($counterOperations % 500 == 0) {
                        $dm->flush();
                        $dm->clear();

                        printf("Count: %d\r\r\r\r\r\r\r\r\r\r\r\r\r\r\r\r", $counterOperations);
                    }
                }

                $dm->flush();
                $dm->clear();
            }
        }

        printf("Count: %d\r\r\r\r\r\r\r\r\r\r\r\r\r\r\r\r\n", $counterOperations);
    }

    /**
     * @param int   $accountId
     * @param int   $brandTemplateId
     * @param int   $backendId
     * @param array $fieldsValue
     *
     * @return KeywordsQueue
     */
    public function makeItemForRemove(
        int     $accountId,
        int     $brandTemplateId,
        int     $backendId,
        array   $fieldsValue
    ): KeywordsQueue
    {
        $fieldsValue['systemAccount'] = $accountId;
        $fieldsValue['brandTemplateId'] = $brandTemplateId;
        $fieldsValue['kcCampaignBackendId'] = $backendId;
        $fieldsValue['error'] = 'Temporary';
        $fieldsValue['delete'] = true;

        $keywordForRemove = new KeywordsQueue();
        $keywordForRemove->fill($fieldsValue);

        return $keywordForRemove;
    }


    /**
     * @param int   $customerId
     * @param int   $criterionId
     * @param int   $parentId
     * @param false $campaignCriterion
     *
     * @return EntityNegativeKeyword|array
     */
    public function makeDeleteItemOperation(
        int     $customerId,
        int     $criterionId,
        int     $parentId,
        bool    $campaignCriterion = false
    ) {
        if ($campaignCriterion) {
            $operation = new EntityNegativeKeyword();
            $operation->NegativeKeywords = [$criterionId];
            $operation->EntityId = $parentId;
            $operation->EntityType = "Campaign";
        } else {
            $operation = [$parentId => $criterionId];
        }

        return $operation;
    }

    /**
     * @param int   $customerId
     * @param array $operations
     * @param bool  $campaignCriterion
     *
     * @return bool
     * @throws ContainerExceptionInterface|NotFoundExceptionInterface
     */
    public function pushOperations(int $customerId, array $operations, bool $campaignCriterion = false): bool
    {
        $campaignService = $this->getBingServiceManager()->getCampaignManagementService($customerId);

        $request = null;
        try {
            if ($campaignCriterion) {
                $request = new DeleteNegativeKeywordsFromEntitiesRequest();
                $request->EntityNegativeKeywords = $operations;

                $response = $campaignService->GetService()->DeleteNegativeKeywordsFromEntities($request);

                if (!empty($response->PartialErrors->BatchError[0])) {
                    print $response->PartialErrors->BatchError[0]->Message. PHP_EOL;
                }
            } else {
                foreach ($operations as $adGroupId => $keywordIds) {
                    $request = new DeleteKeywordsRequest();
                    $request->AdGroupId = $adGroupId;
                    $request->KeywordIds = $keywordIds;

                    $response = $campaignService->GetService()->DeleteKeywords($request);

                    if (!empty($response->PartialErrors->BatchError[0])) {
                        print $response->PartialErrors->BatchError[0]->Message. PHP_EOL;
                    }
                }
            }

            return true;
        } catch (SoapFault $e) {
            print "\nLast SOAP request/response:\n";
            printf("Fault Code: %s\nFault String: %s\n", $e->faultcode, $e->faultstring);

            if ($campaignCriterion && !$response = $campaignService->GetService()->DeleteNegativeKeywordsFromEntities($request)) {
                print("Skip Bing delete keywords by campaign.". PHP_EOL);
            } elseif (!$campaignCriterion && !$response = $campaignService->GetService()->DeleteKeywords($request)) {
                print("Skip Bing delete keywords by AdGroup.". PHP_EOL);
            }

            if (!empty($response->PartialErrors->BatchError[0])) {
                print $response->PartialErrors->BatchError[0]->Message. PHP_EOL;
            }

            return false;
        }
    }
}