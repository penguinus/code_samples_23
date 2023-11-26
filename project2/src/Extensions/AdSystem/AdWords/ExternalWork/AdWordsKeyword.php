<?php

namespace App\Extensions\AdSystem\AdWords\ExternalWork;

use App\Document\KeywordsQueue;
use App\Entity\AdwordsAccount;
use App\Extensions\AdSystem\AdWords\ExternalWork\Auth\AdWordsServiceManager;
use App\Extensions\Common\AdSystemEnum;
use App\Extensions\Common\ExternalWork\KeywordInterface;
use App\Providers\ProviderEntityName;
use Doctrine\ODM\MongoDB\{DocumentManager, MongoDBException};
use Doctrine\ORM\EntityManagerInterface;
use Google\Ads\GoogleAds\Util\V13\ResourceNames;
use Google\Ads\GoogleAds\V13\Services\{AdGroupCriterionOperation, CampaignCriterionOperation, GoogleAdsRow};
use Google\ApiCore\{ApiException, ValidationException};
use Psr\Container\{ContainerExceptionInterface, ContainerInterface, NotFoundExceptionInterface};

/**
 * @Class AdWordsKeyword
 */
class AdWordsKeyword implements KeywordInterface
{
    /**
     * @var string
     */
    protected string $adSystem = AdSystemEnum::ADWORDS;

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
     * @return AdWordsServiceManager
     * @throws ContainerExceptionInterface|NotFoundExceptionInterface
     */
    protected function getGoogleServiceManager(): AdWordsServiceManager
    {
        return $this->container->get('adwords.service_manager');
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
     * @throws ContainerExceptionInterface|NotFoundExceptionInterface|MongoDBException|ApiException|ValidationException
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

        /** @var AdwordsAccount $account */
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
     * @param AdwordsAccount    $account
     * @param int               $brandTemplateId
     * @param int               $backendId
     * @param array             $systemCampaignIds
     * @throws ApiException | ValidationException | ContainerExceptionInterface | MongoDBException | NotFoundExceptionInterface
     */
    private function findByCampaignIdsAndRemove(
        AdwordsAccount  $account,
        int             $brandTemplateId,
        int             $backendId,
        array           $systemCampaignIds
    ) {
        $dm = $this->getDocumentManager();

        $counterOperations = 0;
        foreach (array_chunk($systemCampaignIds, 100) as $systemCampaignIds) {
            $query = sprintf(/** @lang text */
                "SELECT campaign.id, campaign_criterion.negative, campaign_criterion.criterion_id 
                WHERE campaign_criterion.type = KEYWORD 
                AND campaign_criterion.status != REMOVED 
                AND campaign.id IN (%s)",
                implode(', ', $systemCampaignIds)
            );

            $googleAdsServiceClient = $this->getGoogleServiceManager()->getGoogleAdsServiceClient();

            // Issues a search request by specifying page size.
            $campaignCriterionResponse = $googleAdsServiceClient->search(
                $account->getSystemAccountId(),
                $query,
                ['pageSize' => $this->getGoogleServiceManager()::PAGE_SIZE]
            );

            if ($campaignCriterionResponse->getPage()->getPageElementCount() == 0) {
                print("Not found keywords from campaign criteria". PHP_EOL);
                return;
            }

            // Iterates over all rows in all pages and prints the requested field values for
            // the ad group in each row.
            /** @var GoogleAdsRow $googleAdsRow */
            foreach ($campaignCriterionResponse->iterateAllElements() as $googleAdsRow) {
                if (!$googleAdsRow->getCampaignCriterion()->hasNegative()) {
                    continue;
                }

                $fieldsValue = [
                    'systemCampaignId' => $googleAdsRow->getCampaign()->getId(),
                    'systemKeywordId' => $googleAdsRow->getCampaignCriterion()->getCriterionId(),
                    'negative' => true,
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

        print("Count: ". $counterOperations. PHP_EOL);
    }

    /**
     * @param AdwordsAccount    $account
     * @param int               $brandTemplateId
     * @param int               $backendId
     * @param array             $systemAdGroupIds
     * @throws ApiException | ValidationException | ContainerExceptionInterface | MongoDBException | NotFoundExceptionInterface
     */
    private function findByAdgroupIdsAndRemove(
        AdwordsAccount  $account,
        int             $brandTemplateId,
        int             $backendId,
        array           $systemAdGroupIds

    ) {
        $dm = $this->getDocumentManager();

        $counterOperations = 0;
        foreach (array_chunk($systemAdGroupIds, 100) as $systemAdGroupIds) {
            // Creates a query that retrieves keywords.
            $query = sprintf(/** @lang text */
                "SELECT ad_group.id, campaign.id, ad_group_criterion.criterion_id 
                FROM ad_group_criterion 
                WHERE ad_group_criterion.type = KEYWORD 
                AND ad_group_criterion.status != REMOVED 
                AND ad_group.id IN (%s)",
                implode(', ', $systemAdGroupIds)
            );

            $googleAdsServiceClient = $this->getGoogleServiceManager()->getGoogleAdsServiceClient();

            // Issues a search request by specifying page size.
            $adGroupCriterionResponse = $googleAdsServiceClient->search(
                $account->getSystemAccountId(),
                $query,
                ['pageSize' => $this->getGoogleServiceManager()::PAGE_SIZE]
            );

            if ($adGroupCriterionResponse->getPage()->getPageElementCount() == 0) {
                print("Not found keywords from adgroup criteria". PHP_EOL);
                return;
            }

            // Iterates over all rows in all pages and prints the requested field values for
            // the ad group in each row.
            /** @var GoogleAdsRow $googleAdsRow */
            foreach ($adGroupCriterionResponse->iterateAllElements() as $googleAdsRow) {
                $fieldsValue = [
                    'systemAdgroupId' => $googleAdsRow->getAdGroup()->getId(),
                    'systemCampaignId' => $googleAdsRow->getCampaign()->getId(),
                    'systemKeywordId' => $googleAdsRow->getAdGroupCriterion()->getCriterionId(),
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
     * @return AdGroupCriterionOperation|CampaignCriterionOperation
     */
    public function makeDeleteItemOperation(
        int     $customerId,
        int     $criterionId,
        int     $parentId,
        bool    $campaignCriterion = false
    ) {
        if ($campaignCriterion) {
            // Creates campaign criterion resource name.
            $campaignCriterionResourceName = ResourceNames::forCampaignCriterion($customerId, $parentId, $criterionId);

            // Constructs an operation that will remove the keyword with the specified resource name.
            $operation = new CampaignCriterionOperation();
            $operation->setRemove($campaignCriterionResourceName);
        } else {
            // Creates ad group criterion resource name.
            $adGroupCriterionResourceName = ResourceNames::forAdGroupCriterion($customerId, $parentId, $criterionId);

            // Constructs an operation that will remove the keyword with the specified resource name.
            $operation = new AdGroupCriterionOperation();
            $operation->setRemove($adGroupCriterionResourceName);
        }

        return $operation;
    }

    /**
     * @param int                                                       $customerId
     * @param AdGroupCriterionOperation[]|CampaignCriterionOperation[]  $operations
     * @param bool                                                      $campaignCriterion
     *
     * @return bool
     * @throws ContainerExceptionInterface|NotFoundExceptionInterface
     */
    public function pushOperations(int $customerId, array $operations, bool $campaignCriterion = false): bool
    {
        try {
            $googleAdsClient = $this->getGoogleServiceManager();
            // Issues a mutate request to remove the ad group criterion.

            if ($campaignCriterion) {
                $adGroupCriterionServiceClient = $googleAdsClient->getCampaignCriterionServiceClient();
                $response = $adGroupCriterionServiceClient->mutateCampaignCriteria(
                    $customerId,
                    $operations
                );
            } else {
                $adGroupCriterionServiceClient = $googleAdsClient->getAdGroupCriterionServiceClient();
                $response = $adGroupCriterionServiceClient->mutateAdGroupCriteria(
                    $customerId,
                    $operations
                );
            }

            printf(
                "Removed keywords count: '%s'%s",
                $response ->getResults()->count(),
                PHP_EOL
            );

            return true;
        } catch (ApiException $apiException) {
            foreach ($apiException->getMetadata() as $metadatum) {
                foreach ($metadatum['errors'] as $error) {
                    printf('ApiException was thrown with message "%s"',
                        $error['message']. PHP_EOL
                    );
                }
            }

            return false;
        }
    }
}