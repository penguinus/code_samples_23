<?php

namespace App\Extensions\AdSystem\AdWords\InternalWork;

use App\Document\CampaignProcess;
use App\Document\KeywordsQueue;
use App\Entity\BrandKeyword;
use App\Extensions\AdSystem\AdWords\ExternalWork\AdWordsAccount;
use App\Extensions\AdSystem\AdWords\ExternalWork\AdWordsErrorDetail;
use App\Extensions\AdSystem\AdWords\ExternalWork\Auth\AdWordsServiceManager;
use App\Interfaces\EntityInterface\SystemAccountInterface;
use Doctrine\ODM\MongoDB\MongoDBException;
use Doctrine\ORM\EntityManagerInterface;
use Doctrine\ORM\NonUniqueResultException;
use Doctrine\ORM\NoResultException;
use Google\Ads\GoogleAds\V13\Enums\CustomerStatusEnum\CustomerStatus;
use Google\Ads\GoogleAds\V13\Services\GoogleAdsRow;
use App\Extensions\Common\AdSystemEnum;
use App\Extensions\Common\InternalWork\Basic\AccountManager;
use Google\ApiCore\ApiException;
use Google\ApiCore\ValidationException;
use Psr\Container\ContainerInterface;

/**
 * Class AdWordsAccountManager
 *
 * @package App\Extensions\AdSystem\AdWords\InternalWork
 */
class AdWordsAccountManager extends AccountManager
{
    public const KEYWORDS_LIMIT = 3500000;

    /**
     * @var ContainerInterface
     */
    protected ContainerInterface $container;

    /**
     * @var AdWordsAccount
     */
    protected AdWordsAccount $accountManager;

    /**
     * AdWordsAccountManager constructor.
     *
     * @param ContainerInterface        $container
     * @param EntityManagerInterface    $em
     * @param AdWordsAccount            $accountManager
     */
    public function __construct(ContainerInterface $container, EntityManagerInterface $em, AdWordsAccount $accountManager)
    {
        parent::__construct($container, AdSystemEnum::ADWORDS, $em);

        $this->container        = $container;
        $this->accountManager   = $accountManager;
    }

    /**
     * @param string $id
     * @return mixed
     */
    public function get($id)
    {
        return $this->container->get($id);
    }


    /**
     * @return AdWordsAccount
     */
    protected function getAccountManager(): AdWordsAccount
    {
        return $this->accountManager;
    }

    /**
     * @return int
     */
    public function getKeywordLimitInAccount(): int
    {
        return self::KEYWORDS_LIMIT;
    }

    /**
     * @return SystemAccountInterface
     */
    public function getModel(): SystemAccountInterface
    {
        return new \App\Entity\AdwordsAccount();
    }

    /**
     * @return array
     * @throws ValidationException
     */
    public function getAccounts(): array
    {
        return $this->getAccountManager()->getAccounts();
    }

    /**
     * @param array $accounts
     *
     * @return array
     */
    public function getActiveAccounts(array $accounts): array
    {
        $result = [];

        if (isset($accounts['active'])) {
            /** @var array $account */
            foreach ($accounts['active'] as $account) {
                $result[] = $this->getAccountFields($account);
            }
        }

        return $result;
    }

    /**
     * @param array $accounts
     * @return array
     */
    public function getInactiveAccounts(array $accounts): array
    {
        $result = [];

        if (isset($accounts['inactive'])) {
            /** @var array $account */
            foreach ($accounts['inactive'] as $account) {
                $result[] = $this->getAccountFields($account);
            }
        }

        return $result;
    }

    /**
     * @param array $account
     * @return array
     */
    private function getAccountFields(array $account): array
    {
        return ['systemAccountId' => $account['customerId'], 'name' => $account['name']];
    }

    /**
     * @param string $name
     *
     * @return mixed
     */
    public function checkUniqueName(string $name): bool
    {
        return $this->getEntityManager()->getRepository(\App\Entity\AdwordsAccount::class)->checkExistsByName($name);
    }

    /**
     * @param SystemAccountInterface $account
     *
     * @return int
     * @throws MongoDBException|NoResultException|NonUniqueResultException|ValidationException
     */
    public function getAmountKeywordsByAccount(SystemAccountInterface $account): int
    {
        $accountExternal = $this->getAccountManager();

        $amountKeywords = $accountExternal->getAmountKeywords($account->getSystemAccountId());

        return $this->getAmountNotUploadedKeywords($account, $amountKeywords);
    }

    /**
     * @param SystemAccountInterface    $account
     * @param int                       $keywords
     *
     * @return int
     * @throws MongoDBException|NoResultException|NonUniqueResultException
     */
    protected function getAmountNotUploadedKeywords(SystemAccountInterface $account, int $keywords): int
    {
        $dm = $this->getDocumentManager();
        $em = $this->getEntityManager();

        $keywords = $keywords + $dm->createQueryBuilder(KeywordsQueue::class)
                ->count()
                ->field('systemAccount')->equals($account->getId())
                ->field('add')->exists(true)
                ->getQuery()
                ->execute();

        //TODO REMOVE THAT TO REPOSITORY?

        $amountByTemplates = $dm->createAggregationBuilder(CampaignProcess::class)
            ->hydrate(false)
            ->match()
            ->field('systemAccount')->equals($account->getId())
            ->field('add')->exists(true)
            ->field('queuesGenerated')->exists(false)
            ->group()
            ->field('id')->expression('$brandTemplateId')
            ->field('amountCampaigns')->sum(1)
            ->execute()->toArray();

        $notGeneratedKeywords = 0;
        if (!empty($amountByTemplates)) {

            foreach ($amountByTemplates as $amountByTemplate) {
                $brandTemplateKeywords = $em->getRepository(BrandKeyword::class)
                    ->getCountKeywordsByBrandTemplate($amountByTemplate['_id']);

                $notGeneratedKeywords += $brandTemplateKeywords * $amountByTemplate['amountCampaigns'];
            }
        }

        return $keywords + $notGeneratedKeywords;
    }

    /**
     * @return array
     * @throws ValidationException
     */
    public function getActiveAccountIds(): array
    {
        // Creates the Google Ads Service client.
        /** @var AdWordsServiceManager $googleServiceClient */
        $googleServiceClient = $this->get('adwords.service_manager');

        // Creates a query that retrieves all child accounts of the manager specified in search
        $query = /** @lang text */
            "SELECT customer_client.id, customer_client.test_account, customer_client.status, customer_client.manager 
            FROM customer_client 
            WHERE customer_client.level = 1";

        $customers = [];
        try {
            $googleAdsServiceClient = $googleServiceClient->getGoogleAdsServiceClient();

            // Issues a search request by specifying page size.
            $customerClientResponse = $googleAdsServiceClient->search(
                $googleServiceClient->getClientBuilder()->getLoginCustomerId(),
                $query,
                ['pageSize' => $googleServiceClient::PAGE_SIZE]
            );

            // Iterates over all elements to get all customer clients under the specified customer's
            // hierarchy.
            foreach ($customerClientResponse->iterateAllElements() as $googleAdsRow) {
                /** @var GoogleAdsRow $googleAdsRow */
                if ($googleAdsRow->getCustomerClient()->getManager()
                    && $googleAdsRow->getCustomerClient()->getStatus() != CustomerStatus::ENABLED) {
                    continue;
                }

                $customers[] = $googleAdsRow->getCustomerClient()->getId();

            }
        } catch (ApiException $apiException) {
            foreach ($apiException->getMetadata() as $metadatum) {
                foreach ($metadatum['errors'] as $error) {
                    if (!strpos(AdWordsErrorDetail::errorDetail($error["message"]), "internal error")) {
                        $this->getAirbrakeNotifier()->notify(new \Exception(
                            '[Google] ApiException was thrown with message - '
                            . $error["message"] .', when the process was running "getActiveAccountIds()" '. PHP_EOL
                        ));
                    }
                }
            }
        }

        return $customers;
    }
}