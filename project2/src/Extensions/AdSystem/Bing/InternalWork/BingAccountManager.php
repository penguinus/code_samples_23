<?php

namespace App\Extensions\AdSystem\Bing\InternalWork;

use App\Entity\BingAccount;
use App\Entity\BrandKeyword;
use App\Entity\KcCampaign;
use App\Extensions\Common\AdSystemEnum;
use App\Extensions\Common\InternalWork\Basic\AccountManager;
use App\Interfaces\EntityInterface\SystemAccountInterface;
use Doctrine\ORM\EntityManagerInterface;
use Microsoft\BingAds\V13\CustomerManagement\GetAccountsInfoRequest;
use Psr\Container\ContainerInterface;
use App\Enums\Account\AccountLifeCycleStatus;

/**
 * Class BingAccountManager
 * @package App\Extensions\AdSystem\Bing\InternalWork
 */
class BingAccountManager extends AccountManager
{
    public const KEYWORDS_LIMIT = 3500000;

    /**
     * @var ContainerInterface
     */
    protected ContainerInterface $container;

    /**
     * BingAccountManager constructor.
     *
     * @param ContainerInterface        $container
     * @param EntityManagerInterface    $em
     */
    public function __construct(ContainerInterface $container, EntityManagerInterface $em)
    {
        parent::__construct($container, AdSystemEnum::BING, $em);

        $this->container = $container;
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
     * @param string $name
     * @return mixed
     */
    public function checkUniqueName(string $name): bool
    {
        return $this->getEntityManager()->getRepository(BingAccount::class)->checkExistsByName($name);
    }

    /**
     * @return BingAccount
     */
    public function getModel(): SystemAccountInterface
    {
        return new BingAccount();
    }

    /**
     * @return int
     */
    public function getKeywordLimitInAccount(): int
    {
        return self::KEYWORDS_LIMIT;
    }

    /**
     * @return array
     */
    public function getActiveAccountIds(): array
    {
        $array = $this->getAccounts();

        return array_map(function ($item) {
            return $item->Id;
        }, $array);
    }

    /**
     * @return array
     */
    public function getAccounts(): array
    {
        $managedCustomerService = $this->get('bing.service_manager')->getCustomerManagementService();

        $request = new GetAccountsInfoRequest();
        $request->CustomerId = $managedCustomerService->getCustomerId();

        $array = (array)($managedCustomerService->GetService()->GetAccountsInfo($request));

        return $array['AccountsInfo']->AccountInfo;
    }

    /**
     * @param array $accounts
     * @return array
     */
    public function getActiveAccounts(array $accounts): array
    {
        $result = [];
        /** @var \stdClass $account */
        foreach ($accounts as $account) {
            $account->AccountLifeCycleStatus != AccountLifeCycleStatus::INACTIVE
            && $result[] = $this->getAccountFields($account);
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
        /** @var \stdClass $account */
        foreach ($accounts as $account) {
            $account->AccountLifeCycleStatus == AccountLifeCycleStatus::INACTIVE
            && $result[] = $this->getAccountFields($account);
        }
        return $result;
    }

    /**
     * @param \stdClass $account
     * @return array
     */
    private function getAccountFields(\stdClass $account): array
    {
        return ['systemAccountId' => $account->Id, 'name' => $account->Name];
    }

    /**
     * @param BingAccount $account
     * @return int
     * @throws \Exception
     */
    public function getAmountKeywordsByAccount(SystemAccountInterface $account): int
    {
        $em = $this->getEntityManager();
        $dm = $this->getDocumentManager();

        $attributes = ['accounts' => $account->getId()];
        $selectFields = ['backendId'];
        $kcCampaignInMongo = $dm->getRepository(\App\Document\KcCampaign::class)
            ->getByAttributes($dm, $attributes, $selectFields, true);

        if(empty($kcCampaignInMongo))
            return 0;

        $backendIds = array_column($kcCampaignInMongo, 'backendId');

        $attributes = ['backendId' => $backendIds];
        $kcCampaignsInMySql = $em->getRepository(KcCampaign::class)
            ->findByAttributesIn($attributes, [], true, true);

        $backendIdsByTemplate = [];
        foreach ($kcCampaignsInMySql as $kcCampaignInMySql) {
            $backendIdsByTemplate[$kcCampaignInMySql['brand_template_id']][] = $kcCampaignInMySql['backendId'];
        }

        $amountKeywordsByAccount = 0;
        foreach ($backendIdsByTemplate as $brandTemplateId => $backendIds) {
            $amountKeywords = $em->getRepository(BrandKeyword::class)
                ->getCountKeywordsByBrandTemplate($brandTemplateId);

            $builder = $dm->createAggregationBuilder('\App\Document\KcCampaign');
            $countCampaigns = $builder
                ->hydrate(false)
                ->match()
                    ->field('backendId')->in($backendIds)
                ->unwind('$campaigns')
                ->match()
                    ->field('campaigns.systemAccount')->equals($account->getId())
                ->count('count')
                ->getAggregation()->getIterator()->toArray();

            $countCampaigns = $countCampaigns[0]['count'] ?? 0;

            $amountKeywordsByAccount += ($amountKeywords * $countCampaigns);
        }

        return $amountKeywordsByAccount;
    }
}