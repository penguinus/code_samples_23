<?php

namespace App\Extensions\Common\InternalWork\Basic;

use App\Document\Extension;
use App\Entity\BrandExtension;
use App\Entity\BrandTemplate;
use App\Enums\Ads\AdEnum;
use App\Enums\Keyword\MatchType;
use App\Providers\ProviderEntityFieldName;
use Psr\Container\ContainerInterface;

class GenerateCampaignContentManager
{
    use \App\Traits\SetterTrait, \App\Traits\FlushTrait;

    private const KEYWORDS_QUEUE_LIMIT = 5000000;

    /** @var \Doctrine\ODM\MongoDB\DocumentManager */
    private $dm;

    /** @var \Doctrine\ORM\EntityManager */
    private $em;

    /** @var string */
    private $adSystem;

    public function __construct(ContainerInterface $container)
    {
        $this->dm = $container->get('doctrine_mongodb')->getManager();
        $this->em = $container->get('doctrine')->getManager();
    }

    public function setAdSystem(string $adSystem)
    {
        $this->adSystem = $adSystem;
        $this->dm->setAdSystem($adSystem);
    }

    public function addCampaignContentsToQueues(): void
    {
        $keywordsNumber = $this->dm->getRepository(\App\Document\KeywordsQueue::class)->getCountByAttributes($this->dm, []);
        if ($keywordsNumber > self::KEYWORDS_QUEUE_LIMIT) {
            return;
        }

        $campaignProcesses = $this->dm->getRepository(\App\Document\CampaignProcess::class)->getListByQueuesAndAdd($this->dm);

        /** @var \App\Document\CampaignProcess $campaignProcess */
        foreach ($campaignProcesses as $campaignProcess) {
            $amountItems = 0;

            if (empty($campaignProcess->getCampaignId())) {
                continue;
            }

            /** @var \App\Entity\KcCampaign $kcCampaignInMysql */
            $kcCampaignInMysql = $this->em->getRepository(\App\Entity\KcCampaign::class)
                ->findOneBy(['backendId' => $campaignProcess->getBackendId()]);

            if (empty($kcCampaignInMysql)) {
                continue;
            }

            $this->dm->setBrandTemplateId($campaignProcess->getBrandTemplateId());

            /** @var \App\Document\KcCampaign $kcCampaignInMongo */
            $kcCampaignInMongo = $this->dm->getRepository(\App\Document\KcCampaign::class)
                ->getByAttributesOne(
                    $this->dm,
                    [
                        'backendId' => $campaignProcess->getBackendId(),
                        'campaigns.cityId' => $campaignProcess->getCityId()
                    ],
                    [
                        'backendId',
                        'amountItemsInQueue',
                        'amountItemsForUpload',
                        'promotionType',
                        'promotionValue',
                        'budget',
                        'uniqueAdgroupsParams'
                    ]
                );
            if (empty($kcCampaignInMongo)) {
                continue;
            }

            if (!$this->em->getRepository(\App\Entity\BrandTemplate::class)->tryLock($kcCampaignInMysql->getBrandTemplate())) {
                throw new \App\Exceptions\TemplateLockedException('Locked for modifications. Please try again later.');
            }

            $extensionsNumber = $this->addExtensionsToQueue($kcCampaignInMysql->getBrandTemplate()->getId(), $kcCampaignInMongo, $campaignProcess);
            $amountItems += $extensionsNumber;

            $adgroupsNumber = $this->addAdgroupsToQueue($kcCampaignInMysql->getBrandTemplate()->getId(), $kcCampaignInMongo, $campaignProcess);
            $amountItems += $adgroupsNumber;

            $adsNumber = $this->addAdsToQueue($kcCampaignInMysql->getBrandTemplate()->getId(), $campaignProcess);
            $amountItems += $adsNumber;

            $keywordsNumber = $this->addKeywordsToQueue($kcCampaignInMysql->getBrandTemplate()->getId(), $campaignProcess);
            $amountItems += $keywordsNumber;

            $kcCampaignInMongo->updateUploadingStatistic($amountItems);
            $kcCampaignInMongo->setStatusWaitSync(null);
            $this->dm->persist($kcCampaignInMongo);

            $campaignProcess->setQueuesGenerated(true);
            $this->dm->persist($campaignProcess);

            $this->dm->flush();
            $this->dm->clear();

            $this->em->flush();
            $this->em->clear();
        }
    }

    private function addExtensionsToQueue(
        int $brandTemplateId,
        \App\Document\KcCampaign $kcCampaignInMongo,
        \App\Document\CampaignProcess $campaignProcess
    ): int
    {
        $uploadedExtIds = $this->dm->getRepository(Extension::class)
            ->getUploadedExtensionsByKcCampaign(
                $this->dm, $kcCampaignInMongo->getBackendId(), $campaignProcess->getSystemAccount());

        $extensions = $this->em->getRepository(\App\Entity\BrandExtension::class)
            ->getApprovedItemsByTemplate($brandTemplateId, $kcCampaignInMongo, [], $this->adSystem);

        foreach ($extensions as $key => $value) {
            $document = new \App\Document\ExtensionsQueue();
            $document->fill([
                'category' => $value['category'],
                'status' => $value['status'],
                'cityId' => $campaignProcess->getCityId(),
                'systemAccount' => $campaignProcess->getSystemAccount(),
                'brandTemplateId' => $campaignProcess->getBrandTemplateId(),
                'kcCampaignBackendId' => $campaignProcess->getBackendId(),
                'campaignId' => $campaignProcess->getCampaignId(),
                'add' => true,
                'teId' => $value['id'],
            ]);

            # Check if such extension has already uploaded
            if (isset($uploadedExtIds[$value['id']])) {
                $systemExtension = $uploadedExtIds[$value['id']];

                $document->setSystemExtensionId($systemExtension['systemExtensionId']);
                if (isset($systemExtension['systemFeedId'])) {
                    $document->setSystemFeedId($systemExtension['systemFeedId']);
                }
            }

            $document = $this->fillWithValidationOnIsset($document, $value, ['callout', 'devicePreference', 'platformTargeting', 'comments']);

            $array = \App\Document\ExtensionsQueue::getHelperArray();

            foreach ($array as $categoryId => $item) {
                if ($value['category'] == $categoryId) {
                    $data = [];
                    foreach ($item['data'] as $datum) {
                        if (isset($value[$datum]) && !empty($value[$datum])) {
                            $data[$datum] = $value[$datum];
                        /** if phone to adsystem not set process skip and unset document */
                        } else if ($categoryId == BrandExtension::PHONE) {
                            unset($document);
                            continue(2);
                        }
                    }
                    $document->fill($data);
                    if ($item['needValidate']) {
                        $document = $this->fillWithValidationOnIsset($document, $value, $item['validateArray']);
                    }
                }
            }

            /**
             * after add content according to adsystem, it is necessary to check the document,
             * because the document can be unset
             */
            if (isset($document)) {
                $this->dm->persist($document);
            }

            if ($this->checkNeedFlush($key)) {
                $this->dm->flush();
                $this->dm->clear();
            }
        }
        $this->dm->flush();

        return count($extensions);
    }

    private function addAdgroupsToQueue(
        int $brandTemplateId,
        \App\Document\KcCampaign $kcCampaignInMongo,
        \App\Document\CampaignProcess $campaignProcess
    ): int {
        $brandAdgroups = $this->em->getRepository(\App\Entity\BrandAdgroup::class)
            ->getApprovedItemsByTemplate($brandTemplateId);

        $kcCampaignAdgroups = $kcCampaignInMongo->getUniqueAdgroupsParams();
        foreach ($kcCampaignAdgroups as $kcCampaignAdgroup) {
            if (isset($brandAdgroups[$kcCampaignAdgroup->getTeId()])) {
                $brandAdgroups[$kcCampaignAdgroup->getTeId()]['defaultCpc'] = (string)$kcCampaignAdgroup->getCpcBid();
                $brandAdgroups[$kcCampaignAdgroup->getTeId()]['status'] = $kcCampaignAdgroup->getStatus();
            }
        }

        foreach ($brandAdgroups as $brandAdgroup) {
            $document = new \App\Document\AdgroupsQueue;
            $document->fill([
                'adgroupId' => $brandAdgroup['adgroupId'],
                'adgroup' => $brandAdgroup['adgroup'],
                'defaultCpc' => $brandAdgroup['defaultCpc'],
                'status' => $brandAdgroup['status'],
                'systemAccount' => $campaignProcess->getSystemAccount(),
                'brandTemplateId' => $campaignProcess->getBrandTemplateId(),
                'kcCampaignBackendId' => $campaignProcess->getBackendId(),
                'campaignId' => $campaignProcess->getCampaignId(),
                'add' => true,
                'teId' => $brandAdgroup['id'],
            ]);

            $document = $this->fillWithValidationOnEmpty($document, $brandAdgroup, ['destinationUrl', 'comments']);

            $this->dm->persist($document);
        }
        $this->dm->flush();

        return count($brandAdgroups);
    }

    private function addAdsToQueue(int $brandTemplateId, \App\Document\CampaignProcess $campaignProcess): int
    {
        $brandAdgroups = $this->em->getRepository(\App\Entity\BrandAdgroup::class)
            ->getApprovedItemsByTemplate($brandTemplateId);

        $ads = $this->em->getRepository(\App\Entity\Ads\Ad::class)
            ->getApprovedItemsByTemplate(array_column($brandAdgroups, 'adgroup'), $brandTemplateId);

        foreach ($ads as $key => $value) {
            if (!array_key_exists(ucfirst(strtoupper($value['type'])), AdEnum::TYPES)) {
                unset($ads[$key]);
                continue;
            }

            $document = new \App\Document\AdsQueue();
            $document->fill([
                'headline0' => $value['headline0'],
                'headline1' => $value['headline1'],
                'path1' => $value['path1'],
                'path2' => $value['path2'],
                'desc0' => $value['desc0'],
                'destinationUrl' => $value['destinationUrl'],
                'status' => $value['status'],
                'systemAccount' => $campaignProcess->getSystemAccount(),
                'brandTemplateId' => $campaignProcess->getBrandTemplateId(),
                'kcCampaignBackendId' => $campaignProcess->getBackendId(),
                'campaignId' => $campaignProcess->getCampaignId(),
                'adgroup' => $value['adgroup'],
                'adgroupId' => $value['custom_adgroup_id'],
                'adType' => $value['type'],
                'add' => true,
                'teId' => $value['id'],
                'teAdgroupId' => $value['brand_adgroup_id'],
            ]);

            if(isset($value['adgroup_placeholder'])) {
                $document->fill([
                    'adgroupPlaceholder' => $value['adgroup_placeholder'],
                ]);
            }

            if ($value['type'] == \App\Enums\Ads\AdEnum::TYPE_RSA) {
                $document->fill([
                    'pinFirstDescriptions' => \App\Entity\Ads\Ad::mutateListToInt($value['pinFirstDescriptions']),
                    'pinSecondDescriptions' => \App\Entity\Ads\Ad::mutateListToInt($value['pinSecondDescriptions']),
                    'pinFirstHeadlines' => \App\Entity\Ads\Ad::mutateListToInt($value['pinFirstList']),
                    'pinSecondHeadlines' => \App\Entity\Ads\Ad::mutateListToInt($value['pinSecondList']),
                    'pinThirdHeadlines' => \App\Entity\Ads\Ad::mutateListToInt($value['pinThirdList']),
                ]);
            }

            $document = $this->fillWithValidationOnEmpty($document, $value, [
                'comments', 'headline2', 'headline3', 'headline4', 'headline5', 'headline6', 'headline7',
                'headline8','headline9','headline10','headline11','headline12','headline13', 'headline14',
                'desc1', 'desc2', 'desc3'
            ]);

            $this->dm->persist($document);

            if ($this->checkNeedFlush($key)) {
                $this->dm->flush();
                $this->dm->clear();
            }
        }
        $this->dm->flush();

        return count($ads);
    }

    private function addKeywordsToQueue(int $brandTemplateId, \App\Document\CampaignProcess $campaignProcess): int
    {
        $keywords = $this->em->getRepository(\App\Entity\BrandKeyword::class)->getApprovedItemsByTemplate($brandTemplateId);

        foreach ($keywords as $key => $value) {
            if (!array_key_exists($value['matchType'], MatchType::MATCH_TYPES)) {
                unset($keywords[$key]);
                continue;
            }

            $document = new \App\Document\KeywordsQueue;
            $document->fill([
                'keyword' => $value['keyword'],
                'matchType' => $value['matchType'],
                'systemAccount' => $campaignProcess->getSystemAccount(),
                'brandTemplateId' => $campaignProcess->getBrandTemplateId(),
                'kcCampaignBackendId' => $campaignProcess->getBackendId(),
                'campaignId' => $campaignProcess->getCampaignId(),
                'add' => true,
                'teId' => $value['id'],
            ]);

            if (isset($value['brand_adgroup_id'])) {
                $document->fill(['teAdgroupId' => $value['brand_adgroup_id']]);
            }

            if (!$value['negative']) {
                $document->fill([
                    'adgroup' => $value['adgroup'],
                    'status' => $value['status'],
                ]);
            }

            $document = $this->fillWithValidationOnEmpty($document, $value, ['negative', 'maxCpc', 'destinationUrl', 'comments']);

            $this->dm->persist($document);

            if ($this->checkNeedFlush($key)) {
                $this->dm->flush();
                $this->dm->clear();
            }
        }
        $this->dm->flush();

        return count($keywords);
    }
}
