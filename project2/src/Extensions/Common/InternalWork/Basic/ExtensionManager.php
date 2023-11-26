<?php

namespace App\Extensions\Common\InternalWork\Basic;

use App\Document\Extension;
use App\Document\ExtensionsQueue;
use App\Document\KcCampaign;
use App\Entity\Occasion;
use App\Extensions\Common\AdSystemEnum;
use App\Extensions\Common\ContentType;
use App\Extensions\Common\InternalWork\Interfaces\ExtensionManagerInterface;
use App\Interfaces\EntityInterface\SyncAdModelInterface;
use App\Services\Sync\SyncChangesWithCampaigns;
use Doctrine\ODM\MongoDB\DocumentManager;
use Doctrine\ORM\EntityManagerInterface;
use Psr\Container\ContainerInterface;
use Symfony\Contracts\Service\ServiceSubscriberInterface;

/**
 * Class ExtensionManager
 * @package App\Extensions\Common\InternalWork\Basic
 */
abstract class ExtensionManager implements ExtensionManagerInterface
{
    const PROCESS_KC_CAMPAIGNS_LIMIT = 15;

    /**
     * @var ContainerInterface
     */
    protected ContainerInterface $container;

    /**
     * @var SyncChangesWithCampaigns
     */
    protected SyncChangesWithCampaigns $syncChangesWithCampaigns;

    /**
     * @var string
     */
    protected $adSystem;

    /**
     * @var object
     */
    protected $dm;

    public function __construct(
                                 $adSystem,
        ContainerInterface       $container,
        DocumentManager          $dm,
        SyncChangesWithCampaigns $syncChangesWithCampaigns
    ) {
        $this->adSystem                 = strtolower($adSystem);
        $this->container                = $container;
        $this->syncChangesWithCampaigns = $syncChangesWithCampaigns;

        $dm->setAdSystem($this->adSystem);
        $this->dm = $dm;
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
     * @return \Doctrine\ORM\EntityManager
     */
    protected function getEntityManager()
    {
        return $this->get('doctrine');
    }

    /**
     * @return DocumentManager
     */
    protected function getDocumentManager()
    {
        $this->dm->setAdSystem($this->adSystem);

        return $this->dm;
    }

    /**
     * @return SyncChangesWithCampaigns
     */
    protected function getSyncChangesWithCampaigns(): SyncChangesWithCampaigns
    {
        return $this->syncChangesWithCampaigns;
    }

    /**
     * @return array
     */
    abstract protected function getMethodsListForExtensions();

    /**
     * @param iterable $extensions
     * @return array
     */
    protected function getRemovableExtensionsGroupedData(iterable $extensions)
    {
        $result = [];
        /** @var ExtensionsQueue $extension */
        foreach ($extensions as $extension) {
            $backendId   = $extension->getKcCampaignBackendId();
            $accountId   = $extension->getSystemAccount();
            $extensionId = $extension->getSystemExtensionId();
            $campaignId  = $extension->getSystemCampaignId();

            if (isset($result[$backendId])) {
                if (isset($result[$backendId][$accountId])) {
                    if (isset($result[$backendId][$accountId][$extensionId])) {
                        !in_array($campaignId, $result[$backendId][$accountId][$extensionId]['campaigns']) &&
                        $result[$backendId][$accountId][$extensionId]['campaigns'][] = $campaignId;
                    } else {
                        $result[$backendId][$accountId][$extensionId] = [
                            'extension' => $extension,
                            'campaigns' => [$campaignId]
                        ];
                    }
                } else {
                    $result[$backendId][$accountId] = [
                        $extensionId => [
                            'extension' => $extension,
                            'campaigns' => [$campaignId]
                        ]
                    ];
                }
            } else {
                $result[$backendId] = [
                    $accountId => [
                        $extensionId => [
                            'extension' => $extension,
                            'campaigns' => [$campaignId]
                        ]
                    ]
                ];
            }
        }

        return $result;
    }

    /**
     * @return array|\Doctrine\ODM\MongoDB\Iterator\Iterator|int|\MongoDB\DeleteResult|\MongoDB\InsertOneResult|\MongoDB\UpdateResult|object|null
     * @throws \Doctrine\ODM\MongoDB\MongoDBException
     */
    protected function getExtensionsForUpload()
    {
        $backendIds = $this->dm->createAggregationBuilder(ExtensionsQueue::class)
            ->hydrate(false)
            ->match()
            ->field('systemCampaignId')->exists(true)
            ->field('systemExtensionId')->exists(false)
            ->field('add')->exists(true)
            ->field('error')->exists(false)
            ->group()
            ->field('id')->expression('$kcCampaignBackendId')
            ->execute()->toArray();

        # Processing only 7 Kc Campaigns for one time.
        $backendIds = array_slice(array_column($backendIds, '_id'),0,self::PROCESS_KC_CAMPAIGNS_LIMIT);

        $builder = $this->dm->createAggregationBuilder(ExtensionsQueue::class);
        $documentIds = $builder->hydrate(false)
            ->match()
            ->field('kcCampaignBackendId')->in($backendIds)
            ->field('systemCampaignId')->exists(true)
            ->field('systemExtensionId')->exists(false)
            ->field('add')->exists(true)
            ->field('error')->exists(false)
            ->group()
            ->field('id')->expression(
                $builder->expr()
                    ->field('backendId')->expression('$kcCampaignBackendId')
                    ->field('systemAccount')->expression('$systemAccount')
                    ->field('teId')->expression('$teId')
            )
            ->field('documentId')->first('$id')
            ->execute()->toArray();

        $documentIds = array_column($documentIds, 'documentId');

        $extensions = $this->dm->createQueryBuilder(ExtensionsQueue::class)
            ->field('id')->in($documentIds)
            ->getQuery()
            ->execute();

        return $extensions;
    }

    protected function checkAndDeleteIfAlreadyUploaded(array $extensions, int $backendId, int $accountId)
    {
        $uploadedExtIds = $this->dm->getRepository(Extension::class)
            ->getUploadedExtensionsByKcCampaign($this->dm, $backendId, $accountId);

        $counter = 1;
        /** @var ExtensionsQueue $ext */
        foreach ($extensions as $teId => $ext) {
            if (
                empty($ext->getSystemExtensionId())
                && isset($uploadedExtIds[$ext->getTeId()])
                && $uploadedExtIds[$ext->getTeId()]['systemCampaignId'] == $ext->getSystemCampaignId()
            ) {
                $systemExtension = $uploadedExtIds[$ext->getTeId()];

                $ext->setSystemExtensionId($systemExtension['systemExtensionId']);

                if (isset($systemExtension['systemFeedId'])) {
                    $ext->setSystemFeedId($systemExtension['systemFeedId']);
                }

                $this->dm->persist($ext);
                if (($counter % 100) === 0) {
                    $this->dm->flush();
                    $this->dm->clear();
                }

                $counter++;

                unset($extensions[$teId]);
            }
        }

        $this->dm->flush();
        $this->dm->clear();

        return $extensions;
    }

    protected function getExtensionsForRemove()
    {
        $backendIds = $this->dm->createAggregationBuilder(ExtensionsQueue::class)
            ->hydrate(false)
            ->match()
            ->field('systemCampaignId')->exists(true)
            ->field('systemExtensionId')->exists(true)
            ->field('delete')->exists(true)
            ->field('error')->exists(false)
            ->group()
            ->field('id')->expression('$kcCampaignBackendId')
            ->execute()->toArray();

        # Processing only 10 Kc Campaigns for one time.
        $backendIds = array_slice(array_column($backendIds, '_id'),0,10);

        $builder = $this->dm->createAggregationBuilder(ExtensionsQueue::class);

        return $builder->hydrate(false)
            ->match()
                ->field('kcCampaignBackendId')->in($backendIds)
                ->field('systemCampaignId')->exists(true)
                ->field('systemExtensionId')->exists(true)
                ->field('delete')->exists(true)
                ->field('error')->exists(false)
            ->group()
                ->field('id')->expression(
                    $builder->expr()
                        ->field('systemCampaignId')->expression('$systemCampaignId')
                        ->field('category')->expression('$category')
                )
                ->field('systemAccountId')->first('$systemAccount')
                ->field('systemExtensionIds')->addToSet('$systemExtensionId')
            ->execute()->toArray();
    }


    /**
     * @param $extension
     * @return mixed
     * @throws \Exception
     */
    private function executeMethodByExtension($extension)
    {
        $array = $this->getMethodsListForExtensions();

        if (key_exists($extension->getCategory(), $array)) {
            $methodName = "get" . $array[$extension->getCategory()];

            if (method_exists($this, $methodName)) {
                return $this->{$methodName}($extension);
            } else {
                throw new \Exception(
                    "The method for creating an extension of the corresponding category is not implemented.");
            }
        } else {
            throw new \Exception("The specified category is not supported. Category id: {$extension->getCategory()}");
        }
    }

    /**
     * @param $hotsExtension
     * @return null
     */
    public function makeSystemExtension($hotsExtension)
    {
        return $this->executeMethodByExtension($hotsExtension);
    }

    /**
     * @param array $item
     * @param \App\Entity\KcCampaign $kcCampaignInMySql
     * @return mixed|void
     * @throws \Doctrine\ODM\MongoDB\MongoDBException
     */
    public function checkNeedUpdateAndUpdate(array $item, \App\Entity\KcCampaign $kcCampaignInMySql)
    {
        $dm = $this->getDocumentManager();

        /** @var KcCampaign $kcCampaignImMongo */
        $kcCampaignImMongo = $dm->getRepository(KcCampaign::class)
            ->getByAttributesOne($dm, ['backendId' => $item['ID']], ['promotionType', 'promotionValue']);

        if (empty($kcCampaignImMongo))
            return;

        if ($kcCampaignImMongo->getPromotionType() != KcCampaign::CAMPAIGN_WITHOUT_PROMOTION_TYPE &&
            ($kcCampaignImMongo->getPromotionType() != $item['OFFEREXTENSIONTYPEID']) ||
            ($kcCampaignImMongo->getPromotionTypeValue() != filter_var($item['OFFEREXTENSIONVALUE'], FILTER_SANITIZE_NUMBER_INT))
        ) {
            $em = $this->getEntityManager();

            $request = [
                'promotionType' => $item['OFFEREXTENSIONTYPEID'],
                'promotionTypeValue' => filter_var($item['OFFEREXTENSIONVALUE'], FILTER_SANITIZE_NUMBER_INT),
            ];

            $placeholders = [
                'promotionType' => $kcCampaignImMongo->getPromotionType(),
                'needAddToQueue' => true
            ];

            // Get campaigns for sync
            $campaigns = $dm->getRepository(KcCampaign::class)
                ->getCampaignsForSyncByBackendId($dm, $kcCampaignInMySql->getBackendId(), $request);

            # DELETE current promotion extensions
            $this->updatePromotionExtensions(
                $campaigns,
                $kcCampaignInMySql->getBrandTemplate()->getId(),
                SyncAdModelInterface::STATUS_SYNC_DELETE,
                $placeholders
            );

            # ADD new promotions extensions
            $this->updatePromotionExtensions(
                $campaigns,
                $kcCampaignInMySql->getBrandTemplate()->getId(),
                SyncAdModelInterface::STATUS_SYNC_ADD,
                $request
            );

            $where = ['backendId' => $item['ID']];
            $attributes = ['promotionType' => $request['promotionType'], 'promotionValue' => $request['promotionTypeValue']];
            $dm->getRepository(KcCampaign::class)->updateByAttributes($dm, $where, $attributes);
        }
    }

    /**
     * @param array$campaigns
     * @param int $brandTemplate
     * @param bool $approvedOperationType
     * @param array $placeholders
     * @throws \Doctrine\ORM\NonUniqueResultException
     */
    public function updatePromotionExtensions(
        array $campaigns,
        int $brandTemplateId,
        int $approvedOperationType,
        array $placeholders
    ) {
        $em = $this->getEntityManager();

        $syncChangesService = $this->getSyncChangesWithCampaigns()->setAdSystem($this->adSystem);

        $extension = $em->getRepository('App:BrandExtension')
            ->getPromotionalByBrandTemplateAndTypeOne(
                $brandTemplateId,
                $placeholders['promotionType'],
                true,
                $this->adSystem
            );

        $extensions = [];
        if (!empty($extension)) {
            $extension = array_map(function ($element) use ($em, $placeholders, $approvedOperationType) {
                $element['promotionType'] = $placeholders['promotionType'];
                $element['approveTypeOperation'] = $approvedOperationType;

                switch ($approvedOperationType) {
                    case SyncAdModelInterface::STATUS_SYNC_ADD:
                        $element['promotionTypeValue'] = $placeholders['promotionTypeValue'];

                        break;
                    case SyncAdModelInterface::STATUS_SYNC_DELETE:
                        if (isset($placeholders['needAddToQueue']) && $placeholders['needAddToQueue']) {
                            $element['needAddToQueue'] = true;
                        }

                        break;
                    default:
                        throw new \Exception(
                            'Not supported type operation :' . $approvedOperationType .
                            ' use const SyncAdModelInterface sync type only Add or Delete'
                        );
                }

                if ((bool)$element['occasion']) {
                    $occasion = $em->getRepository(Occasion::class)->findOneBy(['id' => $element['occasion']]);
                    if ($occasion) {
                        $element['occasion'] = $occasion->getValue();
                    }
                }
                return $element;
            }, $extension);

            $extensions['items'][$extension[0]['id']] = $extension[0];

            if ($approvedOperationType == SyncAdModelInterface::STATUS_SYNC_DELETE) {
                $extensions['itemsNeedSystemId'][] = $extension[0]['id'];
            }

            $brandTemplateChanges[ContentType::EXTENSION] = $extensions;

            $syncChangesService->syncChanges($campaigns, $brandTemplateId, $brandTemplateChanges);
        }
    }
}