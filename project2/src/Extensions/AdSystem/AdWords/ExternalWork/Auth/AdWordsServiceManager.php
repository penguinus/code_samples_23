<?php

namespace App\Extensions\AdSystem\AdWords\ExternalWork\Auth;

use Google\Ads\GoogleAds\Lib\OAuth2TokenBuilder;
use Google\ApiCore\PathTemplate;
use Google\ApiCore\ValidationException;
use Google\Ads\GoogleAds\Lib\V13\{GoogleAdsClient, GoogleAdsClientBuilder};
use Google\Ads\GoogleAds\V13\Services\{
    AdGroupAdServiceClient, AdGroupBidModifierServiceClient, AdGroupCriterionServiceClient,
    AdGroupServiceClient, AdServiceClient, AssetServiceClient, BatchJobServiceClient, CampaignAssetServiceClient,
    CampaignBidModifierServiceClient, CampaignBudgetServiceClient, CampaignCriterionServiceClient, CampaignServiceClient,
    CustomerServiceClient, GoogleAdsServiceClient};
use Google\Auth\Credentials\{ServiceAccountCredentials, UserRefreshCredentials};
use Monolog\Handler\StreamHandler;
use Monolog\Logger;

/**
 * Class AdWordsServiceManager
 *
 * @package App\Extensions\AdSystem\AdWords\ExternalWork\Auth
 */
final class AdWordsServiceManager
{
    /**
     *
     */
    public const PAGE_LIMIT = 500;

    /*
     * Are limited to 1,000 of resources contained in the underlying API response
     */
    public const PAGE_SIZE = 1000;

    /**
     * Requests that are paginated, such as search requests, are also subject to the
     * Page size cannot exceed 10,000 rows limitation and are rejected if it violates this limit,
     * with the error: INVALID_PAGE_SIZE.
     * read to: https://developers.google.com/google-ads/api/docs/best-practices/quotas#paginated_requests
     */
    public const PAGINATED_LIMIT = 10000;

    /** Max 3500000 */
    public const KEYWORDS_LIMIT = 3500000;

    /**
     * All developer tokens, including those with Standard Access, are limited to 1,000 get requests per day
     * read to: https://developers.google.com/google-ads/api/docs/best-practices/quotas#get_requests
     */
    private const GET_REQUESTS = 1000;

    /**
     * A mutate request cannot contain more than 5,000 operations per request.
     * API return error "TOO_MANY_MUTATE_OPERATIONS"
     * read to: https://developers.google.com/google-ads/api/docs/best-practices/quotas#mutate_requests
     */
    public const MUTATE_OPERATIONS_LIMIT = 5000;

    /** Max 10000 */
    public const SELECT_IN_LIMIT = 5000;

    /**
     * @var string
     */
    private string $google_version = 'v13';

    /**
     * @var GoogleAdsClientBuilder
     */
    private GoogleAdsClientBuilder $clientBuilder;

    /**
     * @var array
     */
    protected array $services = [];

    /**
     * Existing environment
     *
     * @var string
     */
    private string $environment;

    /**
     * @var string
     */
    protected string $projectDir;

    /**
     * @var Logger
     */
    private Logger $logger;


    /**
     * AdWordsServiceManager constructor.
     * @param string $rootDir
     * @param string $env
     */
    public function __construct(string $rootDir, string $env)
    {
        $this->environment = $env;
        $this->projectDir = $rootDir;
        // Create a logger instance to be used by both API clients.
        $this->logger = (new Logger("migration-examples-logger"))
            ->pushHandler(new StreamHandler("php://stderr", Logger::INFO));

        // Set builder foк Google API
        $this->clientBuilder = $this->setClient();
    }

    /**
     * @return string
     */
    public function getEnv(): string
    {
        return $this->environment;
    }

    /**
     * @return string
     */
    public function getProjectDir(): string
    {
        return $this->projectDir;
    }

    /**
     * @return GoogleAdsClientBuilder
     */
    public function getClientBuilder(): GoogleAdsClientBuilder
    {
        return $this->clientBuilder;
    }

    /**
     * @return string
     */
    public function getGoogleVersion(): string
    {
        return $this->google_version;
    }


    /**
     * Builds a Google Ads client to be used for Google Ads API calls.
     *
     * @return GoogleAdsClientBuilder
     */
    protected function setClient(): GoogleAdsClientBuilder
    {
        return (new GoogleAdsClientBuilder())
            ->fromFile(dirname($this->projectDir). '/config/google/google_auth.ini')
            ->withOAuth2Credential($this->getGoogleToken())
            ->withLogger($this->logger);
    }

    /**
     * Generate a refreshable OAuth2 credential to be used for authentication against the Google Ads API.
     *
     * @return ServiceAccountCredentials|UserRefreshCredentials
     */
    protected function getGoogleToken()
    {
        return (new OAuth2TokenBuilder())
            ->fromFile(dirname($this->projectDir). '/config/google/google_auth.ini')
            ->build();
    }

    /**
     * @param $service_name
     * @return mixed
     */
    protected function getGoogleService($service_name)
    {
        if (empty($this->services[$service_name])) {
            $clientBuilder = $this->getClientBuilder();

            if (!isset($this->services[$service_name])) {
                $this->services[$service_name] = [];
            }

            $this->services[$service_name] = (new googleAdsClient($clientBuilder))->{'get'. $service_name}();
        }
        return $this->services[$service_name];
    }

    /**
     * @param string $path
     * @param string $resourceName
     *
     * @return array
     * @throws ValidationException
     */
    public function parseName(string $path, string $resourceName): array
    {
        $pathTemplate = new PathTemplate($path);
        return $pathTemplate->match($resourceName);
    }

    /**
     * @return GoogleAdsServiceClient
     */
    public function getGoogleAdsServiceClient(): GoogleAdsServiceClient
    {
        return $this->getGoogleService('GoogleAdsServiceClient');
    }

    /**
     * @return CampaignBudgetServiceClient
     */
    public function getCampaignBudgetServiceClient(): CampaignBudgetServiceClient
    {
        return $this->getGoogleService('CampaignBudgetServiceClient');
    }
    /**
     * @return CampaignBidModifierServiceClient
     */
    public function getCampaignBidModifierServiceClient(): CampaignBidModifierServiceClient
    {
        return $this->getGoogleService('CampaignBidModifierServiceClient');
    }

    /**
     * @return AdGroupBidModifierServiceClient
     */
    public function getAdGroupBidModifierServiceClient(): AdGroupBidModifierServiceClient
    {
        return $this->getGoogleService('AdGroupBidModifierServiceClient');
    }

    /**
     * @return CampaignServiceClient
     */
    public function getCampaignServiceClient(): CampaignServiceClient
    {
        return $this->getGoogleService('CampaignServiceClient');
    }

    /**
     * @return CampaignCriterionServiceClient
     */
    public function getCampaignCriterionServiceClient(): CampaignCriterionServiceClient
    {
        return $this->getGoogleService('CampaignCriterionServiceClient');
    }

    /**
     * @return AdGroupCriterionServiceClient
     */
    public function getAdGroupCriterionServiceClient(): AdGroupCriterionServiceClient
    {
        return $this->getGoogleService('AdGroupCriterionServiceClient');
    }

    /**
     * @return AdGroupServiceClient
     */
    public function getAdGroupServiceClient(): AdGroupServiceClient
    {
        return $this->getGoogleService('AdGroupServiceClient');
    }

    /**
     * @return AdGroupAdServiceClient
     */
    public function getAdGroupAdServiceClient(): AdGroupAdServiceClient
    {
        return $this->getGoogleService('AdGroupAdServiceClient');
    }

    /**
     * @return AdServiceClient
     */
    public function getAdServiceClient(): AdServiceClient
    {
        return $this->getGoogleService('AdServiceClient');
    }

    /**
     * @return BatchJobServiceClient
     */
    public function getBatchJobServiceClient(): BatchJobServiceClient
    {
        return $this->getGoogleService('BatchJobServiceClient');
    }

    /**
     * @return CustomerServiceClient
     */
    public function getCustomerServiceClient(): CustomerServiceClient
    {
        return $this->getGoogleService('CustomerServiceClient');
    }

    /**
     *
     * @return AssetServiceClient
     */
    public function getAssetServiceClient(): AssetServiceClient
    {
        return $this->getGoogleService('AssetServiceClient');
    }

    /**
     *
     * @return CampaignAssetServiceClient
     */
    public function getCampaignAssetServiceClient(): CampaignAssetServiceClient
    {
        return $this->getGoogleService('CampaignAssetServiceClient');
    }
}
