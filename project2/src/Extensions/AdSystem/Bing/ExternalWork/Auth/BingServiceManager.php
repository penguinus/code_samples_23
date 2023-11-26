<?php

namespace App\Extensions\AdSystem\Bing\ExternalWork\Auth;

use App\Extensions\AdSystem\Bing\ExternalWork\Exception\RefreshTokenExpiredException;
use App\Services\SlackSenderService;
use Microsoft\BingAds\Auth\AuthorizationData;
use Microsoft\BingAds\Auth\OAuthDesktopMobileAuthCodeGrant;
use Microsoft\BingAds\Auth\OAuthScope;
use Microsoft\BingAds\Auth\OAuthTokenRequestException;
use Microsoft\BingAds\Auth\ServiceClient;
use Microsoft\BingAds\Auth\ServiceClientType;
use Psr\Container\ContainerInterface;
use Symfony\Component\Cache\Adapter\AdapterInterface;
use Symfony\Contracts\Service\ServiceSubscriberInterface;


/**
 * Class BingServiceManager
 */
final class BingServiceManager implements ServiceSubscriberInterface
{
    private const REFRESH_TOKEN_CACHE_EXPIRE = 60 * 60 * 24 * 30; # 30 days

    /**
     * The Bing Ads developer access token.
     * Used as the DeveloperToken header element in calls to the Bing Ads web services.
     */
    private $developerToken; // For sandbox use BBD37VB98

    /**
     * The Sandbox or Production.
     */
    private $apiEnvironment;

    /**
     * It's Application Id which we're getting after register app
     */
    private $clientId;

    /**
     * The identifier of the customer that owns the account.
     * Used as the CustomerId header element in calls to the Bing Ads web services.
     */
    private $customerId;

    /**
     * The password
     */
    private $clientSecret;

    /**
     * The value in the path of the request to control who can sign in to the application.
     */
    private $tenant;

    /**
     * The authorization service calls back to your application with the redirection URI,
     * which includes an authorization code
     */
    private $redirectUri;

    /**
     * Refresh token path
    */
    private $OAuthRefreshTokenPath;

    /**
     * @var AuthorizationData
     */
    private $authorizationData;

    /**
     * @var ServiceClient
     */
    private $managementProxy;

    /**
     * Existing environment
     *
     * @var string
     */
    private $environment;

    /**
     * @var string
     */
    private $projectDir;

    /**
     * @var string
     */
    private $serverName;

    /**
     * @var string
     */
    private $serviceName;

    /**
     * @var array
     */
    protected $services = [];

    /**
     * @var ContainerInterface
     */
    private $container;


    /**
     * BingServiceManager constructor.
     * @param string $serverName
     * @param string $rootDir
     * @param string $env
     * @param ContainerInterface $container
     */
    public function __construct(string $rootDir, string $env, string $serverName, ContainerInterface $container)
    {
        $this->serverName = $serverName;
        $this->projectDir = $rootDir;
        $this->environment = $env;
        $this->container = $container;
        $this->OAuthRefreshTokenPath = "{$this->projectDir}/../../bing_config/auth/bing_auth_refresh.txt";
        $this->setConfig();
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
     * @return string
     */
    public function getServerName(): string
    {
        return $this->serverName;
    }

    /**
     * Get developerToken.
     * @return string
     */
    public function getDeveloperToken(): string
    {
        return $this->developerToken;
    }

    /**
     * Get developerToken.
     * @return string
     */
    public function getCustomerId(): string
    {
        return $this->customerId;
    }

    /**
     * Get developerToken.
     * @return string
     */
    public function getAccessToken()
    {
        return $this->authorizationData->Authentication->OAuthTokens->AccessToken;
    }

    /**
     * @return array
     */
    public static function getSubscribedServices(): array
    {
        return [
            'cache' => AdapterInterface::class,
            'slack_sender_service' => SlackSenderService::class,
        ];
    }

    /**
     * @param string $id
     * @return mixed
     */
    protected function get($id)
    {
        return $this->container->get($id);
    }

    /**
     * @return AdapterInterface
     */
    protected function getCache(): AdapterInterface
    {
        return $this->get('cache');
    }

    /**
     * @return SlackSenderService
     */
    protected function getSlackSenderService(): SlackSenderService
    {
        return $this->container->get('slack_sender_service');
    }

    /**
     * Gets the value for the specified setting name.
     *
     * @param $name
     * @return mixed
     * @throws \Exception
     */
    protected function getConfigurationData($name)
    {
        $configIniFilePath = dirname($this->projectDir). '/config/bing/bing_auth.ini';

        if (!file_exists($configIniFilePath)) {
            throw new \Exception(
                sprintf(
                    "Config file not found as specified: '%s' or in the home directory: '%s'.",
                    'bing_auth.ini',
                    $configIniFilePath
                )
            );
        }

        $configurationData = parse_ini_file($configIniFilePath);

        if (array_key_exists(ucfirst($name), $configurationData)) {
            return $configurationData[ucfirst($name)];
        } else {
            throw new \Exception(
                sprintf(
                    "Config file not found as specified setting name: '%s'.",
                    $name
                )
            );
        }
    }

    /**
     * Set config by environment
     */
    protected function setConfig()
    {
        $this->developerToken   = $this->getConfigurationData('DeveloperToken');
        $this->apiEnvironment   = $this->getConfigurationData('ApiEnvironment');
        $this->clientId         = $this->getConfigurationData('ClientId');
        $this->customerId       = $this->getConfigurationData('CustomerId');
        $this->tenant           = $this->getConfigurationData('Tenant');
        $this->redirectUri      = $this->getConfigurationData('RedirectUri');

        if ($this->environment == "prod") {
            $this->clientSecret = $this->getConfigurationData('ClientSecret');
        }
    }


    /**
     * @param $service_name
     * @param null $accountId
     * @return mixed
     */
    public function getService($service_name, $accountId = null)
    {
        #TODO Temporary fix. Refactor after checking result.
        return $this->Authenticate($service_name, $accountId);

        /*if (empty($this->services[$service_name][$accountId])) {
            $this->services[$service_name][$accountId] = $this->Authenticate($service_name, $accountId);
        }

        return $this->services[$service_name][$accountId];*/
    }

    /**
     * @param null $accountId
     * @return mixed
     */
    public function getCustomerManagementService($accountId = null)
    {
        return $this->getService(ServiceClientType::CustomerManagementVersion13, $accountId);
    }

    /**
     * @param null $accountId
     * @return mixed
     */
    public function getCampaignManagementService($accountId = null)
    {
        return $this->getService(ServiceClientType::CampaignManagementVersion13, $accountId);
    }

    /**
     * @param null $accountId
     * @return mixed
     */
    public function getBulkService($accountId = null)
    {
        return $this->getService(ServiceClientType::BulkVersion13, $accountId);
    }

    /**
     * @param string $serviceName
     * @param null $accountId
     * @return ServiceClient
     */
    public function Authenticate(string $serviceName, $accountId = null)
    {
        // Authenticate for Bing Ads services with a Microsoft Account.
        $this->serviceName = $serviceName;

        $this->AuthenticateWithOAuth();

        if(!empty($accountId))
            $this->authorizationData->withAccountId($accountId);

        $this->managementProxy = new ServiceClient(
            $serviceName,
            $this->authorizationData,
            $this->GetApiEnvironment()
        );

        return $this->managementProxy;
    }

    /**
     * Return null if service isn't initialized (call getService())
     *
     * @return ServiceClient|null
     * @throws \Exception
     */
    public function refreshLastServiceClient()
    {
        if(isset($this->managementProxy)) {
            $this->managementProxy = new ServiceClient(
                $this->serviceName,
                $this->authorizationData,
                $this->GetApiEnvironment()
            );

            return $this->managementProxy;
        } else {
            return null;
        }
    }

    /**
     * Sets the global authorization data instance with OAuthWebAuthCodeGrant or OAuthDesktopMobileAuthCodeGrant.
     * Set authorization data
     *
     * @throws RefreshTokenExpiredException
     * @throws \Psr\Cache\InvalidArgumentException
     */
    protected function AuthenticateWithOAuth()
    {
        if (isset($this->authorizationData)) # Already authenticated
            return;

        $authentication = (new OAuthDesktopMobileAuthCodeGrant())
            ->withClientId($this->clientId)
            ->withEnvironment($this->apiEnvironment)
            ->withOAuthScope(OAuthScope::MSADS_MANAGE)
            ->withTenant($this->tenant)
            ->withRedirectUri($this->redirectUri)
        ;

        if ($this->environment == "prod") {
            $authentication->withClientSecret($this->clientSecret);
        }

        $this->authorizationData = (new AuthorizationData())
            ->withAuthentication($authentication)
            ->withDeveloperToken($this->developerToken)
            ->withCustomerId($this->customerId);

        try {
            $refreshToken = $this->getRefreshTokenFromCache();

            # Try get from file
            if ($refreshToken == null) {
                $refreshToken = $this->ReadOAuthRefreshToken();
                $this->storeRefreshTokenToCache($refreshToken);
            }

            if ($refreshToken != null) {
                $this->authorizationData->Authentication->RequestOAuthTokensByRefreshToken($refreshToken);

                if(exec('whoami') !== "www-data") {
                    $this->WriteOAuthRefreshToken($this->authorizationData->Authentication->OAuthTokens->RefreshToken);
                }
            } else {
                $this->sendTokenExpiredNotification();

                if (exec('whoami') !== "www-data") {
                    $this->RequestUserConsent();
                } else {
                    throw new RefreshTokenExpiredException();
                }
            }
        } catch(OAuthTokenRequestException $e) {
            $this->sendTokenExpiredNotification();

            if (exec('whoami') !== "www-data") {
                printf("Error: %s\n", $e->Error);
                printf("Description: %s\n", $e->Description);

                $this->RequestUserConsent();
            } else {
                throw new RefreshTokenExpiredException($e->Description);
            }
        }
    }

    /**
     * Request for get a new tokens (AccessToken and RefreshToken)
     * @throws \Psr\Cache\InvalidArgumentException
     */
    protected function RequestUserConsent()
    {
        print "You need to provide consent for the application to access your Bing Ads accounts. " .
            "Copy and paste this authorization endpoint into a web browser and sign in with a Microsoft account " .
            "with access to a Bing Ads account: \n\n" . $this->authorizationData->Authentication->GetAuthorizationEndpoint() .
            "\n\nAfter you have granted consent in the web browser for the application to access your Bing Ads accounts, " .
            "please enter the response URI that includes the authorization 'code' parameter: \n\n";

        $responseUri = fgets(STDIN);
        print "\n";

        $this->authorizationData->Authentication->RequestOAuthTokensByResponseUri(trim($responseUri));
        $this->WriteOAuthRefreshToken($this->authorizationData->Authentication->OAuthTokens->RefreshToken);
        $this->storeRefreshTokenToCache($this->authorizationData->Authentication->OAuthTokens->RefreshToken);

        $this->removeNotificationSentMarker();
    }

    /**
     * @return mixed
     */
    public function GetApiEnvironment()
    {
        return $this->apiEnvironment;
    }

    /**
     * @return bool|null|string
     */
    public function ReadOAuthRefreshToken()
    {
        $refreshToken = null;

        if (file_exists($this->OAuthRefreshTokenPath) && filesize($this->OAuthRefreshTokenPath) > 0)
        {
            $refreshTokenFile = @\fopen($this->OAuthRefreshTokenPath,"r");
            $refreshToken = fread($refreshTokenFile, filesize($this->OAuthRefreshTokenPath));
            fclose($refreshTokenFile);
        }

        return $refreshToken;
    }

    /**
     * @return string|null
     * @throws \Psr\Cache\InvalidArgumentException
     */
    protected function getRefreshTokenFromCache()
    {
        $rTInCache = $this->getCache()->getItem('bing_refresh_token');

        if (!$rTInCache->isHit()) {
            return null;
        } else {
            return $rTInCache->get();
        }
    }

    /**
     * @param $refreshToken
     */
    public function WriteOAuthRefreshToken($refreshToken)
    {
        $refreshTokenFile = \fopen($this->OAuthRefreshTokenPath,"wb");
        if (file_exists($this->OAuthRefreshTokenPath) && !empty($refreshToken))
        {
            fwrite($refreshTokenFile, $refreshToken);
            fclose($refreshTokenFile);
        }

        return;
    }

    /**
     * @param $refreshToken
     * @throws \Psr\Cache\InvalidArgumentException
     */
    protected function storeRefreshTokenToCache($refreshToken)
    {
        $rTInCache = $this->getCache()->getItem('bing_refresh_token');
        $rTInCache->set($refreshToken);
        $rTInCache->expiresAfter(self::REFRESH_TOKEN_CACHE_EXPIRE);
        $this->getCache()->save($rTInCache);
    }

    /**
     * @throws \Psr\Cache\InvalidArgumentException
     */
    protected function sendTokenExpiredNotification()
    {
        if (!$this->getServerName() == "production") {
            return false;
        }

        $notificationSent = $this->getCache()->getItem('notification_token_expired_sent');
        if (!$notificationSent->isHit()) {

            $message = 'HOTS Error: Bing Authentication Refresh Token has Expired!'
                . " Sent from the '{$this->getServerName()}' server.";

            try {
                $slackSenderService = $this->getSlackSenderService();

                if ($slackSenderService->sendCustomizedMessagesToSlack($message) !== FALSE) {
                    $notificationSent->set(1);
                    $this->getCache()->save($notificationSent);
                }
            } catch (\Exception $e) {
                print $e->getMessage(). PHP_EOL;
            }

        }
    }

    /**
     * @throws \Psr\Cache\InvalidArgumentException
     */
    private function removeNotificationSentMarker()
    {
        if ($this->getServerName() == "production") {
            $this->getCache()->deleteItem('notification_token_expired_sent');
        }
    }
}