<?php

namespace App\Extensions\AdSystem\Bing\ExternalWork\Bulk;

use App\Entity\BingBatchJob;
use Microsoft\BingAds\V13\Bulk\ResponseMode;
use Exception;
use ZipArchive;

/**
 * Class BatchUploadDownloadHelper
 * @package App\Extensions\AdSystem\Bing\ExternalWork\Bulk
 */
class BatchUploadDownloadHelper
{
    /**
     *
     */
    const FORMAT_VERSION = 6.0;

    /**
     *
     */
    const HEADERS_IN_RESULT_ADD = 3;
    /**
     *
     */
    const HEADERS_IN_RESULT_UPDATE = 2;
    /**
     *
     */
    const HEADERS_IN_RESULT_REMOVE = 2;

    /**
     *
     */
    const UPLOADING_FILE_PREFIX = "UPLOAD";
    /**
     *
     */
    const RESULT_FILE_PREFIX = "RESULT";

    /**
     * Array of fields for making csv file
     *
     * @var array
     */
    public $fields;

    /**
     * AdGroup, Keyword, Ad
     *
     * @var string
     */
    public $operandType;

    /**
     * ADD, UPDATE, REMOVE
     *
     * @var string
     */
    public $action;

    /**
     * The identifier of the customer that owns the account.
     *
     * @var integer
     */
    public $customerId;

    /**
     * The identifier of the account.
     *
     * @var integer
     */
    public $accountId;

    /**
     * Csv file path
     *
     * @var string
     */
    public $filePath;

    /**
     * Zip file path (zip include csv file)
     *
     * @var string
     */
    public $uploadZipFilePath = "";

    /**
     * @var
     */
    public $resultZipFilePath;


    /**
     * BatchUploadDownloadHelper constructor.
     * @param null|int $customerId
     * @param int $accountId
     * @param string $operandType
     * @param string $action
     */
    public function __construct(?int $customerId, int $accountId, string $operandType, string $action)
    {
        $this->customerId = $customerId;
        $this->accountId = $accountId;
        $this->operandType = $operandType;
        $this->action = $action;
    }

    /**
     * Returns directory path for saving csv file
    */
    protected function getDir(): string
    {
        $path = sys_get_temp_dir()."/HOTS/".$this->operandType;

        if (!file_exists($path)) {
            mkdir($path, 0777, true);
        }
        return $path;
    }

    /**
     * Returns file path for saving csv file (include name)
     *
     * @param string $filePath
     * @return string
     */
    protected function getFilePath(string $filePath = ""): string
    {
        if(!empty($filePath)) {
            return $filePath."-".time().".csv";
        } else {
            return $this->getDir()."/".$this->operandType."-".$this->action."-".time().".csv";
        }
    }

    /**
     * Returns full file path for saving zip which including csv file (include name)
     *
     * @param string
     * @return string $filePath
     */
    protected function getUploadZipFilePath(string $filePath = ""): string
    {
        if(!empty($filePath)) {
            return $filePath;
        } else {
            $fullPath = $this->getDir()."/".self::UPLOADING_FILE_PREFIX."-".$this->operandType."-".$this->action.".zip";
            $this->uploadZipFilePath = $fullPath;

            return $this->uploadZipFilePath;
        }
    }

    /**
     * Returns full file path for download zip which including csv file (include name)
     *
     * @param string $filePath
     * @return string
     */
    protected function getResultZipFilePath(string $filePath = ""): string
    {
        if(!empty($filePath)) {
            return $filePath;
        } else {
            $fullPath = $this->getDir()."/".self::RESULT_FILE_PREFIX."-".md5(mt_rand()).".zip";
            $this->resultZipFilePath = $fullPath;

            return $this->resultZipFilePath;
        }
    }

    /**
     * Making csv file by operations
     *
     * @param array $fields
     * @param array $operations
     * @param string $filePath
     * @return $this
    */
    public function makeFile(array $fields, array $operations, string $filePath = ""): BatchUploadDownloadHelper
    {
        $this->fields = $fields;

        $filePath = $this->getFilePath($filePath);
        $file = fopen($filePath, "w");

        foreach ($this->getHeaders() as $header) {
            fputcsv ($file, $header,',');
        }
        foreach($operations as $operation) {
            fputcsv ($file, $operation,',');
        }

        fclose($file);

        $this->filePath = $filePath;

        return $this;
    }

    /**
     * Compress csv file to zip and then removing it
     *
     * @param string $filePath
     * @return $this
     * @throws Exception
    */
    public function compressFileToZip(string $filePath = "")
    {
        if(!empty($this->filePath) && file_exists($this->filePath)) {
            $zipFilePath = $this->getUploadZipFilePath($filePath);

            $archive = new ZipArchive();
            if ($archive->open($zipFilePath, ZipArchive::CREATE | ZipArchive::OVERWRITE) === TRUE) {
                $archive->addFile($this->filePath, basename($this->filePath));
                $archive->close();

                unlink($this->filePath);
            } else {
                unlink($this->filePath);
                throw new Exception ("Compress operation to ZIP file failed.");
            }
        } else {
            $message = "Doesn't exists csv file!\n At first you have make csv file using 'makeFile' function.\n";
            throw new Exception ($message);
        }

        return $this;
    }

    /**
     * Init empty array with keys by fields
     *
     * @return array
     */
    protected function initEmptyArrayByFields(): array
    {
        return array_fill_keys(array_keys($this->fields), '');
    }

    /**
     * Returns headers for csv file
     * File include 3 line of headers (see self::NUMBER_OF_HEADERS)
     *
     * @return array
     */
    protected function getHeaders(): array
    {
        $headers = array();
        $headers[] = $this->fields;
        $headers[] = $this->getFormatVersionHeader();
        $headers[] = $this->getAccountInfoHeader($this->customerId, $this->accountId);

        return $headers;
    }

    /**
     * Returns format version header for csv file
     *
     * @return array
     */
    protected function getFormatVersionHeader(): array
    {
        $formatVersionHeader = $this->initEmptyArrayByFields();
        $formatVersionHeader['type'] = "Format Version";
        $formatVersionHeader['name'] = self::FORMAT_VERSION;

        return $formatVersionHeader;
    }

    /**
     * Returns account info header for csv file
     *
     * @param null|int $customerId
     * @param int $accountId
     * @return array
     */
    protected function getAccountInfoHeader(?int $customerId, int $accountId): array
    {
        $accountInfoHeader = $this->initEmptyArrayByFields();
        $accountInfoHeader['type'] = "Account";
        $accountInfoHeader['id'] = $accountId;
        $accountInfoHeader['parentId'] = $customerId;

        return $accountInfoHeader;
    }

    /**
     * Upload bulk file with operations to Bing
     *
     * @param string $uploadUrl
     * @param string $token
     * @param string $developerToken
     * @return bool
     * @throws Exception
    */
    function uploadFile(string $uploadUrl, string $token, string $developerToken): bool
    {
        if(!empty($this->uploadZipFilePath) && file_exists($this->uploadZipFilePath))
        {
            date_default_timezone_set("UTC");
            $ch = curl_init($uploadUrl);

            curl_setopt($ch, CURLOPT_SSL_VERIFYHOST, 0);
            curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, 0);

            // Set the authorization headers.
            $authorizationHeaders = [];
            $authorizationHeaders[] = "DeveloperToken: " . $developerToken;
            $authorizationHeaders[] = "CustomerId: " . $this->customerId;
            $authorizationHeaders[] = "CustomerAccountId: " . $this->accountId;

            if (!empty($token)) {
                $authorizationHeaders[] = "AuthenticationToken: " . $token;
            } else {
                throw new Exception("Invalid Authentication Token.");
            }

            curl_setopt($ch, CURLOPT_HTTPHEADER, $authorizationHeaders);
            curl_setopt($ch, CURLOPT_RETURNTRANSFER, 1);
            curl_setopt($ch, CURLOPT_POST, 1);

            $file = curl_file_create($this->uploadZipFilePath, "application/zip", "payload.zip");
            curl_setopt($ch, CURLOPT_POSTFIELDS, ["payload" => $file]);

            $result = curl_exec($ch);
            $info = curl_getinfo($ch);
            $http_code = $info['http_code'];

            if (curl_errno($ch)) {
                print "Curl Error: " . curl_error($ch) . "\n";
            } elseif ($http_code != 200) {
                // Whether the curl execution failed, the response could include Bing Ads error codes
                // if the bulk file upload failed, for example in the range of 3220-3227.
                print "Upload Result:\n" . $result . "\n";
                print "HTTP Result Code:\n" . $http_code . "\n";

                /**
                 * {
                 *   "TrackingId":"Tracking-Id-Here",
                 *   "RequestId":"Request-Id-Here",
                 *   "Code":3224,
                 *   "ErrorCode":"BulkServiceUrlAlreadyUsedForUpload",
                 *   "Message":"The URL has already been used for file upload."
                 * }
                 *
                 */
            }

            curl_close($ch);

            if ($http_code == 200) {
                return true;
            } else {
                return false;
            }
        } else {
            $message = "Doesn't exists zip file!\n At first you have to make csv file using 'makeFile' function.\n
                Then compress file to zip using 'compressFileToZip' function.\n";
            throw new Exception ($message);
        }
    }

    /**
     * Returns csv content as string
     *
     * @param boolean $unlinkZip
     * @return mixed
     * @throws Exception
     */
    function getCsvContentFromZip(bool $unlinkZip = true)
    {
        $fromZipArchive = $this->resultZipFilePath;

        if(!empty($fromZipArchive) && file_exists($fromZipArchive)) {
            $zip = new ZipArchive;
            if ($zip->open($fromZipArchive) === TRUE) {
                $csvContent = $zip->getFromIndex(0);
                $zip->close();

                if($unlinkZip) unlink($fromZipArchive);

                return $csvContent;
            } else {
                throw new Exception ("Decompress operations from ZIP file failed.");
            }
        } else {
            $message = "Doesn't exists zip file!\nAt first you have to download file by URL";
            throw new Exception ($message);
        }
    }

    /**
     * Downloads zip archive which include csv file with result
     *
     * @param string $downloadUrl
     * @param string $resultZipFilePath
     * @return $this
     * @throws Exception
     */
    function downloadFile(string $downloadUrl, string $resultZipFilePath = ""): BatchUploadDownloadHelper
    {
        $resultZipFilePath = $this->getResultZipFilePath($resultZipFilePath);

        if (!$reader = fopen($downloadUrl, 'rb')) {
            throw new Exception("Failed to open URL " . $downloadUrl . ".");
        }

        if (!$writer = fopen($resultZipFilePath, 'wb')) {
            fclose($reader);
            throw new Exception("Failed to create ZIP file " . $resultZipFilePath . ".");
        }

        $bufferSize = 100 * 1024;

        while (!feof($reader)) {
            if (false === ($buffer = fread($reader, $bufferSize))) {
                fclose($reader);
                fclose($writer);
                throw new Exception("Read operation from URL failed.");
            }

            if (fwrite($writer, $buffer) === false) {
                fclose($reader);
                fclose($writer);
                throw new Exception ("Write operation to ZIP file failed.");
            }
        }

        fclose($reader);
        fflush($writer);
        fclose($writer);

        return $this;
    }

    /**
     * @param string $action
     * @return int
     */
    static public function getNumberOfHeadersInResultBatch(string $action): int
    {
        $numberOfHeaders = 0;
        // Remove headers from result
        if($action == BingBatchJob::ACTION_ADD) {
            $numberOfHeaders = self::HEADERS_IN_RESULT_ADD;
        } else if ($action == BingBatchJob::ACTION_UPDATE || $action == BingBatchJob::ACTION_REMOVE) {
            $numberOfHeaders = self::HEADERS_IN_RESULT_UPDATE;
        }

        return $numberOfHeaders;
    }

    /**
     * @param string $action
     * @throws Exception
     * @return string
     */
    static public function getResponseMode(string $action): string
    {
        switch ($action) {
            case BingBatchJob::ACTION_ADD:
                return ResponseMode::ErrorsAndResults;
            case BingBatchJob::ACTION_UPDATE:
                return ResponseMode::ErrorsOnly;
            case BingBatchJob::ACTION_REMOVE:
                return ResponseMode::ErrorsOnly;
            default:
                throw new Exception ("Unknown action during bulk uploading!");
        }
    }
}