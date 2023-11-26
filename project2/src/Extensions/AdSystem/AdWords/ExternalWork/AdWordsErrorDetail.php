<?php

namespace App\Extensions\AdSystem\AdWords\ExternalWork;


use App\Extensions\Common\ExternalWork\ErrorDetailInterface;

/**
 * Class AdWordsErrorDetail
 * @package App\Extensions\AdSystem\AdWords\ExternalWork
 */
class AdWordsErrorDetail implements ErrorDetailInterface
{
    /**
     * @param int|string $error
     * @return int|string
     */
    public static function errorDetail($error): string
    {
        if (strpos($error, "AdError.INVALID_INPUT") !== false)
            return "One of the fields in an ad contains invalid characters.";
        elseif (strpos($error, "AdError.LINE_TOO_WIDE") !== false)
            return "One of the lines in an ad was longer than the maximum allowed length."
                . " The length limits are documented in the https://support.google.com/adwords/answer/1704389";
        elseif (strpos($error, "AdGroupAdError.CANNOT_OPERATE_ON_REMOVED_ADGROUPAD") !== false)
            return "An operation attempted to update a removed ad.";
        elseif (strpos($error, "AdGroupCriterionError.INVALID_KEYWORD_TEXT") !== false)
            return "The keyword text contains invalid characters. A list of valid characters is available"
                . " on the https://support.google.com/adwords/answer/2453980#symbol";
        elseif (strpos($error, "AdGroupServiceError.DUPLICATE_ADGROUP_NAME") !== false)
            return "An ad group is being added or renamed, but the name is already being used by another ad group.";
        elseif (strpos($error, "AuthenticationError.CUSTOMER_NOT_FOUND") !== false)
            return "No account found for the customer ID provided in the header.";
        elseif (strpos($error, "AuthenticationError.GOOGLE_ACCOUNT_COOKIE_INVALID") !== false)
            return "The access token in the request header is either invalid or has expired.";
        elseif (strpos($error, "AuthenticationError.NOT_ADS_USER") !== false)
            return "The login used to generate the access token is not associated with any AdWords account.";
        elseif (strpos($error, "AuthorizationError.UNABLE_TO_AUTHORIZE") !== false)
            return "There was an issue with completing authorization for a given user/account.";
        elseif (strpos($error, "AuthorizationError.USER_PERMISSION_DENIED") !== false)
            return "There is no link between the MCC account authenticated in the request"
                . " and the client account specified in the headers.";
        elseif (strpos($error, "BiddingError.BID_TOO_HIGH_FOR_DAILY_BUDGET") !== false)
            return "The bid on a keyword or ad group is higher than the daily budget of the campaign.";
        elseif (strpos($error, "BiddingError.BID_TOO_MANY_FRACTIONAL_DIGITS") !== false)
            return "The bid value is not an exact multiple of the minimum CPC. For example, US$0.015 is not a valid bid.";
        elseif (strpos($error, "BiddingError.BID_TOO_BIG") !== false)
            return "The error is returned even though the bid is within the campaign budget.";
        elseif (strpos($error, "BiddingError.CANNOT_SET_SITE_MAX_CPC") !== false)
            return "The siteMaxCpc field can not be set to a non-zero value.";
        elseif (strpos($error, "BulkMutateJobError.PAYLOAD_STORE_UNAVAILABLE") !== false)
            return "The results of a bulk mutate job cannot be found or are temporarily unavailable.";
        elseif (strpos($error, "CampaignError.DUPLICATE_CAMPAIGN_NAME") !== false)
            return "A campaign is being added or renamed, but the name is already being used by another campaign.";
        elseif (strpos($error, "CriterionError.AD_SCHEDULE_EXCEEDED_INTERVALS_PER_DAY_LIMIT") !== false)
            return "The number of AdSchedule entries in a day exceeds the limit.";
        elseif (strpos($error, "CustomerSyncError.TOO_MANY_CHANGES") !== false)
            return "There were too many changed entities to return.";
        elseif (strpos($error, "DatabaseError.CONCURRENT_MODIFICATION") !== false)
            return "Multiple processes are trying to update the same entity at the same time.";
        elseif (strpos($error, "DistinctError.DUPLICATE_ELEMENT") !== false)
            return "The request contains two parameters that are identical and redundant.";
        elseif (strpos($error, "EntityNotFound.INVALID_ID") !== false)
            return "The ID of the entity you are operating on isn't valid.";
        elseif (strpos($error, "InternalApiError.UNEXPECTED_INTERNAL_API_ERROR") !== false)
            return "Something unexpected happened while processing the request.";
        elseif (strpos($error, "JobError.TOO_LATE_TO_CANCEL_JOB") !== false)
            return "The bulk mutate job can no longer be cancelled.";
        elseif (strpos($error, "NotEmptyError.EMPTY_LIST") !== false)
            return "A required list is empty.";
        elseif (strpos($error, "NotWhitelistedError.CUSTOMER_ADS_API_REJECT") !== false)
            return "The account is temporarily in read-only mode.";
        elseif (strpos($error, "OperationAccessDenied.ADD_OPERATION_NOT_PERMITTED") !== false)
            return "The ADD operator can't be used for operations in the service and target account.";
        elseif (strpos($error, "OperationAccessDenied.MUTATE_ACTION_NOT_PERMITTED_FOR_CLIENT") !== false)
            return "The attempted operation cannot be performed through the API.";
        elseif (strpos($error, "CriterionPolicyError") !== false && strpos($error, "PolicyViolationError") !== false)
            return "A keyword you are adding violates an AdWords policy.";
        elseif (strpos($error, "PolicyViolationError") !== false)
            return "An ad you are adding violates an AdWords policy.";
        elseif (strpos($error, "QuotaCheckError.ACCOUNT_DELINQUENT") !== false)
            return "The MCC account that was used to register for API access does not have an active billing mechanism.";
        elseif (strpos($error, "QuotaCheckError.ACCOUNT_INACCESSIBLE") !== false)
            return "The target account has been temporarily disabled due to suspicious activity and cannot be accessed "
                . " via the API.";
        elseif (strpos($error, "QuotaCheckError.TERMS_AND_CONDITIONS_NOT_SIGNED") !== false)
            return "You have not yet signed the Terms & Conditions.";
        elseif (strpos($error, "QuotaCheckError.DEVELOPER_TOKEN_NOT_APPROVED") !== false)
            return "You are using an unapproved developer token to make calls against a production account.";
        elseif (strpos($error, "QuotaCheckError.INCOMPLETE_SIGNUP") !== false)
            return "The developer token has not been approved and you're trying to use it for requests against"
                . " a production account.";
        elseif (strpos($error, "QuotaCheckError.INVALID_TOKEN_HEADER") !== false)
            return "OThe developer token in the request is missing or invalid.";
        elseif (strpos($error, "QuotaCheckError.MONTHLY_BUDGET_REACHED") !== false)
            return "The API monthly budget limit has been exceeded.";
        elseif (strpos($error, "QuotaCheckError.QUOTA_EXCEEDED") !== false)
            return "A system frequency limit has been exceeded.";
        elseif (strpos($error, "RangeError.TOO_LOW") !== false)
            return "A value was lower than the minimum allowed.";
        elseif (strpos($error, "RateExceededError.RATE_EXCEEDED") !== false)
            return "Too many requests were made to the API in a short period of time.";
        elseif (strpos($error, "ReportDefinitionError.CUSTOMER_SERVING_TYPE_REPORT_MISMATCH") !== false)
            return "The type of report definition being created isn't compatible with the account type.";
        elseif (strpos($error, "ReportInfoError.INVALID_USER_ID_IN_HEADER") !== false)
            return "The client is using an invalid user or effective user ID in the header.";
        elseif (strpos($error, "RequestError.INVALID_INPUT") !== false)
            return "The request is malformed.";
        elseif (strpos($error, "RequiredError.REQUIRED") !== false)
            return "The request is missing required information.";
        elseif (strpos($error, "SizeLimitError.RESPONSE_SIZE_LIMIT_EXCEEDED") !== false)
            return "The response contains too many items.";
        elseif (strpos($error, "EntityCountLimitExceeded.ADGROUP_LIMIT") !== false)
            return "EntityCountLimitExceeded.ADGROUP_LIMIT.";
        elseif (strpos($error, "CriterionError.KEYWORD_HAS_TOO_MANY_WORDS") !== false) {
            return "The keyword has too many words";
        } elseif (strpos($error, "AdGroupCriterionError.CANNOT_MODIFY_URL_FIELDS_WITH_DUPLICATE_ELEMENTS") !== false) {
            return "Not allowed to modify url fields of an ad group criterion if there are duplicate"
                . " elements for that ad group criterion in the request.";
        } elseif (strpos($error, "A transient internal error has occurred.") !== false) {
            return "Google Ads API encountered an unexpected transient internal error."
                . " The user should retry their request in these cases.";
        } elseif (strpos($error, "An internal error has occurred.") !== false) {
            return "Google Ads API encountered unexpected internal error.";
        } elseif (strpos($error, "Too many requests. Retry in 900 seconds.") !== false) {
            return "Google Ads API encountered too many requests for one customer.";
        } else {
            return self::extracted($error);
        }

    }

    /**
     * @param $error
     * @return string
     */
    public static function extracted($error): string
    {
        $error_message = substr($error, stripos($error, "errorString") + 17);
        $error_message = substr($error_message, 0, stripos($error_message, "'"));
        if (!empty($error_message)) {
            return $error_message;
        } else {
            return $error;
        }
    }
}