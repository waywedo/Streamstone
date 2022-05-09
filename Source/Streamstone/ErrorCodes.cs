using Azure;

namespace Streamstone
{
    /// <summary>
    /// Table Service Error Codes.
    /// Streamstone is using ErrorCodes since upgrade to Azure.Data.Tables (12.5.0) because HTTP status codes don't
    /// seem to return the expected values and therefore cannot be used to determine the correct cause of exceptions.
    /// (e.g. EntityAlreadyExists is returned with Bad Request (400) instead of Conflict (409).)
    /// (May be related to use of Azurite for development.)
    /// </summary>
    /// <remarks>
    /// https://docs.microsoft.com/en-us/rest/api/storageservices/Table-Service-Error-Codes
    /// </remarks>
    public enum ErrorCode
    {
        DuplicatePropertiesSpecified,
        EntityNotFound,
        EntityAlreadyExists,
        EntityTooLarge,
        HostInformationNotPresent,
        InvalidDuplicateRow,
        InvalidInput,
        InvalidValueType,
        JsonFormatNotSupported,
        MethodNotAllowed,
        NotImplemented,
        OutOfRangeInput,
        PropertiesNeedValue,
        PropertyNameInvalid,
        PropertyNameTooLong,
        PropertyValueTooLarge,
        TableAlreadyExists,
        TableBeingDeleted,
        TableNotFound,
        TooManyProperties,
        UpdateConditionNotSatisfied,
        XMethodIncorrectCount,
        XMethodIncorrectValue,
        XMethodNotUsingPost,
    }

    public static class ErrorCodeExtensions
    {
        public static bool HasErrorCode(this RequestFailedException ex, ErrorCode errorCode)
        {
            return ex.ErrorCode == errorCode.ToString();
        }
    }
}
