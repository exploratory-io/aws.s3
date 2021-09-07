#' @rdname get_folder
#' @title List bucket contents
#' @description List the S3 folders as a data frame
#' @template bucket
#' @param bucket Character string that limits the response to folders under the bucket.
#' @param prefix Character string that limits the response to keys that begin with the specified prefix
#' @template dots
#' @details From the AWS doc: \dQuote{This implementation of the GET operation returns some or all (up to 1000) of the objects in a bucket. You can use the request parameters as selection criteria to return a subset of the objects in a bucket.} The \code{max} and \code{marker} arguments can be used to retrieve additional pages of results. Values from a call are store as attributes
#' @return \code{get_folder} returns a data frame
#' @examples
#' \dontrun{
#'   # basic usage
#'   b <- bucketlist()
#'   get_bucket(b[1,1])
#'   get_bucket_df(b[1,1])
#'
#'   # bucket names with dots
#'   ## this (default) should work:
#'   get_bucket("this.bucket.has.dots", url_style = "path")
#'   ## this probably wont:
#'   #get_bucket("this.bucket.has.dots", url_style = "virtual")
#' }
#'
#' @references \href{https://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketGET.html}{API Documentation}
#' @seealso \code{\link{bucketlist}}, \code{\link{get_object}}
#' @export
#' @importFrom utils tail
get_folder <- function(bucket, prefix = NULL, ...) {
  # To get all folders, pass Inf.
  max <- Inf
  # 1000 is limit per call.
  limit <- 1000
  query <- list(prefix = prefix, delimiter = "/", "max-keys" = limit, marker = NULL)
  result <- aws.s3::s3HTTP(verb = "GET", bucket = bucket, query = query, parse_response = TRUE, ...)
  shouldStop <- FALSE
  nextMarker <- ""
  # Handle pagination for large result set.
  # if IsTruncted is true, need to send another request to get the remaining result.
  # NOTE: There are two cases where you should stop even if IsTruncted is true. (Most likely this happens for an empty bucket)
  # Case1: Contents is empty.
  # Case2: nextMarker is same as previous marker.
  while (result[["IsTruncated"]] == "true" & !shouldStop) {
    # Get the last row from the result and check the Key and pass that as a new marker to make the pagination works.
    if (is.null(tail(result, 1)[["Contents"]])) {# if result does not have Contents, stop here.
      shouldStop <- TRUE
    } else if (tail(result, 1)[["Contents"]][["Key"]] == nextMarker) { # if nextMarker is same as the current one, stop here.
      shouldStop <- TRUE
    } else {
      nextMarker <-  tail(result, 1)[["Contents"]][["Key"]]
      query <- list(prefix = prefix, delimiter = "/", "max-keys" = 1000, marker = nextMarker)
      # Send another query to get remaining.
      additionalResult <- aws.s3::s3HTTP(verb = "GET", bucket = bucket, query = query, parse_response = TRUE, ...)
      # Append additional query result
      combinedResult <- c(result, additionalResult)
      combinedResult[["IsTruncated"]] <- additionalResult[["IsTruncated"]]
      result <- combinedResult
    }
  }
  # Folders are stored under CommonPrefixes
  df <- data.frame(result[names(result) == "CommonPrefixes"])

  # The data frame looks like this so gather columns and keep only "folder" column.
  #   CommonPrefixes CommonPrefixes.1 CommonPrefixes.2
  # 1          data/           data2/           data3/
  if (nrow(df) > 0) {
    df %>% tidyr::gather(key="key", value="folder") %>% select("folder")
  } else { # if no folder is found, return empty data frame.
    data.frame()
  }
}
