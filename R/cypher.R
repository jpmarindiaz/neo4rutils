
#' @export
call_cypher <- function (query, con)
{
  include_stats <- TRUE
  query_clean <- clean_query(query)
  query_jsonised <- to_json_neo(query_clean)
  body <- glue("{\"statements\" : [ %query_jsonised% ]}",
               .open = "%", .close = "%")
  res <- POST(url = glue("{con$url}/db/data/transaction/commit?includeStats=true"),
              add_headers(.headers = c(`Content-Type` = "application/json",
                                       accept = "application/json", Authorization = paste0("Basic ",
                                                                                           con$auth))), body = body)
  if(status_code(res) != 200) stop("API error")

  parse_api_results(res = res)
}


to_json_neo <- function (query) {
  jsonlite::toJSON(list(statement = query, includeStats = TRUE,
                        meta = TRUE, resultDataContents = list("row")), auto_unbox = TRUE)
}

clean_query <- function (query) {
  res <- gsub("^\\/\\/.+$", "\n", query, perl = TRUE)
  res <- gsub("\n", " ", res)
  res <- gsub("\"", "'", res)
  res
}


parse_api_results <- function (res){
  api_content <- content(res)
  if (length(api_content$errors) > 0) {
    return(list(error_code = api_content$errors[[1]]$code,
                error_message = api_content$errors[[1]]$message))
  }
  results <- api_content$results[[1]]
  if (length(results$data) == 0) {
    message("No data returned.")
  }
  res_names <- results$columns
  res_data <- results$data

  d <- transpose(res_data)
  drow <- d$row[[1]]
  drow <- jsonlite::fromJSON(jsonlite::toJSON(drow))
  dmeta <- d$meta[[1]] %>%
    bind_rows() %>%
    select(.id = id, .type = type, .deleted = deleted)
  bind_cols(drow,dmeta)
}


