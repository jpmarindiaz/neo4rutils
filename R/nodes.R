#' @export
create_nodes <- function(nodes, label = NULL, con = con, show_query = FALSE){
  #nodes <- list(list(Name = "jp", age = 32), list(Name = "Ey",age = 23))
  #nodes <- list(list(title = "New movie", country = "COL", vals = c("valsss","other")))
  #label <- "Movie"
  if(!is.null(label)){
    qtpl <- 'CREATE (a{idx}:{label}{{prop}})'
    qnode <- str_tpl_format(qtpl, list(label = label))
  }else{
    qtpl <- 'CREATE (a{idx}{{prop}})'
    qnode <- qtpl
  }
  props <- map(nodes, write_props_cypher)
  l <- map(seq_along(props), ~list(prop = props[[.]], label = label, idx=.))
  q <- str_tpl_format_map(l, qtpl)
  ret_nodes <- paste("a",1:length(props),sep = "")
  # ret <- map(ret_nodes, function(x){
  #   paste0("ID(",x,"), ",x)
  # }) %>% paste0(.,collapse = ", ")
  ret <- ret_nodes %>% paste0(.,collapse = ", ")
  q <- paste0(paste0(q, collapse = "\n"), paste0("\nRETURN ",ret))
  if(show_query) message(q)
  d <- call_cypher(q, con)

  # Clean up response
  drow <- d$data$row
  dmeta <- d$data$meta
  if(length(dmeta[[1]]) > 1){
    drow <- drow[[1]]
    dmeta <- dmeta[[1]]
  }else{
    drow <- map(drow,1)
    dmeta <- map(dmeta,1)
  }
  drow <- jsonlite::fromJSON(jsonlite::toJSON(drow))
  dmeta <-  dmeta %>%
    bind_rows() %>%
    select(.id = id, .type = type, .deleted = deleted)
  bind_cols(drow,dmeta)
}

#' @export
load_nodes_csv <- function(csv_url, label, con, show_query = FALSE){
  #csv_url <- "https://raw.githubusercontent.com/jpmarindiaz/neo4rutils/master/inst/data/movies.csv"
  d <- read_csv(csv_url, n_max = 5)
  props <- paste0("csvLine.",names(d))
  names(props) <- names(d)
  props <- write_props_cypher(as.list(props), quote = FALSE)
  qtpl <- 'MERGE (a:{label}{{props}})'
  on_load_query <- str_tpl_format(qtpl, list(label = label, props = props))
  if(show_query) message(on_load_query)
  load_csv(url = csv_url,
           con = con, header = TRUE, periodic_commit = 1000,
           as = "csvLine", on_load = on_load_query)

  #   on_load_query <- 'MERGE (a:artist { name: csvLine.artist})
  # MERGE (al:album {name: csvLine.album_name})
  #   MERGE (a) -[:has_recorded] -> (al)
  #   RETURN a AS artists, al AS albums;'
  #   # Send the csv
  #   load_csv(url = csv_url,
  #            con = con, header = TRUE, periodic_commit = 50,
  #            as = "csvLine", on_load = on_load_query)

}


#' @export
get_node_count <- function(label = NULL, con = con){
  if(is.null(label)){
    q <- "MATCH (n)\nRETURN COUNT(n)"
  }else if(is.na(label)){
    q <- "MATCH (n) \nWHERE size(labels(n)) = 0\nRETURN COUNT(n)"
  }
  else {
    q <- "MATCH (n:{label})\nRETURN COUNT(n)"
    q <- str_tpl_format(q,list(label = label))
  }
  unname(unlist(call_api(q, con)))
}

#' @export
get_node_count_by_label <- function(con){
  labels <- get_available_labels(con)
  labels <- c(labels,NA)
  x <- map_int(labels, ~get_node_count(label = ., con))
  data_frame(label = labels, node_count = x)
}

#' @export
get_label_node_count <- function(label = NULL, con = NULL){
  labelin <- label
  x <- get_node_count_by_label(con)
  if(!label %in% x$label) return(0)
  x %>%
    filter(label == labelin) %>%
    pull(node_count)
}



#' @export
get_node_by_id <- function(.id, con = con, asList = TRUE){
  #.id <- sample(allNodes$.id,1)
  q <- 'MATCH (n)
  WHERE ID(n) = {.id}
  RETURN n'
  q <- str_tpl_format(q,list(.id = .id))
  n <- call_api(q, con)[[1]]
  n$.id <- .id
  if(asList) as.list(n)
  n
}

#' @export
get_node_by_uid <- function(uid, prop = "uid", label = NULL, con = NULL, asList = TRUE){
  if(is.character(uid)){
    uid <- paste0("'",uid,"'")
  }
  if(is.null(label)){
    q <- "MATCH (n) WHERE n.{prop} = {uid} RETURN ID(n),n"
    q <- str_tpl_format(q,list(uid = uid, prop = prop))
  }else{
    labelIn <- label
    currentLabelConstraints <- get_constraints(con) %>%
      filter(label == labelIn) %>% pull(property_keys)
    if(!prop %in% currentLabelConstraints){
      stop(prop, " needs to be a unique constraint for label: ", label)
    }
    q <- "MATCH (n:{label}) WHERE n.{prop} = {uid} RETURN ID(n),n"
    q <- str_tpl_format(q,list(uid = uid, prop = prop, label = label))
  }
  n <- call_api(q, con)
  if(length(n)==0){
    warning("uid not in db ", uid)
    return(NULL)
  }
  nn <- data_frame(.id = n[[1]]$value)
  nn <- bind_cols(nn, n[[2]])
  if(asList) as.list(nn)
  nn
}

#' @export
get_node_keys <- function(label = NULL, con = NULL, asTable = TRUE){
  if(is.null(label)){
    q <- "MATCH (p) RETURN id(p),keys(p);"
  }else{
    q <- "MATCH (p:{label}) RETURN id(p),keys(p);"
    q <- str_tpl_format(q,list(label = label))
  }
  ans <- call_api(q,con, type = "row", output = "json")
  x <- jsonlite::fromJSON(ans)[[1]][[1]]
  keys <- map(x,~as.vector(.[[2]])) %>% set_names(map(x,~.[[1]]))
  if(asTable){
    keys <- map(keys, ~data_frame(key = .)) %>% bind_rows(.id = ".id")
    keys$.id <- as.numeric(keys$.id)
  }
  keys
}

#' @export
get_available_labels <- function(con){
  con$get_labels() %>% pull(labels)
}

#' @export
get_nodes_table <- function(label = NULL, con = NULL){

  if(is.null(label)){
    q <- "MATCH (n)\nRETURN ID(n) as `.id`,labels(n) as `.label`,n"
  }else{
    if(is.na(label)){
      q <- "MATCH (n) \nWHERE size(labels(n)) = 0\nRETURN n"
      q <- str_tpl_format(q, list(label = label))
      d <- call_cypher(q, con)
      if(is.null(d$data)) return()

      drow <- d$data$row
      dmeta <- d$data$meta

      drow <- map(drow,1)
      drow <- jsonlite::fromJSON(jsonlite::toJSON(drow))
      dmeta <- map(dmeta,1) %>% bind_rows() %>%
        select(.id = id, .type = type, .deleted = deleted)

      return(flatten_df_list(bind_cols(drow,dmeta)))
    }
    if(!label %in% get_available_labels(con)) stop("label not in Labels")
    q <- "MATCH (n:{label})\nRETURN ID(n) as `.id`,labels(n) as `.label`,n"
    q <- str_tpl_format(q, list(label = label))
  }
  d <- call_cypher(q, con)
  if(is.null(d$data)) return()

  drow <- d$data$row
  dmeta <- d$data$meta

  drow <- map(drow, ~set_names(.,d$columns))
  dmeta <- d$meta

  drowt <- transpose(drow)
  hasManyElements <- function(x) any(map(x, length) > 1)
  hasMany <- map_lgl(drowt, hasManyElements)
  drowtDf <- drowt[hasMany][[1]]
  drowtDf <- jsonlite::fromJSON(jsonlite::toJSON(drowtDf))
  drowtMeta <- drowt[!hasMany]
  drowtMeta <- map(drowtMeta, function(x){
    x[map_lgl(x,is_empty)] <- NA
    unlist(x)
  }) %>% as_data_frame()
  if(nrow(drowtMeta) > 0){
    dres <- cbind(drowtDf, drowtMeta)
  }else{
    dres <- drowtDf
  }
  flatten_df_list(dres)
}

#' @export
load_nodes_df <- function(d, label = NULL, con = NULL, show_query = FALSE){
  nodes <- transpose(d)
  create_nodes(nodes, label = label, con = con, show_query = show_query)
}

#' @export
delete_node <- function(.id, con = NULL, withRels = FALSE){
  if(withRels){
    q = "
    MATCH (n)-[r]-()
    WHERE id(n)={.id}
    DELETE n, r"
  }else{
    q <- "
    MATCH (n)
    WHERE id(n)={.id}
    DELETE n"
  }
  q <- str_tpl_format(q,list(.id = .id))
  call_api(q, con)
  TRUE
}

#' @export
delete_node_by_uid <- function(uid, prop = "uid", label = NULL, con = NULL, withRels = FALSE){
  if(is.character(uid)){
    uid <- paste0("'",uid,"'")
  }
  if(withRels){
    q = "
    MATCH (n:{label})-[r]-()
    WHERE n.{prop}={uid}
    DELETE n, r"
  }else{
    q <- "
    MATCH (n:{label})
    WHERE n.{prop}={uid}
    DELETE n"
  }
  q <- str_tpl_format(q,list(uid=uid,label = label, prop = prop))
  if(is.null(label))
    q <- gsub(":{label}","",q, fixed = TRUE)
  d <- call_cypher(q,con)
  TRUE
}

#' @export
delete_labeled_nodes <- function(label = NULL, con = NULL, withRels = FALSE){
  if(withRels){
    q <- "
    MATCH (n:{label})-[r]-()
    DELETE n, r"
  }else{
    q <- "MATCH (n:{label})
    DELETE n"
  }
  q <- str_tpl_format(q,list(label = label))
  call_api(q,con)
  get_label_node_count(label = label, con = con) == 0
}




