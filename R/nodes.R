#' @export
create_nodes <- function(nodes, label = NULL, con = con, show_query = FALSE){
  #nodes <- list(list(Name = "jp", age = 32), list(Name = "Ey",age = 23))
  #nodes <- list(id = "New movie", country = "COL", vals = c("val1", "val2"))
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
  ret <- paste0(paste("a",1:length(props),sep = ""),collapse=",")
  q <- paste0(paste0(q, collapse = "\n"), paste("\nRETURN ",ret))
  if(show_query) message(q)
  call_api(q, con)
}

#' @export
load_nodes_csv <- function(csv_url, label, con, show_query = FALSE){
  #csv_url <- "https://raw.githubusercontent.com/jpmarindiaz/RNeo4jUtils/master/inst/data/movies.csv"
  d <- read_csv(csv_url, n_max = 5)
  props <- paste0("csvLine.",names(d))
  names(props) <- names(d)
  props <- write_props_cypher(as.list(props), quote = FALSE)
  qtpl <- 'MERGE (a:{label}{{props}})'
  on_load_query <- str_tpl_format(qtpl, list(label = label, props = props))
  if(show_query) message(on_load_query)
  load_csv(url = csv_url,
           con = con, header = TRUE, periodic_commit = 50,
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
  if(!is.null(label)){
    q <- "MATCH (n:{label})\nRETURN COUNT(n)"
    q <- str_tpl_format(q,list(label = label))
  }else{
    q <- "MATCH (n)\nRETURN COUNT(n)"
  }
  unname(unlist(call_api(q, con)))
}

#' @export
get_node_count_by_label <- function(con){
  labels <- con$get_labels() %>% pull(labels)
  x <- map_int(labels, ~get_node_count(label = ., con))
  data_frame(label = labels, node_count = x)
}

#' @export
get_label_node_count <- function(label = NULL, con = NULL){
  labelin <- label
  get_node_count_by_label(con) %>%
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
get_node_by_uid <- function(uid, prop, label = NULL, con = NULL, asList = TRUE){
  if(is.character(uid)){
    uid <- paste0("'",uid,"'")
  }
  if(!prop %in% con$get_constraints()$property_keys)
    stop(prop, " needs to be a unique constraint")
  if(is.null(label)){
    q <- "MATCH (n) WHERE n.{prop} = {uid} RETURN ID(n),n"
    q <- str_tpl_format(q,list(uid = uid, prop = prop))
  }else{
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
get_nodes_table <- function(label = NULL, con = NULL){
  labels <- con$get_labels() %>% pull(labels)
  if(!is.null(label) && !label %in% labels) stop("label not in Labels")
  if(!is.null(label) && get_label_node_count(label, con) == 0) return(data_frame(.id = numeric(0)))
  q <- ifelse(is.null(label),"MATCH (n)\nRETURN ID(n),n","MATCH (n:{label})\nRETURN ID(n),n")
  if(!is.null(label)) q <- str_tpl_format(q, list(label = label))
  nodes <- call_api(q, con)
  #nodes <- call_api(q, con, type = "graph")$nodes
  ids <- nodes[[1]] %>% select(.id = value)
  nodesdf <- nodes[[2]]
  if(!"data.frame" %in% class(nodesdf)){
    common_vars <- names(transpose(nodesdf))
    nodesdf <- map(nodesdf, function(x){
      x[common_vars] <- map(common_vars, ~as.character(x[[.]]))
      x
    }) %>% bind_rows()
  }
  bind_cols(ids,nodesdf)
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
delete_node_by_uid <- function(uid, prop, label, graph = NULL, withRels = FALSE){
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
  cypher(graph,q)
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
  get_label_node_count(label = "Movie", con = con) == 0
  }




