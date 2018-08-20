#' @export
create_edge <- function(src_id, tgt_id, rel_type, props, show_query = FALSE){
  src_id <- 204
  tgt_id <- 205
  rel_type <- "RELATION"
  props <- list(x = 23, txt = "weeeee")
  #ed <- list(from = 1, to = "la-estrategia", myreltype = "RELATED")
  props <- write_props_cypher(props)
  qtpl <- "MATCH (n1), (n2)
  WHERE ID(n1) = {src_id} AND ID(n2) = {tgt_id}
  CREATE (n1)-[r:{rel_type}{{props}}]->(n2)
  RETURN n1,r,n2"
  vals <- list(src_id = src_id, tgt_id = tgt_id, rel_type = rel_type, props = props)
  q <- str_tpl_format(qtpl,vals)
  if(show_query) message(q)
  call_api(q, con)
}


#' @export
load_edges_csv <- function(csv_url = NULL,
                           rel_type = NULL,
                           src_col = NULL,
                           src_label = NULL,
                           src_uid_prop = NULL,
                           tgt_col = NULL,
                           tgt_label = NULL,
                           tgt_uid_prop = NULL,
                           rel_props = NULL,
                           con = NULL,
                           show_query = TRUE,
                           periodic_commit = 20
                           ){
  d <- read_csv(csv_url, n_max = 5, col_types = cols(.default = "c"))
  on_load_query <- prep_edges_load_query(d = d,
                                         rel_type = rel_type,
                                         src_col = src_col,
                                         src_label = src_label,
                                         src_uid_prop = src_uid_prop,
                                         tgt_col = tgt_col,
                                         tgt_label = tgt_label,
                                         tgt_uid_prop = tgt_uid_prop,
                                         rel_props = rel_props,
                                         con = con,
                                         show_query = show_query)
  if(show_query) message(on_load_query)
  load_csv(url = csv_url,
           con = con, header = TRUE, periodic_commit = periodic_commit,
           as = "csvLine", on_load = on_load_query)
}

prep_edges_load_query <- function(d = NULL,
                                  rel_type = NULL,
                                  src_col = NULL,
                                  src_label = NULL,
                                  src_uid_prop = NULL,
                                  tgt_col = NULL,
                                  tgt_label = NULL,
                                  tgt_uid_prop = NULL,
                                  rel_props = NULL,
                                  con = NULL,
                                  show_query = TRUE){
  calc_rel_props <- names(d)[!names(d) %in% c(src_label, tgt_label,rel_type,src_col,tgt_col)]
  if(is.null(rel_props)){
    rel_props <- calc_rel_props
  }
  if(!rel_props %in% names(d)) stop("Rel prop columns not found in csv")
  rel_type <- ifelse(is.null(rel_type),"",paste0(":",rel_type))
  src_label <- ifelse(is.null(src_label),"",paste0(":",src_label))
  tgt_label <- ifelse(is.null(tgt_label),"",paste0(":",tgt_label))
  props <- paste0("csvLine.",rel_props)
  names(props) <- rel_props
  props <- write_props_cypher(as.list(props), quote = FALSE)
  qtpl <- '
  MATCH (n1{src_label}{{src_uid_prop}:csvLine.{src_col}})
  MATCH (n2{tgt_label}{{tgt_uid_prop}:csvLine.{tgt_col}})
  CREATE UNIQUE (n1)-[r{rel_type}{{props}}]-(n2)
  RETURN n1,r,n2'
  vals <- list(src_label = src_label,
               src_col = src_col, src_uid_prop = src_uid_prop,
               tgt_label = tgt_label,
               tgt_col = tgt_col, tgt_uid_prop = tgt_uid_prop,
               rel_type = rel_type,
               props = props)
  # USING PERIODIC COMMIT 50 LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/jpmarindiaz/neo4rutils/master/inst/data/roles.csv' AS csvLine
  # MERGE (n1:Person{id:csvLine.personId})
  # MERGE (n2:Movie{id:csvLine.movieId})
  # MERGE (n1)-[r:TEST2333]->(n2)
  on_load_query <- str_tpl_format(qtpl, vals)
  on_load_query
}



#' @export
get_edges_table <- function(rel_type = NULL, con = NULL){
  if(is.null(rel_type)){
    rel_types <- get_available_rel_types(con)
    edges <- map(rel_types, ~get_edges_rel_type_table(.,con)) %>%
      bind_rows()
  }else{
    edges <- get_edges_rel_type_table(rel_type, con)
  }
  edges
}

#' @export
get_edges_rel_type_table <- function(rel_type, con, src_cols = NULL, tgt_cols = NULL){
  if(!rel_type %in% get_available_rel_types(con))
    stop("Rel type not in db")
  q <- glue("MATCH (n1)-[r:{rel_type}]->(n2) RETURN n1,r,n2")
  res <- call_api(q, con, meta = TRUE)
  edges <- res$r %>% select(.rel_id = id, everything(), -type, -deleted)
  #get_constraints(con)
  if(is.null(src_cols)) src_cols <- character(0)
  if(is.null(tgt_cols)) tgt_cols <- character(0)
  n1 <- res$n1 %>% select(.src_id = id, one_of(src_cols))
  names(n1)[names(n1) %in% src_cols] <- paste0("src_",src_cols)
  n2 <- res$n2 %>% select(.tgt_id = id, one_of(tgt_cols))
  names(n2)[names(n2) %in% tgt_cols] <- paste0("tgt_",src_cols)
  edges <- bind_cols(edges,n1,n2) %>%
    mutate(rel_type = rel_type) %>%
    select(rel_type, .rel_id, .src_id, .tgt_id, everything())
}




#' @export
get_total_edge_count <- function(graph){
  q <- "MATCH (n)-[r]-(m)
    RETURN COUNT(r)"
  flatten_int(cypher(graph, q))
}



#' @export
load_edges_data_frame <- function(edges,
                                  sourceCol = NULL, targetCol = NULL,
                                  relType = NULL, relTypeCol = NULL,
                                  sourceProp = NULL, targetProp = NULL,
                                  sourceLabel = NULL, targetLabel = NULL,
                                  graph = NULL,createNodes = FALSE){
  ed <- transpose(edges)
  f <- function(e){
    #e <- ed[[1]]
    props <- e
    props[sourceCol] <- NULL
    props[targetCol] <- NULL
    src <- get_node_by_uid(e[[sourceCol]],prop = sourceProp, label = sourceLabel,
                           graph = graph)
    tgt <- get_node_by_uid(e[[targetCol]],prop = targetProp, label = targetLabel,
                           graph = graph)
    if(is.null(src) || is.null(tgt))
      return(paste0("ERROR in src:",e[[sourceCol]], ", tgt: ",e[[targetCol]]))
    if(!is.null(relTypeCol)){
      if(!relTypeCol %in% names(e)) stop("RelTypeCol not in node")
      relType <- e[[relTypeCol]]
    }
    props <- props[!is.na(props)]
    if(is.na(relType))
      return(paste0("ERROR: REL_TYPE is NA in src:",e[[sourceCol]], ", tgt: ",e[[targetCol]]))

    if(length(props) == 0)
      props <- NULL

    createRel(src, relType, tgt, props)
    "OK"
  }
  map(ed,f)
}

#' @export
get_edge_count <- function(rel_type= NULL, con = con){
  if(is.null(rel_type)){
    q <- "MATCH (n)-[r]->()\nRETURN COUNT(r)"
  }else if(is.na(rel_type)){
    q <- "MATCH (n) \nWHERE size(labels(n)) = 0\nRETURN COUNT(n)"
  }else {
    q <- "MATCH (n)-[r:`{rel_type}`]->()\nRETURN COUNT(r)"
    q <- str_tpl_format(q,list(rel_type = rel_type))
  }
  unname(unlist(call_api(q, con)))
}

#' @export
get_available_rel_types <- function(con){
  q <- "MATCH (n)-[r]-(m) RETURN DISTINCT type(r) AS type"
  res <- call_api(q, con)
  res$type %>% pull(1)
}

#' @export
get_edge_count_by_rel_type <- function(con = con){
  rel_types <- get_available_rel_types(con)
  rel_types <- c(rel_types)
  x <- map_int(rel_types, ~get_edge_count(rel_type = ., con))
  data_frame(rel_types = rel_types, edge_count = x)
}


#' @export
delete_edges <- function(con){
  q <- "MATCH ()-[r]-()\nDELETE r"
  res <- call_api(q, con)
  get_edge_count(rel_type = NULL, con)
}

