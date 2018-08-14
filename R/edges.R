
create_edges <- function(edges, rel_type = NULL,
                         src_col = NULL, src_label = NULL, src_uid_prop = NULL,
                         tgt_col = NULL, tgt_label = NULL, tgt_uid_prop = NULL,
                         rel_type_col = NULL,
                         con = con, show_query = FALSE){
  # edges <- list(list(from = 1, to = "la-estrategia", myreltype = "RELATED"),list(from = 2, to = "la-estrategia", anotherrel = "another1",x=2, myreltype = "RELTYPE2"))
  edges2 <- map(seq_along(edges),function(i){
    ed <- edges[[i]]
    src_uid <- ed[[src_col]]
    src_uid <- add_quotes(src_uid)
    tgt_uid <- ed[[tgt_col]]
    tgt_uid <- add_quotes(tgt_uid)
    idx <- i
    if(is.null(rel_type)){
      props <- ed %>% keep(!names(ed) %in% c(src_col, tgt_col,rel_type_col))
      rel_type_value <- ed[[rel_type_col]]
    }else{
      props <- ed %>% keep(!names(ed) %in% c(src_col, tgt_col))
      rel_type_value <- rel_type
    }
    list(src_uid_prop = src_uid_prop, src_uid = src_uid, src_label = src_label,
         tgt_uid_prop = tgt_uid_prop, tgt_uid = tgt_uid, tgt_label = tgt_label,
         idx = idx, REL_TYPE = rel_type_value, props = props)
  })
  if(src_uid_prop == ".id" || tgt_uid_prop == ".id"){
    tpl <- "ID(n{idx}) = {src_uid} AND ID(m{idx}) = {tgt_uid}"
    qmatch <- str_tpl_format_map(edges2,"ID(n{idx}) = {src_uid} AND ID(m{idx}) = {tgt_uid}")
    qmatchn <- paste0(str_tpl_format_map(edges2,"(n{idx}),(m{idx})"), collapse = ",")
    qmatch <- paste0("MATCH ", qmatchn, " WHERE ", paste0(qmatch, collapse = " AND "))
  }else{
    if(is.null(src_label) || is.null(tgt_label)){
      qmatch <- "(n{idx}{{src_uid_prop}:{src_uid}}),(m{idx}{{tgt_uid_prop}:{tgt_uid}})"
    }else{
      qmatch <- "(n{idx}:{src_label}{{src_uid_prop}:{src_uid}}),(m{idx}:{tgt_label}{{tgt_uid_prop}:{tgt_uid}})"
    }
    qmatch <- str_tpl_format_map(edges2, qmatch)
    qmatch <- paste0("MATCH ",paste0(qmatch, collapse = ", "))
  }
  qmerge <- map(seq_along(edges2), function(i){
    ed <- edges2[[i]]
    if(length(ed$props) > 0){
      q <- "MERGE (n{idx})-[e{idx}:{REL_TYPE}{{props}}]-(m{idx})"
    }else{
      q <- "MERGE (n{idx})-[e{idx}:{REL_TYPE}]-(m{idx})"
    }
    props <- write_props_cypher(ed$props)
    str_tpl_format(q, list(REL_TYPE = add_quotes(ed$REL_TYPE,""), props = props, idx = i))
  })
  qmerge <- paste0(qmerge, collapse = "\n")
  q <- paste(qmatch, qmerge, sep = "\n")
  ret <- paste0(paste("e",1:length(edges),sep = ""),collapse=",")
  q <- paste0(paste0(q, collapse = "\n"), paste("\nRETURN ",ret))
  if(show_query) message(q)
  call_api(q, con)
}


#' @export
get_edges_table <- function(con, rel_type = NULL){
  q <- "MATCH ()-[r]->() RETURN r"
  edges <- call_api(q, con)

  r <- cypherToList(graph, q)
  start <- map_chr(r, ~ basename(attr(.$r,"start")))
  end <-  map_chr(r, ~ basename(attr(.$r,"end")))
  type <-  map_chr(r, ~ basename(attr(.$r,"type")))
  relId <-  map_chr(r, ~ basename(attr(.$r,"self")))
  edgesTable <- data_frame(.relId = relId, type = type, from = start, to = end)
  labels <- getLabel(graph)
  nodesTables <- get_nodes_table(graph)
  nodesTablesIds <- nodesTables[c(".id","label")]
  edgesTable$fromLabel <- match_replace(edgesTable$from,nodesTablesIds)
  edgesTable$toLabel <- match_replace(edgesTable$to,nodesTablesIds)
  edgesTable
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
load_edges_csv <- function(file, sourceCol = NULL, targetCol = NULL,
                           relType = NULL,
                           sourceProp = NULL, targetProp = NULL,
                           sourceLabel = NULL, targetLabel = NULL,
                           graph = NULL,createNodes = FALSE){
  edges <- read_csv(file)
  load_edges_data_frame(edges,sourceCol = sourceCol, targetCol = targetCol,
                        relType = relType,
                        sourceProp = sourceProp, targetProp = targetProp,
                        sourceLabel = sourceLabel, targetLabel = targetLabel,
                        graph = graph,createNodes = FALSE)
}


#' @export
get_edge_count <- function(relType= NULL, con = con){
  if(!is.null(relType)){
    q <- "MATCH (n)-[r:`{relType}`]-(m)
    RETURN COUNT(r)"
    q <- str_tpl_format(q,list(relType = relType))
  }else{
    q <- "MATCH (n)-[r]-(m)
    RETURN COUNT(r)"
  }
  unname(unlist(call_api(q, con)))
}

