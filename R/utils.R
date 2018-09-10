

add_quotes <- function(x, quote = '"'){
  ifelse(is.character(x),paste0(quote,x,quote),x)
}

write_props_cypher <- function(node, quote = TRUE){
  # node <- list(Name = "Whatever", age = 45, vals = c("v1","v2"), xx = 1:4)
  l <- list_to_name_value(node)
  props <- map(l, function(v){
    if(length(v$value) > 1 ){
      quote <- !is.numeric(v$value)
      vv <- ifelse(is.numeric(v$value),
                   paste0("[",paste0(v$value, collapse = ","),"]"),
                   paste0("[",paste0(paste0('"',v$value,'"'), collapse = ","),"]")
      )
    }else{
      if(quote){
        vv <- ifelse(is.numeric(v$value),v$value, paste0('"',v$value,'"'))
      }else{
        vv <- v$value
      }
    }
    paste0(v$name,': ',vv)
  })
  paste0(props, collapse = ", ")
}


str_tpl_format_map <- function(l, tpl){
  map(l, function(x){
    str_tpl_format(tpl,x)
    })
}

str_tpl_format <- function(tpl, l){
  if("list" %in% class(l)){
    l <- l[lapply(l,length)==1]
    listToNameValue <- function(l){
      mapply(function(i,j) list(name = j, value = i), l, names(l), SIMPLIFY = FALSE)
    }
    f <- function(tpl,l){
      if(is.null(l$value)) return(tpl)
      gsub(paste0("{",l$name,"}"), l$value, tpl, fixed = TRUE)
    }
    lvals <- listToNameValue(l)
    return(Reduce(f,lvals, init = tpl))
  }
  if("data.frame" %in% class(l)){
    myTranspose <- function(x) lapply(1:nrow(x), function(i) lapply(l, "[[", i))
    return( unlist(lapply(myTranspose(l), function(l, tpl) str_tpl_format(tpl, l), tpl = tpl)) )
  }
}


list_to_name_value <- function(l){
  transpose(list(name = names(l), value = unname(l)))
}

list_to_df <- function(x){
  bind_rows_safely <- safely(bind_rows)
  y <- bind_rows_safely(x)
  if(is.null(y$result)){
    cls <- map(x, function(.x){
      map(.x, class)
    })
    vars <- bind_rows(cls) %>%
      map(~length(unique(na.omit(.)))) %>%
      keep(~. > 1) %>% names()
    x2 <- map(x,function(.x){
      if(any(vars %in% names(.x))){
        vars_in <- vars[vars %in% names(.x)]
        .x[vars_in] <- map(.x[vars_in], as.character)
      }
      .x
    })
    y <- bind_rows_safely(x2)
  }
  y$result
}


match_replace <- function (v, dic, force = TRUE){
  matches <- dic[[2]][match(v, dic[[1]])]
  out <- matches
  if (!force)
    out[is.na(matches)] <- v[is.na(matches)]
  out
}

