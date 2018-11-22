context("DB Connection")

test_that("connection",{

  # MAKE SURE NEO4J IS UP: neo4j start
  #dotenv::load_dot_env(".env")
  #neo4j <- as.list(Sys.getenv(c("NEO4J_URL","NEO4J_USR","NEO4J_PWD")))
  # con <- neo4j_api$new(url = neo4j$NEO4J_URL,
  #                      user = neo4j$NEO4J_USR, password = neo4j$NEO4J_PWD)

  con <- neo4j_api$new(url = "http://localhost:7474",
                       user = "neo4j", password = "neo4jpwd")
  clear_db(con)
  expect_true(con$ping() == 200)
  drop_all_constraints(con)

  create_constraint("Movie","id", con = con)
  create_constraint("Movie","uid", con = con)
  expect_equal(con$get_constraints() %>% pull(property_keys),c("id","uid"))
  drop_constraint("Movie","id", con = con)
  expect_equal(con$get_constraints() %>% pull(property_keys),"uid")

})
