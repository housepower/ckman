[1mdiff --git a/controller/clickhouse.go b/controller/clickhouse.go[m
[1mindex 396aaee..f011d25 100644[m
[1m--- a/controller/clickhouse.go[m
[1m+++ b/controller/clickhouse.go[m
[36m@@ -653,12 +653,6 @@[m [mfunc (ck *ClickHouseController) MaterializedView(c *gin.Context) {[m
 		return[m
 	}[m
 [m
[31m-	if req.Polulate && req.ToTable != "" {[m
[31m-		err := fmt.Errorf("can't create a new materialized view with pululate while to table")[m
[31m-		model.WrapMsg(c, model.INVALID_PARAMS, err)[m
[31m-		return[m
[31m-	}[m
[31m-[m
 	if req.Operate != model.OperateCreate && req.Operate != model.OperateDelete {[m
 		err := fmt.Errorf("operate only supports create and delete")[m
 		model.WrapMsg(c, model.INVALID_PARAMS, err)[m
[1mdiff --git a/model/ck_table.go b/model/ck_table.go[m
[1mindex 0ba9957..d9d3f4c 100644[m
[1m--- a/model/ck_table.go[m
[1m+++ b/model/ck_table.go[m
[36m@@ -227,7 +227,6 @@[m [mtype MaterializedViewReq struct {[m
 	Database  string `json:"database"`[m
 	Order     []string[m
 	Partition CkTablePartition[m
[31m-	ToTable   string `json:"table"`[m
 	Statement string `json:"statement"`[m
 	Dryrun    bool   `json:"dryrun"`[m
 	Operate   int    `json:"operate"`[m
[1mdiff --git a/service/clickhouse/clickhouse_service.go b/service/clickhouse/clickhouse_service.go[m
[1mindex 28da552..6848f0b 100644[m
[1m--- a/service/clickhouse/clickhouse_service.go[m
[1m+++ b/service/clickhouse/clickhouse_service.go[m
[36m@@ -953,17 +953,13 @@[m [mfunc MaterializedView(conf *model.CKManClickHouseConfig, req model.MaterializedV[m
 			partition = fmt.Sprintf("toYYYYMMDD(`%s`)", req.Partition.Name)[m
 		}[m
 [m
[31m-		var toTable string[m
[31m-		if req.ToTable != "" {[m
[31m-			toTable = fmt.Sprintf("TO `%s`.`%s`", req.Database, req.ToTable)[m
[31m-		}[m
 		var populate string[m
 		if req.Populate {[m
 			populate = "POPULATE"[m
 		}[m
 [m
[31m-		query = fmt.Sprintf("CREATE MATERIALIZED VIEW IF NOT EXISTS `%s`.`%s` ON CLUSTER `%s` %s ENGINE=%s PARTITION BY %s ORDER BY (`%s`) %s AS %s",[m
[31m-			req.Database, req.Name, conf.Cluster, toTable, req.Engine, partition, strings.Join(req.Order, "`,`"), populate, req.Statement)[m
[32m+[m		[32mquery = fmt.Sprintf("CREATE MATERIALIZED VIEW IF NOT EXISTS `%s`.`%s` ON CLUSTER `%s` ENGINE=%s PARTITION BY %s ORDER BY (`%s`) %s AS %s",[m
[32m+[m			[32mreq.Database, req.Name, conf.Cluster, req.Engine, partition, strings.Join(req.Order, "`,`"), populate, req.Statement)[m
 	} else if req.Operate == model.OperateDelete {[m
 		query = fmt.Sprintf("DROP VIEW IF EXISTS `%s`.`%s` ON CLUSTER `%s`) SYNC",[m
 			req.Database, req.Name, conf.Cluster)[m
