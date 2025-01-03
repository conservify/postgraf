package main

import (
	"context"
	"flag"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
)

type Query struct {
	Table string
	SQL   string
	Tags  []string
}

var (
	Queries = []*Query{
		&Query{
			Table: "pg_db_stats",
			Tags:  []string{},
			SQL: `
select
  numbackends,
  xact_commit,
  xact_rollback,
  blks_read,
  blks_hit,
  tup_returned,
  tup_fetched,
  tup_inserted,
  tup_updated,
  tup_deleted,
  conflicts,
  temp_files,
  temp_bytes,
  deadlocks,
  blk_read_time,
  blk_write_time,
  extract(epoch from (now() - pg_postmaster_start_time()))::int8
    as postmaster_uptime_s,
  0
    as backup_duration_s,
  case when pg_is_in_recovery() then 1 else 0 end
    as in_recovery_int
from
  pg_stat_database, pg_control_system()
where
  datname = current_database();
`,
		},
		&Query{
			Table: "pg_index_stats",
			Tags:  []string{"table_name", "schema", "index_name"},
			SQL: `
WITH q_locked_rels AS (
  select relation from pg_locks where mode = 'AccessExclusiveLock' and granted
)
SELECT
  schemaname::text as schema,
  indexrelname::text as index_name,
  relname::text as table_name,
  coalesce(idx_scan, 0) as idx_scan,
  coalesce(idx_tup_read, 0) as idx_tup_read,
  coalesce(idx_tup_fetch, 0) as idx_tup_fetch,
  coalesce(pg_relation_size(indexrelid), 0) as index_size_b,
  quote_ident(schemaname)||'.'||quote_ident(sui.indexrelname) as index_full_name_val,
  case when not i.indisvalid then 1 else 0 end as is_invalid_int,
  case when i.indisprimary then 1 else 0 end as is_pk_int,
  case when i.indisunique or indisexclusion then 1 else 0 end as is_uq_or_exc
FROM
  pg_stat_user_indexes sui
  JOIN
  pg_index i USING (indexrelid)
WHERE
  NOT schemaname like E'pg\\_temp%'
  AND i.indrelid not in (select relation from q_locked_rels)
  AND i.indexrelid not in (select relation from q_locked_rels)
ORDER BY
  schemaname, relname, indexrelname;
`,
		},
		&Query{
			Table: "pg_locks_mode",
			Tags:  []string{"lockmode"},
			SQL: `
WITH q_locks AS (
  select
    *
  from
    pg_locks
  where
    pid != pg_backend_pid()
    and database = (select oid from pg_database where datname = current_database())
)
SELECT
  lockmodes AS lockmode,
  coalesce((select count(*) FROM q_locks WHERE mode = lockmodes), 0) AS count
FROM
  unnest('{AccessShareLock, ExclusiveLock, RowShareLock, RowExclusiveLock, ShareLock, ShareRowExclusiveLock,  AccessExclusiveLock, ShareUpdateExclusiveLock}'::text[]) lockmodes;
`,
		},
		&Query{
			Table: "pg_table_stats",
			Tags:  []string{"table_name", "schema"},
			SQL: `
select
  quote_ident(schemaname) as schema,
  quote_ident(ut.relname) as table_name,
  pg_table_size(relid) as table_size_b,
  abs(greatest(ceil(log((pg_table_size(relid)+1) / 10^6)), 0))::text
    as table_size_cardinality_mb, -- i.e. 0=<1MB, 1=<10MB, 2=<100MB,..
  pg_total_relation_size(relid) as total_relation_size_b,
  case when reltoastrelid != 0
    then pg_total_relation_size(reltoastrelid)
    else 0::int8 end as toast_size_b,
  (extract(epoch from now() - greatest(last_vacuum, last_autovacuum)))::int8
    as seconds_since_last_vacuum,
  (extract(epoch from now() - greatest(last_analyze, last_autoanalyze)))::int8
    as seconds_since_last_analyze,
  case when 'autovacuum_enabled=off' = ANY(c.reloptions) then 1 else 0 end
    as no_autovacuum,
  seq_scan,
  seq_tup_read,
  coalesce(idx_scan, 0) as idx_scan,
  coalesce(idx_tup_fetch, 0) as idx_tup_fetch,
  n_tup_ins,
  n_tup_upd,
  n_tup_del,
  n_tup_hot_upd,
  n_live_tup,
  n_dead_tup,
  vacuum_count,
  autovacuum_count,
  analyze_count,
  autoanalyze_count,
  age(relfrozenxid) as tx_freeze_age,
  relpersistence
from
  pg_stat_user_tables ut
  join
  pg_class c on c.oid = ut.relid
where
  -- leaving out fully locked tables as pg_relation_size
  -- also wants a lock and would wait
  not exists (select 1 from pg_locks where relation = relid
  and mode = 'AccessExclusiveLock' and granted)
  and c.relpersistence != 't'; -- and temp tables
`,
		},
		&Query{
			Table: "pg_backends",
			Tags:  []string{},
			SQL: `
with sa_snapshot as (
  select * from pg_stat_activity
  where datname = current_database()
  and not query like 'autovacuum:%'
  and pid != pg_backend_pid()
)
select
  (select count(*) from sa_snapshot) as total,
  (select count(*) from pg_stat_activity
    where pid != pg_backend_pid()) as instance_total,
  current_setting('max_connections')::int as max_connections,
  (select count(*) from sa_snapshot where state = 'active') as active,
  (select count(*) from sa_snapshot where state = 'idle') as idle,
  (select count(*) from sa_snapshot
    where state = 'idle in transaction') as idleintransaction,
  (select count(*) from sa_snapshot
    where wait_event_type in ('LWLockNamed', 'Lock', 'BufferPin'))
    as waiting,
  (select extract(epoch from max(now() - query_start))::int
    from sa_snapshot where wait_event_type
    in ('LWLockNamed', 'Lock', 'BufferPin'))
    as longest_waiting_seconds,
  (select extract(epoch from (now() - backend_start))::int
    from sa_snapshot order by backend_start limit 1)
    as longest_session_seconds,
  (select extract(epoch from (now() - xact_start))::int
    from sa_snapshot where xact_start is not null
    order by xact_start limit 1) as longest_tx_seconds,
  (select extract(epoch from (now() - xact_start))::int
    from pg_stat_activity where query like 'autovacuum:%'
    order by xact_start limit 1) as longest_autovacuum_seconds,
  (select extract(epoch from max(now() - query_start))::int
    from sa_snapshot where state = 'active') as longest_query_seconds,
  (select max(age(backend_xmin))::int8 from sa_snapshot)
    as max_xmin_age_tx,
  (select count(*) from pg_stat_activity
    where datname = current_database()
    and query like 'autovacuum:%') as av_workers;
`,
		},
	}
)

type Row struct {
	Measure string
	Tags    map[string]interface{}
	Fields  map[string]interface{}
}

type Gathered struct {
	Rows []*Row
}

func (g *Gathered) Combine(o *Gathered) *Gathered {
	g.Rows = append(g.Rows, o.Rows...)
	return g
}

func (g *Gathered) Write(now int64, db string) error {
	for _, row := range g.Rows {
		fmt.Printf("%v", row.Measure)
		fmt.Printf(",datname=%s", db)
		for key, value := range row.Tags {
			fmt.Printf(",%v=%v", key, value)
		}

		first := true
		for key, value := range row.Fields {
			if first {
				fmt.Printf(" ")
				first = false
			} else {
				fmt.Printf(",")
			}

			switch v := value.(type) {
			case string:
				fmt.Printf("%v=\"%v\"", key, strings.ReplaceAll(v, "\"", ""))
			default:
				fmt.Printf("%v=%v", key, value)
			}
		}
		fmt.Printf(" %v\n", now)
	}

	return nil
}

func gatherQuery(ctx context.Context, o *options, conn *pgx.Conn, query *Query) (*Gathered, error) {
	q, err := conn.Query(ctx, query.SQL)
	if err != nil {
		return nil, err
	}

	defer q.Close()

	tags := make(map[string]bool)

	for _, tag := range query.Tags {
		tags[tag] = true
	}

	rows := make([]*Row, 0)

	for q.Next() {
		values, err := q.Values()
		if err != nil {
			return nil, err
		}

		row := &Row{
			Measure: query.Table,
			Tags:    make(map[string]interface{}),
			Fields:  make(map[string]interface{}),
		}

		for key, value := range o.ParsedTags() {
			row.Tags[key] = value
		}

		for i, column := range q.FieldDescriptions() {
			name := string(column.Name)

			if values[i] != nil {
				if ok := tags[name]; ok {
					row.Tags[name] = values[i]
				} else {
					row.Fields[name] = values[i]
				}
			}
		}

		rows = append(rows, row)
	}

	return &Gathered{Rows: rows}, nil
}

func queryStringArray(ctx context.Context, conn *pgx.Conn, sql string) ([]string, error) {
	rows, err := conn.Query(ctx, sql)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	values := []string{}

	for rows.Next() {
		var name string

		rows.Scan(&name)

		values = append(values, name)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return values, nil
}

func databaseNames(ctx context.Context, o *options) ([]string, error) {
	conn, err := pgx.Connect(ctx, o.URL)
	if err != nil {
		return nil, fmt.Errorf("(db-conn): %v", err)
	}

	defer conn.Close(ctx)

	return queryStringArray(ctx, conn, "SELECT datname FROM pg_database where datistemplate = false")
}

func tableNames(ctx context.Context, conn *pgx.Conn) ([]string, error) {
	return queryStringArray(ctx, conn, "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema'")
}

func gatherQueries(ctx context.Context, o *options, conn *pgx.Conn, queries []*Query) (*Gathered, error) {
	g := &Gathered{}

	for _, query := range queries {
		if gathered, err := gatherQuery(ctx, o, conn, query); err != nil {
			return nil, fmt.Errorf("(db-query) %v: %v", query.SQL, err)
		} else {
			g = g.Combine(gathered)
		}
	}

	return g, nil
}

func gatherGueJobs(ctx context.Context, o *options, conn *pgx.Conn) (*Gathered, error) {
	queries := []*Query{
		&Query{
			Table: "pg_gue_jobs",
			Tags:  []string{"job_type", "queue"},
			SQL: `
SELECT 
CASE WHEN queue = '' THEN 'none' ELSE queue END AS queue,
job_type, COUNT(*) AS queue_length,
EXTRACT(SECONDS FROM NOW() - MIN(created_at))::real AS oldest_age,
EXTRACT(SECONDS FROM NOW() - MAX(created_at))::real AS newest_age
FROM gue_jobs
WHERE run_at < NOW()
GROUP BY queue, job_type;
`,
		},
		&Query{
			Table: "pg_gue_job_errors",
			Tags:  []string{"job_type", "queue"},
			SQL: `
SELECT
CASE WHEN queue = '' THEN 'none' ELSE queue END AS queue,
job_type, COUNT(*) AS error_length
FROM gue_jobs
WHERE run_at > NOW()
GROUP BY queue, job_type;
`,
		},
	}

	return gatherQueries(ctx, o, conn, queries)
}

func gatherSpecials(ctx context.Context, o *options, conn *pgx.Conn) (*Gathered, error) {
	tables, err := tableNames(ctx, conn)
	if err != nil {
		return nil, err
	}

	g := &Gathered{}

	for _, table := range tables {
		if table == "gue_jobs" {
			if more, err := gatherGueJobs(ctx, o, conn); err != nil {
				return nil, err
			} else {
				g = g.Combine(more)
			}
		}
	}

	return g, nil
}

func gather(ctx context.Context, o *options) error {
	dbs, err := databaseNames(ctx, o)
	if err != nil {
		return fmt.Errorf("(db-names): %v", err)
	}

	parsed, err := url.Parse(o.URL)
	if err != nil {
		return fmt.Errorf("(db-url): %v", err)
	}

	excluded := make(map[string]bool)

	for _, name := range strings.Split(o.Exclude, ",") {
		excluded[name] = true
	}

	for _, db := range dbs {
		if excluded[db] {
			continue
		}

		parsed.Path = db

		conn, err := pgx.Connect(ctx, parsed.String())
		if err != nil {
			return fmt.Errorf("(db-conn): %v", err)
		}

		defer conn.Close(ctx)

		now := time.Now().UTC().UnixNano()

		for _, query := range Queries {
			if gathered, err := gatherQuery(ctx, o, conn, query); err != nil {
				return fmt.Errorf("(db-query) %v: %v", query.SQL, err)
			} else {
				if err := gathered.Write(now, db); err != nil {
					return err
				}
			}
		}

		if gathered, err := gatherSpecials(ctx, o, conn); err != nil {
			return fmt.Errorf("(db-special): %v", err)
		} else {
			if err := gathered.Write(now, db); err != nil {
				return err
			}
		}
	}

	return nil
}

type options struct {
	URL     string
	Once    bool
	Exclude string
	Tags    string
}

func (o *options) ParsedTags() map[string]string {
	parsed := make(map[string]string)
	expressions := strings.Split(o.Tags, ",")
	for _, expression := range expressions {
		maybePair := strings.Split(expression, "=")
		if len(maybePair) == 2 {
			parsed[maybePair[0]] = maybePair[1]
		}
	}
	return parsed
}

func main() {
	o := &options{
		URL: os.Getenv("DATABASE_URL"),
	}

	flag.BoolVar(&o.Once, "once", false, "run once")
	flag.StringVar(&o.Tags, "tags", "pgm", "tags")
	flag.StringVar(&o.Exclude, "exclude", "rdsadmin", "Exclude")

	flag.Parse()

	ctx := context.Background()

	if o.Once {
		if err := gather(ctx, o); err != nil {
			panic(err)
		}

		return
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGHUP)

	for {
		<-c

		if err := gather(ctx, o); err != nil {
			panic(err)
		}
	}
}
