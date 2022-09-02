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

	"github.com/jackc/pgx/v4"
)

type Query struct {
	Table string
	SQL   string
	Tags  []string
}

var (
	Queries = []*Query{
		&Query{
			Table: "db_stats",
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
  extract(epoch from (now() - pg_backup_start_time()))::int8
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
			Table: "index_stats",
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
  regexp_replace(regexp_replace(pg_get_indexdef(sui.indexrelid),indexrelname,'X'), '^CREATE UNIQUE','CREATE') as index_def,
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
			Table: "locks_mode",
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
			Table: "table_stats",
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
			Table: "backends",
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
	Tags   map[string]interface{}
	Fields map[string]interface{}
}

func gatherQuery(ctx context.Context, o *options, conn *pgx.Conn, query *Query) ([]*Row, error) {
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
			Tags:   make(map[string]interface{}),
			Fields: make(map[string]interface{}),
		}

		for key, value := range o.ParsedTags() {
			row.Tags[key] = value
		}

		for i, column := range q.FieldDescriptions() {
			name := string(column.Name)

			if ok := tags[name]; ok {
				row.Tags[name] = values[i]
			} else {
				row.Fields[name] = values[i]
			}
		}

		rows = append(rows, row)
	}

	return rows, nil
}

func databaseNames(ctx context.Context, o *options) ([]string, error) {
	conn, err := pgx.Connect(ctx, o.URL)
	if err != nil {
		return nil, fmt.Errorf("(db-conn): %v", err)
	}

	defer conn.Close(ctx)

	rows, err := conn.Query(ctx, "SELECT datname FROM pg_database where datistemplate = false")
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	databases := []string{}

	for rows.Next() {
		var name string

		rows.Scan(&name)

		databases = append(databases, name)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return databases, nil
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

	for _, db := range dbs {
		parsed.Path = db

		conn, err := pgx.Connect(ctx, parsed.String())
		if err != nil {
			return fmt.Errorf("(db-conn): %v", err)
		}

		defer conn.Close(ctx)

		now := time.Now().UTC().Format(time.RFC3339Nano)

		for _, query := range Queries {
			gathered, err := gatherQuery(ctx, o, conn, query)
			if err != nil {
				return fmt.Errorf("(qb-query) %v: %v", query.SQL, err)
			}

			for _, row := range gathered {
				fmt.Printf("%v", query.Table)
				fmt.Printf(",datname=%s", db)
				for key, value := range row.Tags {
					fmt.Printf(",%v=%v", key, value)
				}
				first := true
				for key, value := range row.Fields {
					if first {
						fmt.Printf(" ")
					} else {
						fmt.Printf(",")
					}
					fmt.Printf("%v=%v", key, value)
				}
				fmt.Printf(" %v\n", now)
			}
		}
	}

	return nil
}

type options struct {
	URL  string
	Once bool
	Tags string
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
