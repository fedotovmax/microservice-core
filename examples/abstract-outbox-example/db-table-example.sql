create table if not exists events (

  id uuid primary key,

  aggregate_id varchar(100) not null,

  event_topic varchar(100) not null,

  event_type varchar(100) not null, 

  payload jsonb not null,

  status varchar not null default 'new' check(status in ('new', 'done')),

  created_at timestamp not null,

  reserved_to timestamp default null

);



CREATE INDEX CONCURRENTLY idx_events_new_created_at 
ON events (created_at, reserved_to) 
WHERE status = 'new';