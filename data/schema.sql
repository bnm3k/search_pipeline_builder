begin;

install vss;

create table issues(
    id integer primary key,
    publish_date date not null,
    url varchar not null,
    num_entries integer not null default 0
);

create sequence if not exists entries_id_sequence start 1;

create table entries(
    id uinteger primary key default nextval('entries_id_sequence'),
    issue_id integer,
    title varchar not null,
    author varchar,
    content varchar,
    main_link varchar,
    other_links varchar[],
    tag varchar,

    foreign key(issue_id) references issues(id)
);


create sequence embeddings_metadata_id_sequence start 1;
create table embeddings_metadata(
    id integer primary key
        default nextval('embeddings_metadata_id_sequence'),
    model_name varchar not null unique,
    dimension integer not null,
    index_filename varchar
);

commit;
