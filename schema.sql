begin;

create table issues(
    id integer primary key,
    publish_date date not null,
    url varchar not null
);

create table entries(
    id integer primary key,
    issue_id integer,
    title varchar not null,
    author varchar,
    content varchar,
    main_link varchar,
    other_links varchar[],
    tag varchar,

    foreign key(issue_id) references issues(id)
);

commit;
