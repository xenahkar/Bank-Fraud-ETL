-- STG
create table public.kkar_stg_transactions(
	trans_id varchar,
	trans_date timestamp,
	card_num varchar,
	oper_type varchar,
	amt decimal,
	oper_result varchar,
	terminal varchar
	);

create table public.kkar_stg_terminals(
	terminal_id varchar PRIMARY KEY,
	terminal_type varchar,
	terminal_city varchar,
	terminal_address varchar,
	create_dt date,
	update_dt date
	);

create table public.kkar_stg_blacklist (
	entry_dt date,
	passport_num varchar
	);

create table public.kkar_stg_cards(
	card_num varchar primary key,
	account_num varchar,
	create_dt date,
	update_dt date
	);

create table public.kkar_stg_accounts(
	account_num varchar primary key,
	valid_to date,
	client varchar,
	create_dt date,
	update_dt date
	);

create table public.kkar_stg_clients(
	client_id varchar primary key,
	last_name varchar,
	first_name varchar,
	patronymic varchar,
	date_of_birth date,
	passport_num varchar,
	passport_valid_to date,
	phone varchar,
	create_dt date,
	update_dt date
	);

-- Fact
create table public.kkar_dwh_fact_transactions(
	trans_id varchar unique,
	trans_date timestamp,
	card_num varchar,
	oper_type varchar,
	amt decimal,
	oper_result varchar,
	terminal varchar
	);

create table public.kkar_dwh_fact_passport_blacklist(
	entry_dt date,
	passport_num varchar
	);

-- DWH
create table public.kkar_dwh_dim_terminals(
	terminal_id varchar primary key,
	terminal_type varchar,
	terminal_city varchar,
	terminal_address varchar,
	create_dt date,
	update_dt date
	);

create table public.kkar_dwh_dim_cards(
	card_num varchar primary key,
	account_num varchar,
	create_dt date,
	update_dt date
	);

create table public.kkar_dwh_dim_accounts(
	account_num varchar unique,
	valid_to date,
	client varchar,
	create_dt date,
	update_dt date
	);

create table public.kkar_dwh_dim_clients(
	client_id varchar unique primary key,
	last_name varchar,
	first_name varchar,
	patronymic varchar,
	date_of_birth date,
	passport_num varchar,
	passport_valid_to date,
	phone varchar,
	create_dt date,
	update_dt date
	);

-- report
create table public.kkar_rep_fraud(
	event_dt timestamp,
	passport_num varchar,
	fio varchar,
	phone varchar,
	event_type int,
	report_dt date
	);