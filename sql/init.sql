SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET client_min_messages = warning;
SET row_security = off;
SET default_tablespace = '';
SET default_with_oids = false;

--Setup database
drop database if exists datalake;
create database datalake;
\c datalake;

drop schema if exists ods;
create schema ods;
drop schema if exists staging;
create schema staging;
drop schema if exists insights;
create schema insights;

drop table if exists ods.transactions;
CREATE TABLE ods.transactions (
        "blockTimestamp" varchar,
        "contractAddress" varchar,
        "tokenId" varchar,
        "fromAddress" varchar,
        "toAddress" varchar,
        "quantity" varchar,
        "blockchainEvent" varchar,
        "transactionType" varchar
);

drop table if exists staging.transactions;
CREATE TABLE staging.transactions (
	blockTimestamp timestamp NULL,
	contractAddress varchar NULL,
	tokenId int8 NULL,
	fromAddress varchar NULL,
	toAddress varchar NULL,
	quantity int8 NULL,
	blockchainEvent varchar NULL,
	transactionType varchar NULL
);

drop table if exists ods.metadata;
CREATE TABLE ods.metadata (
        "contract_address" varchar,
        "token_id" varchar,
        "name" varchar,
        "description" varchar,
        "minted_timestamp" varchar,
        "supply" varchar,
        "image_url" varchar,
        "media_url" varchar,
        "external_url" varchar,
        "properties" varchar,
        "metadata_url" varchar,
        "last_refreshed" varchar
);

drop table if exists staging.metadata;
CREATE TABLE staging.metadata (
        contract_address varchar,
        token_id BIGINT,
        name varchar,
        description varchar,
        minted_timestamp timestamp,
        supply integer,
        image_url varchar,
        media_url varchar,
        external_url varchar,
        properties varchar,
        metadata_url varchar,
        last_refreshed timestamp
);

create or replace view insights.metadata_view
as
SELECT 
contract_address, token_id, "name", description, minted_timestamp, supply, media_url, properties, last_refreshed
FROM staging.metadata;

create or replace view insights.transactions_view
as
SELECT blocktimestamp, contractaddress, tokenid, fromaddress, toaddress, quantity, blockchainevent, transactiontype
FROM staging.transactions;

create or replace view insights.total_transfers_by_address
as
select 
to_char(tv.blocktimestamp, 'YYYY-MM') transfer_month_year,
fromaddress,
count(*) total_transfers
from insights.transactions_view tv
inner join insights.metadata_view mv 
on tv."tokenid"=mv."token_id"
group by to_char(tv.blocktimestamp, 'YYYY-MM') ,fromaddress
order by 3 desc;