create database demodb;
use demodb;

create table stock_prices_stage 
(
    ticker varchar(30),
    as_of_date date,
    open_price double,
    high_price double,
    low_price double,
    close_price double
);

create table stock_prices
(
    id int not null auto_increment,
    ticker varchar(30),
    as_of_date date,
    open_price double,
    high_price double,
    low_price double,
    close_price double,
    created_at timestamp,
    updated_at timestamp,
    primary key (id)
);

create index ids_stock_prices_stage on stock_prices_stage(ticker, as_of_date);
create index ids_stock_prices on stock_prices(ticker, as_of_date);