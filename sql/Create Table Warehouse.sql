-- DIMENSION TABLES

create table dim_city (
   city_id        varchar(20) primary key,
   city_name      varchar(100),
   state          varchar(100),
   office_address varchar(1000)
);

create table dim_customer (
   customer_id      varchar(20) primary key,
   customer_name    varchar(100),
   city_id          varchar(20),
   first_order_date date,
   customer_type    varchar(50),
   guide_name       varchar(100),
   postal_address   varchar(1000),
   foreign key ( city_id )
      references dim_city ( city_id )
);

create table dim_store (
   store_id     varchar(20) primary key,
   store_name   varchar(100),
   city_id      varchar(20),
   phone_number varchar(20),
   foreign key ( city_id )
      references dim_city ( city_id )
);

create table dim_item (
   item_id          varchar(20) primary key,
   item_description varchar(1000),
   item_size        decimal(10,2),
   item_weight      decimal(10,2),
   price            money
);

create table dim_time (
   time_id int primary key,
   day     int,
   month   int,
   quarter int,
   year    int
);

-- FACT TABLES

create table fact_saleitem (
   saleitem_id      varchar(20) primary key,
   customer_id      varchar(20),
   time_id          int,
   item_id          varchar(20),
   quantity_ordered int,
   total_item_price money,
   foreign key ( customer_id )
      references dim_customer ( customer_id ),
   foreign key ( time_id )
      references dim_time ( time_id ),
   foreign key ( item_id )
      references dim_item ( item_id )
);

create table fact_salestore (
   salestore_id  varchar(20) primary key,
   store_id      varchar(20),
   item_id       varchar(20),
   date_id       int,
   quantity_sale int,
   foreign key ( store_id )
      references dim_store ( store_id ),
   foreign key ( item_id )
      references dim_item ( item_id ),
   foreign key ( date_id )
      references dim_time ( time_id )
);

create table fact_import (
   import_id       varchar(20) primary key,
   store_id        varchar(20),
   item_id         varchar(20),
   date_id         int,
   quantity_import int,
   foreign key ( store_id )
      references dim_store ( store_id ),
   foreign key ( item_id )
      references dim_item ( item_id ),
   foreign key ( date_id )
      references dim_time ( time_id )
);