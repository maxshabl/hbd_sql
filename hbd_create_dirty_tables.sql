DROP TABLE IF EXISTS hbd_dirty CASCADE ;
DROP TABLE IF EXISTS hbd_ccon CASCADE ;
DROP TABLE IF EXISTS hbd_cnha CASCADE ;
DROP TABLE IF EXISTS hbd_cnct CASCADE ;
DROP TABLE IF EXISTS hbd_cnin CASCADE ;
DROP TABLE IF EXISTS hbd_cnsr CASCADE ;
DROP TABLE IF EXISTS hbd_cngr CASCADE ;
DROP TABLE IF EXISTS hbd_cnsu CASCADE ;
DROP TABLE IF EXISTS hbd_cnoe CASCADE ;
DROP TABLE IF EXISTS hbd_cnem CASCADE ;

CREATE TABLE hbd_dirty
(
	file_id character varying COLLATE pg_catalog."default",
    country_code character varying COLLATE pg_catalog."default",
    city_code character varying COLLATE pg_catalog."default",
    hotel_code character varying COLLATE pg_catalog."default",
    room_type character varying COLLATE pg_catalog."default" default '',
	room_type_qty integer,
    meal_type character varying COLLATE pg_catalog."default",
    check_in date,
    check_out date,
    currency_code character varying COLLATE pg_catalog."default",
    adult_price numeric(20,7),
    child_price numeric(20,7),	
	min_nights integer,
    total_price numeric(20,7)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE hbd_dirty
    OWNER to oex_dev;

CREATE TABLE hbd_ccon
(
    file_id character varying COLLATE pg_catalog."default" primary key ,
    external_inventory character varying COLLATE pg_catalog."default",
    destination_code character varying COLLATE pg_catalog."default",
    office_code integer,
    contract_number integer,
    contract_name character varying COLLATE pg_catalog."default",
    company_code character varying COLLATE pg_catalog."default",
    type_of_service character varying COLLATE pg_catalog."default",
    hotel_code character varying COLLATE pg_catalog."default",
    giata_hotel_code integer,
    initial_date date,
    end_date date,
    no_hotel character varying COLLATE pg_catalog."default",
    currency character varying COLLATE pg_catalog."default",
    base_board character varying COLLATE pg_catalog."default",
    classification character varying COLLATE pg_catalog."default",
    payment_model character varying COLLATE pg_catalog."default",
    daily_price character varying COLLATE pg_catalog."default",
    release_days character varying COLLATE pg_catalog."default",
    minimum_child_age integer,
    maximum_child_age integer,
    opaque character varying COLLATE pg_catalog."default",
    fix_rate character varying COLLATE pg_catalog."default",
    contract_type character varying COLLATE pg_catalog."default",
    maximum_rooms integer,
    hotel_content integer,
    selling_price character varying COLLATE pg_catalog."default",
    internal_field character varying COLLATE pg_catalog."default",
    internal_field_data character varying COLLATE pg_catalog."default",
    internal_classification character varying COLLATE pg_catalog."default",
    is_total_price_per_stay character varying COLLATE pg_catalog."default"
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE hbd_ccon
    OWNER to oex_dev;

	
CREATE TABLE hbd_cnsu
(
    file_id character varying COLLATE pg_catalog."default",
    hotel_id integer,
	initial_date date,
	final_date date,
	application_initial_date date,
	application_final_date date,
	supplement_or_discount_code character varying COLLATE pg_catalog."default",
    type character varying COLLATE pg_catalog."default",
	is_per_pax character varying COLLATE pg_catalog."default",
	opaque character varying COLLATE pg_catalog."default",
	"order" integer,
	application_type character varying COLLATE pg_catalog."default",
	amount numeric(20,7) DEFAULT 0,    
	percentage numeric(20,7) DEFAULT 0,
    is_cumulative character varying COLLATE pg_catalog."default",
    rate integer,
	room_type character varying COLLATE pg_catalog."default" default '',
    characteristic character varying COLLATE pg_catalog."default" default '',
    board character varying COLLATE pg_catalog."default",
    adults integer,
    pax_order integer,
    min_age integer,
    max_age integer,
    number_of_days integer,
    length_of_stay integer,
    limit_date character varying COLLATE pg_catalog."default",
    on_monday character varying COLLATE pg_catalog."default",
    on_tuesday character varying COLLATE pg_catalog."default",
    on_wednesday character varying COLLATE pg_catalog."default",
    on_thursday character varying COLLATE pg_catalog."default",
    on_friday character varying COLLATE pg_catalog."default",
    on_saturday character varying COLLATE pg_catalog."default",
    on_sunday character varying COLLATE pg_catalog."default",
	net_price numeric(20,7) DEFAULT 0,
    price numeric(20,7) DEFAULT 0
    
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE hbd_cnsu
    OWNER to oex_dev;


	
CREATE TABLE hbd_cnct
(
    file_id character varying COLLATE pg_catalog."default",
	hotel_id integer,
	id integer primary key,
    initial_date date,
    final_date date,
    room_type character varying COLLATE pg_catalog."default" default '',
    characteristic character varying COLLATE pg_catalog."default" default '',
    "generic Rate" character varying COLLATE pg_catalog."default",
    market_price character varying COLLATE pg_catalog."default",
    is_price_per_pax character varying COLLATE pg_catalog."default",
    net_price character varying COLLATE pg_catalog."default",
    price character varying COLLATE pg_catalog."default",
    "specific Rate" character varying COLLATE pg_catalog."default",
    base_board character varying COLLATE pg_catalog."default",
    amount numeric(20,7)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE hbd_cnct
    OWNER to oex_dev;
	

	
CREATE TABLE hbd_cnin
(
    file_id character varying COLLATE pg_catalog."default",
	hotel_id integer,
	id integer PRIMARY KEY,
    initial_date date,
    final_date date,
    room_type character varying COLLATE pg_catalog."default" default '',
    characteristic character varying COLLATE pg_catalog."default" default '',
    rate integer,
	release integer,
	allotment integer
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE hbd_cnin
    OWNER to oex_dev;
	


	
CREATE TABLE hbd_cnha
(
    file_id character varying COLLATE pg_catalog."default",
	hotel_id integer,    
    room_type character varying COLLATE pg_catalog."default" default '',
    characteristic character varying COLLATE pg_catalog."default" default '',
	standard_capacity integer, 
	min_pax integer, 
	max_pax integer, 
	max_adult integer, 
	max_children integer, 
	max_infant integer, 
	min_adults integer, 
	min_children integer,
	primary key (file_id, room_type, characteristic)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE hbd_cnha
    OWNER to oex_dev;
	
CREATE TABLE hbd_cnsr
(
    file_id character varying COLLATE pg_catalog."default",
	hotel_id integer,    
    initial_date date,
    final_date date,	
    board_code character varying COLLATE pg_catalog."default",	
    is_per_pax character varying COLLATE pg_catalog."default",	
    amount numeric(20,7) DEFAULT 0,    
	percentage numeric(20,7) DEFAULT 0,	
    rate integer,	
    room_type character varying COLLATE pg_catalog."default" default '',	
    characteristic character varying COLLATE pg_catalog."default" default '',	
    min_age integer,	
    max_age integer,	
	on_monday character varying COLLATE pg_catalog."default",
    on_tuesday character varying COLLATE pg_catalog."default",
    on_wednesday character varying COLLATE pg_catalog."default",
    on_thursday character varying COLLATE pg_catalog."default",
    on_friday character varying COLLATE pg_catalog."default",
    on_saturday character varying COLLATE pg_catalog."default",
    on_sunday character varying COLLATE pg_catalog."default",
	internal_field character varying COLLATE pg_catalog."default",
	net_price numeric(20,7) DEFAULT 0,
	price numeric(20,7) DEFAULT 0
	
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE hbd_cnsr
    OWNER to oex_dev;
	
	
CREATE TABLE hbd_cngr
(
    file_id character varying COLLATE pg_catalog."default",
	hotel_id integer,    
    initial_date date,
    final_date date,	
    application_initial_date date,
	application_final_date date,
	min_days integer,    
	max_days integer,
	rate integer,
	room_type character varying COLLATE pg_catalog."default" default '',	
    characteristic character varying COLLATE pg_catalog."default" default '',
    board character varying COLLATE pg_catalog."default",
    frees integer,
	free_code character varying COLLATE pg_catalog."default",
	discount numeric(20,7) DEFAULT 0,
	application_base_type character varying COLLATE pg_catalog."default",
	application_board_type character varying COLLATE pg_catalog."default",
	application_discount_type character varying COLLATE pg_catalog."default",
	application_stay_type character varying COLLATE pg_catalog."default",	
	on_monday character varying COLLATE pg_catalog."default",
    on_tuesday character varying COLLATE pg_catalog."default",
    on_wednesday character varying COLLATE pg_catalog."default",
    on_thursday character varying COLLATE pg_catalog."default",
    on_friday character varying COLLATE pg_catalog."default",
    on_saturday character varying COLLATE pg_catalog."default",
    on_sunday character varying COLLATE pg_catalog."default",
    week_day_validation_type character varying COLLATE pg_catalog."default"
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE hbd_cngr
    OWNER to oex_dev;
	
CREATE TABLE hbd_cnoe
(
    file_id character varying COLLATE pg_catalog."default",
	hotel_id integer,    
    code1 character varying COLLATE pg_catalog."default",
    code2 character varying COLLATE pg_catalog."default",
    is_included character varying COLLATE pg_catalog."default"
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE hbd_cnoe
    OWNER to oex_dev;

CREATE TABLE hbd_cnem
(
    file_id character varying COLLATE pg_catalog."default",
	hotel_id integer,    
    application_date date,
    initial_date date,
    final_date date,
	type character varying COLLATE pg_catalog."default",
	rate integer,
    room_type character varying COLLATE pg_catalog."default" default '',	
    characteristic character varying COLLATE pg_catalog."default" default '',
	board character varying COLLATE pg_catalog."default",
	minimum_days integer,
	maximum_days integer,
	on_monday character varying COLLATE pg_catalog."default",
    on_tuesday character varying COLLATE pg_catalog."default",
    on_wednesday character varying COLLATE pg_catalog."default",
    on_thursday character varying COLLATE pg_catalog."default",
    on_friday character varying COLLATE pg_catalog."default",
    on_saturday character varying COLLATE pg_catalog."default",
    on_sunday character varying COLLATE pg_catalog."default"
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE hbd_cnem
    OWNER to oex_dev;



