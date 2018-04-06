-- Table: public.hbd_cnct
DROP TABLE IF EXISTS public.hbd_api_csv;

CREATE TABLE public.hbd_api_csv
(
    req_id integer,
    destination_code character varying COLLATE pg_catalog."default",
    hotel_id integer DEFAULT 0,
    inc_office character varying COLLATE pg_catalog."default",
    company_code character varying COLLATE pg_catalog."default",
	contract_number character varying COLLATE pg_catalog."default",
	contract_name character varying COLLATE pg_catalog."default",
	classification character varying COLLATE pg_catalog."default",
	check_in date,
	check_out date,
	adults character varying COLLATE pg_catalog."default",
	children character varying COLLATE pg_catalog."default",
	room_type character varying COLLATE pg_catalog."default",
	characteristic character varying COLLATE pg_catalog."default",
	board character varying COLLATE pg_catalog."default",
	selling_price character varying COLLATE pg_catalog."default",
	net_price character varying COLLATE pg_catalog."default",
	price character varying COLLATE pg_catalog."default",
	currency character varying COLLATE pg_catalog."default",
	allotment character varying COLLATE pg_catalog."default",
	expiry_date character varying COLLATE pg_catalog."default",
	packaging character varying COLLATE pg_catalog."default",
	direct_payment character varying COLLATE pg_catalog."default",
	internal_fild character varying COLLATE pg_catalog."default",
	cancelation character varying COLLATE pg_catalog."default",
	promotions character varying COLLATE pg_catalog."default",
	fees character varying COLLATE pg_catalog."default",
	child_age_from integer,
	child_age_to integer,
	internal_fild_2 character varying COLLATE pg_catalog."default",
	amount double precision,
	internal_fild_3 character varying COLLATE pg_catalog."default",
	min_pax integer,
	max_pax integer,
	min_adult integer,
	max_adult integer,
	max_child integer,
	max_infant integer
    
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE public.hbd_api_csv
    OWNER to oex_dev;