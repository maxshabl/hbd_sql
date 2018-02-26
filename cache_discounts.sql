-- Table: public.hbd_dirty_cnct

-- DROP TABLE public.hbd_dirty_cnct;

/*CREATE TABLE IF NOT EXISTS hbd_discounts
(
    file_id character varying ,
    hotel_id integer,
    initial_date date,
    final_date date,
    room_type character varying,
    characteristic character varying,
    is_price_per_pax integer,
	cnsr_is_per_pax character varying,
    base_board character varying,
    amount numeric(20,7),
	age integer
	
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE hbd_discounts
    OWNER to oex_dev;*/

drop function if exists hbd_add_discounts(text, text, int[]);
create function hbd_add_discounts(check_in text, check_out text, VARIADIC ages int[]) returns setof hbd_discounts as
$$
DECLARE
	discounts hbd_discounts ;
	--ages int[] := child_ages || 30 ;
BEGIN
	return query select cnct.file_id, cnct.hotel_id, to_date(cnct.initial_date, 'YYYYMMDD'), to_date(cnct.final_date, 'YYYYMMDD'), cnct.room_type, cnct.characteristic, 
    	cnct.is_price_per_pax, cnsr.is_per_pax, cnct.base_board, cnct.amount, 5
    	from hbd_dirty_cnct as cnct
		inner join hbd_dirty_cnha as cnha on cnha.file_id=cnct.file_id and cnha.room_type=cnct.room_type and cnha.characteristic=cnct.characteristic
		left join hbd_dirty_cnsr as cnsr on cnsr.file_id=cnct.file_id and cnsr.room_type=cnct.room_type and cnsr.characteristic=cnct.characteristic and 
			to_date(cnsr.initial_date, 'YYYYMMDD') < to_date(check_in, 'YYYYMMDD') and to_date(cnsr.final_date, 'YYYYMMDD') > to_date(check_out, 'YYYYMMDD')
		--where (to_date(cnct.initial_date, 'YYYYMMDD') < to_date(check_in, 'YYYYMMDD') AND to_date(cnct.final_date, 'YYYYMMDD') > to_date(check_out, 'YYYYMMDD'))
		where (date: to_date(cnct.initial_date, 'YYYYMMDD'), date: to_date(cnct.final_date, 'YYYYMMDD') overlaps to_date(cnct.final_date, 'YYYYMMDD') > to_date(check_out, 'YYYYMMDD'))
		limit 100 ;
END;
$$
language plpgsql stable ;