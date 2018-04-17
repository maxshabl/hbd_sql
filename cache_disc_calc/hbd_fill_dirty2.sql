----------------------------- заполняем hbd_dirty -----------------------------------------------
--alter table hbd_cnct
--	add column date_interval daterange ;
	
--update hbd_cnct set date_interval = daterange( initial_date + days, initial_date + days + 1, '[)' );

drop table if exists hbd_dirty cascade ;
create table hbd_dirty as 
	(select 
		cnct.file_id,
		md5 ( cnct.contract_name || '.' ||cnct.contract_number || '.' || cnct.room_type || '.' || cnct.characteristic || '.' || cnct.base_board ) as rt_id,
		hs."supplierCountryCode" as country_code, 
		hs."supplierCityCode" as city_code, 
		ccon.hotel_code,
		cnct.contract_name,
		ccon.contract_number,
		cnct.room_type||'.'||cnct.characteristic as room_type_id,  
		cnct.base_board ||'-'|| ccon.company_code as meal_type,
		cnct.room_type,
		cnct.characteristic,
	 	case
			when cnct.generic_rate is not null then
				cnct.generic_rate 
			else 
				cnct.specific_rate 
		end as rate,
		cnct.base_board as board,
		0 as cnsr_amount,
		0 as cnsr_percentage,
	 	'' as cnsr_is_per_pax, 
		cnct.date_interval,
		ccon.currency as currency_code, 
		0 as adult_price, 
		0 as child_price,
		cast(cnct.amount as double precision),
		cnct.is_price_per_pax,
		0 as min_nights,		
		int4range( ccon.minimum_child_age::int, ccon.maximum_child_age::int, '[]' ) as childe_ages,
		cnha.standard_capacity,
		cnha.min_pax,
		cnha.max_pax,
		cnha.max_adult,
		cnha.max_children,
		cnha.max_infant,
		cnha.min_adult,
		cnha.min_children,
	 	int4range( 0, 0, '()' ) as cnsr_pax_ages
	from hbd_cnct as cnct 
	inner join hbd_ccon as ccon 
		ON cnct.file_id=ccon.file_id
	inner join hbd_cnha as cnha on cnha.file_id = cnct.file_id and cnha.room_type = cnct.room_type and cnha.characteristic = cnct.characteristic
	inner join msql_hotel_supplier hs on hs."supplierHotelCode" = ccon.hotel_code and hs."supplierId" = 5
	 )
	
	union all
	
	(select 
		cnct.file_id,
		md5 ( cnct.contract_name || '.' ||cnct.contract_number || '.' || cnct.room_type || '.' || cnct.characteristic || '.' || cnsr.board_code ) as rt_id,
		hs."supplierCountryCode" as country_code, 
		hs."supplierCityCode" as city_code, 
		ccon.hotel_code,
		cnct.contract_name,
		ccon.contract_number,
		cnct.room_type||'.'||cnct.characteristic as room_type_id,  
		cnct.base_board ||'-'|| ccon.company_code as meal_type,
		cnct.room_type,
		cnct.characteristic,
		case
			when cnct.generic_rate is not null then
				cnct.generic_rate 
			else 
				cnct.specific_rate 
		end as rate,
		cnsr.board_code as board,
		coalesce( cnsr.amount, 0 ) as cnsr_amount,
		coalesce( cnsr.percentage, 0 ) as cnsr_percentage,
	 	cnsr.is_per_pax as cnsr_is_per_pax, 
		cnct.date_interval,
		ccon.currency as currency_code, 
		0 as adult_price, 
		0 as child_price,
		cast(cnct.amount as double precision),
		cnct.is_price_per_pax,
		0 as min_nights,		
		int4range( ccon.minimum_child_age::int, ccon.maximum_child_age::int, '[]' ) as childe_ages,
		cnha.standard_capacity,
		cnha.min_pax,
		cnha.max_pax,
		cnha.max_adult,
		cnha.max_children,
		cnha.max_infant,
		cnha.min_adult,
		cnha.min_children,
	 	int4range( cnsr.min_age::int, cnsr.max_age::int, '[]' ) as cnsr_pax_ages
	from hbd_cnct as cnct 
	inner join hbd_ccon as ccon 
		ON cnct.file_id=ccon.file_id
	inner join hbd_cnha as cnha on cnha.file_id = cnct.file_id and cnha.room_type = cnct.room_type and cnha.characteristic = cnct.characteristic
	inner join msql_hotel_supplier hs on hs."supplierHotelCode" = ccon.hotel_code and hs."supplierId" = 5
	left join hbd_cnsr as cnsr on cnsr.file_id = cnct.file_id and ( cnsr.room_type = cnct.room_type or cnsr.room_type = '' )
	and ( cnsr.characteristic = cnct.characteristic or cnsr.characteristic = '' ) 
	and NOT ISEMPTY(cnct.date_interval * daterange(cnsr.initial_date, cnsr.final_date, '[)'))
	--and cnsr.initial_date::timestamp <= cnct.initial_date::timestamp and cnsr.final_date::timestamp >= cnct.final_date::timestamp
	and	case
			when cnct.generic_rate is not null then
				cnsr.rate = cnct.generic_rate
			when  cnct.specific_rate != '0' then
				cnsr.rate = cnct.specific_rate 
		end 
	 where cnsr.board_code is not null
	);
	
ALTER TABLE hbd_dirty
    OWNER to oex_dev;

