
drop function if exists _xp_hbd_sad_list(date, date, integer[]); 
 
CREATE FUNCTION _xp_hbd_sad_list(in_date date, out_date date , variadic ages integer[]) 
	--RETURNS setof hbd_sad AS $$
	RETURNS void AS $$
DECLARE
	
BEGIN
	--return query
	drop table if exists _hbd_sad_list;
	create  table _hbd_sad_list as
	with 
	
	-- делаем таблицу с днями недели и их количеством
	bk_days as (
		select 
			count( book_days )::integer as count_bk_days, extract( dow from book_days.book_days::date )::integer as bk_day
			from generate_series( in_date, out_date - 1, '1 day' ) as book_days
			group by bk_day
	),
	
	-- запрашиваем таблицу с информацией о совместимости скидок
	cnoe as (
		select file_id, array_agg( cnoe.code1 ) as code1, json_agg(cnoe.*) as cnoe from hbd_cnoe as cnoe
			group by file_id
	),
	-- помещаем pax в таблицу
	pax_data as ( 
		select row_number() over ( order by pax_age desc ) as pax_id, pax_age, 
			( out_date - in_date ) as rest_days,
			( out_date - in_date ) as rest_nights
			from unnest ( array[30, 30] ) as pax_age order by pax_age desc 
	),
	-- cross join c ccon чтобы получить данные о группировке pax  в adult, child, infant
	ccon_pax as (
		select pax_data.*, ccon.* from hbd_ccon as ccon cross join pax_data
	),
	-- для скидок нужно знать номер ребенка, количество взрослых. Нумеруем детей. ccon соответствует отелю и имеет возрастные границы детей.
	-- для каждой записи ccon нужно определить порядковый номер child 
	ccon_child as ( 
		select 
			row_number() over ( partition by ccon.file_id order by ccon.pax_age ) as child_id,
			ccon.file_id,
			ccon.pax_id
			from ccon_pax as ccon 
			where ccon.minimum_child_age::int <= pax_age and pax_age <= ccon.maximum_child_age::int --and ccon.file_id = '102_162171_M_F'
			order by file_id
	),
	
	ccon_infant as ( 
		select 
			row_number() over ( partition by ccon.file_id order by ccon.pax_age ) as infant_id,
			ccon.file_id,
			ccon.pax_id
			from ccon_pax as ccon 
			where ccon.minimum_child_age::int > pax_age  --and ccon.file_id = '102_162171_M_F'
			order by file_id
	),
	-- соединяем с ccon_pax получая ccon, умноженный на количество pax с pax_id, добавляя child_id + считаем adult
	ccon as (
		select
			--distinct on ( pax.file_id )
			pax.file_id,
			pax.hotel_code,
			pax.rest_days,
			pax.rest_nights,
			child.child_id,
			infant.infant_id,
			pax.pax_id,
			pax.pax_age as age,
			count( pax.pax_id ) over ( partition by pax.file_id ) - 
			( count( infant.infant_id ) over ( partition by pax.file_id ) ) as paxes,
			( count( pax.pax_id ) over ( partition by pax.file_id ) ) -
			( count( child.child_id ) over ( partition by pax.file_id ) ) - 
			( count( infant.infant_id ) over ( partition by pax.file_id ) ) as adults,
			( count( child.child_id ) over ( partition by pax.file_id ) ) as childes,
			( count( infant.infant_id ) over ( partition by pax.file_id ) ) as infants
			from ccon_pax as pax 
			left join ccon_child child on
				pax.file_id = child.file_id and pax.pax_id = child.pax_id											 
			left join ccon_infant infant on 
				pax.file_id = infant.file_id and pax.pax_id = child.pax_id
			--where pax.file_id = '436_138726_M_F'
			
	)
	----------------------------------------------------------	
	, nights AS (SELECT DATE_PART('day', '2018-08-26'::timestamp - '2018-08-20'::timestamp)::int AS v)
	, query AS (SELECT fc.*, upper(fc.date_interval * daterange('2018-08-20', '2018-08-26', '[)')) - lower(fc.date_interval * daterange('2018-08-20', '2018-08-26', '[)')) AS nights, (upper(fc.date_interval * daterange('2018-08-20', '2018-08-26', '[)')) - lower(fc.date_interval * daterange('2018-08-20', '2018-08-26', '[)')))::int * adult_price AS total_adult_price, (upper(fc.date_interval * daterange('2018-08-20', '2018-08-26', '[)')) - lower(fc.date_interval * daterange('2018-08-20', '2018-08-26', '[)')))::int * child_price AS total_child_price FROM fast_caches_170704 AS fc WHERE NOT ISEMPTY(fc.date_interval * daterange('2018-08-20', '2018-08-26', '[)')) AND fc.min_nights<=(SELECT v FROM nights) AND fc.supplier_id=5)
	, q0 AS (SELECT (0 + row_number() OVER ()) as id, sum(nights) as sum_nights, country_code,city_code,main_hotel_code,hotel_code,accommodation_type,pq.room_type, file_id, meal_type,currency_code,supplier_hotel_id,pq.hotel_id,room_type_id,meal_type_id,meal_type_name,room_type_name,sum(total_adult_price) AS adult_price, sum(child_price) AS child_price,'2|0'::text as p0 FROM query as pq JOIN hbd_room_type AS rt ON pq.hotel_code::int=rt.hotel_id AND 2>=rt.min_pax AND 2<=rt.max_pax AND 2>=rt.min_adult AND 2<=rt.max_adult AND 0<=rt.max_children GROUP BY country_code,city_code,main_hotel_code,hotel_code,accommodation_type,pq.room_type,file_id,meal_type,currency_code,supplier_hotel_id,pq.hotel_id,room_type_id,meal_type_id,meal_type_name,room_type_name)
	, pre_rooms as (SELECT q0.id as i0, p0, q0.supplier_hotel_id as h, q0.currency_code as c FROM q0 WHERE true AND q0.sum_nights>=(SELECT v FROM nights))
	, uni AS (SELECT * FROM q0)
	, rooms AS (SELECT DISTINCT ARRAY[p0] AS p, ARRAY[first_value(i0) OVER(partition by ARRAY(SELECT unnest(ARRAY[i0]) ORDER BY 1))] AS r, h, c FROM pre_rooms)
	, dirty_rooms AS (SELECT DISTINCT md5( q.file_id || '.' || q.room_type ) as cnct_id, q.* FROM uni AS q JOIN rooms r ON q.id = ANY(r.r) ORDER BY q.id)
	----------------------------------------------------------
	,
	cnct as (
		select 
			ccon.*,
			/*cnct.cnct_id, 
			cnha.room_type, 
			cnha.characteristic,
			cnct.meal_type as base_board, 
			cipp.is_per_pax as is_price_per_pax,
			cnct.adult_price as amount,			
			cnha.standard_capacity,
			cnha.max_pax,
			cnha.max_children*/
			from dirty_rooms as cnct 
			--inner join hbd_cnct_is_per_pax cipp on md5( cipp.file_id || '.' || cipp.room_type  || '.' || cipp.characteristic ) = cnct.cnct_id
			inner join ccon on ccon.file_id = cnct.file_id			
			inner join hbd_cnha as cnha 
				on cnha.file_id = cnct.file_id and cnct.cnct_id = md5( cnha.file_id || '.' || cnha.room_type  || '.' || cnha.characteristic )  -- присоединяем структуру с параметрами размещения, отбрасывая то, что не подходит по парамерам вместимости						
					--and cnha.max_pax >= ccon.paxes and cnha.max_children >= ccon.childes
			order by cnct.cnct_id, cnha.room_type, cnha.characteristic
			
	),
	-- считаем базовую цену + базовую цену на завтраки 
	cnsr as (
		select						
			cnct.rest_days,
			cnct.rest_nights,
			cnct.paxes,
			cnct.adults,
			cnct.childes,
			cnct.infants,			
			cnct.standard_capacity,
			cnct.max_pax,
			cnct.max_children,						 
			cnct.is_price_per_pax as cnct_is_per_pax,
		    cnsr.is_per_pax as cnsr_is_per_pax,
			cnct.cnct_id, cnct.file_id, cnct.hotel_code, cnct.room_type, cnct.characteristic, cnct.base_board,
			array [ 
				case cnsr.on_sunday when 'Y' then 0 else null end,
				case cnsr.on_monday when 'Y' then 1 else null end,
				case cnsr.on_tuesday when 'Y' then 2 else null end,
				case cnsr.on_wednesday when 'Y' then 3 else null end,
				case cnsr.on_thursday when 'Y' then 4 else null end,
				case cnsr.on_friday when 'Y' then 5 else null end,
				case cnsr.on_saturday when 'Y' then 6 else null end 
			] week_days,
			--cnct.amount,			
			

			-- получаем цену за номер как за сервис за всех				
			case
				when cnct.is_price_per_pax = 'N' then cnct.amount
				else 0
			end	as service_base_price,
											
			-- получаем цену на человека 			
			case
				when cnct.is_price_per_pax = 'Y' then
					cnct.amount / cnct.paxes
				else 0
			end as pax_base_price,
											
			-- получаем цену за питание как за сервис 
			case
				-- расчет если скидка в абсолютных значениях
				when cnsr.is_per_pax = 'N' and cnsr.amount is not null
					then cnsr.amount

				-- расчет если скидка в проыентах																										
				when cnct.is_price_per_pax = 'N' and cnsr.is_per_pax = 'N' and cnsr.amount is null 
					then cnct.amount * cnsr.percentage / 100 
				when cnct.is_price_per_pax = 'Y' and cnsr.is_per_pax = 'N' and cnsr.amount is null																									
					then  ( ( cnct.amount * cnct.standard_capacity ) * cnsr.percentage / 100 )
				else 0
			end  as service_base_board_price,
											
			-- получаем цену за питание за человека
			
			case
				-- расчет если скидка в абсолютных значениях
				when cnsr.is_per_pax = 'Y' and cnsr.amount is not null
					then cnsr.amount * cnct.paxes

				-- расчет если скидка в процентах																					
				when cnct.is_price_per_pax = 'N' and cnsr.is_per_pax = 'Y' and cnsr.amount is null																								
					then  ( ( cnct.amount / cnct.standard_capacity ) * cnsr.percentage / 100 ) * cnct.paxes					
				when cnct.is_price_per_pax = 'Y' and cnsr.is_per_pax = 'Y' and cnsr.amount is null																								
					then  ( cnsr.percentage / 100 )	* cnct.paxes
				else 0
			end pax_base_board_price
											
			from cnct			
			left join hbd_cnsr as cnsr  -- присоединяем наценки по завтракам, учитывая параметры поиска
				on cnsr.file_id = cnct.file_id  
					and ( cnsr.room_type = cnct.room_type or cnsr.room_type is null )  
					and ( cnsr.characteristic = cnct.characteristic or cnsr.characteristic is null ) 
					and ( ( cnsr.min_age is null and cnsr.min_age is null ) or ( cnsr.min_age <= cnct.age  and cnsr.max_age >= cnct.age ) ) 
					and ( in_date,  out_date ) overlaps ( cnsr.initial_date, cnsr.final_date )
			
			--group by cnct.file_id, cnct.room_type, cnct.characteristic, cnct.amount 
			
	),
	-- собираем скидки и наценки 
	cnsu as (
		select
			'cnsu' as sad_name,
			cn.rest_days,
			cn.rest_nights,
			cn.paxes,
			cn.adults,
			cn.childes,
			cn.infants,			
			cn.standard_capacity,
			cn.max_pax,
			cn.max_children,						 
			cn.is_price_per_pax as cnct_is_per_pax,
		    
			cn.cnct_id, cn.file_id, cn.hotel_code, cn.room_type, cn.characteristic, cn.base_board, cnsu.supplement_or_discount_code as sad_code,
			cnsu.order,
			cnsu.type,
			cnsu.application_type,	
			cnsu.amount,
			 
			cnsu.percentage ,			
			cnsu.is_cumulative as cnsu_is_cumulative,
			cnsu.is_per_pax as cnsu_is_per_pax,
			array [ 
				case cnsu.on_sunday when 'Y' then 0 else null end,
				case cnsu.on_monday when 'Y' then 1 else null end,
				case cnsu.on_tuesday when 'Y' then 2 else null end,
				case cnsu.on_wednesday when 'Y' then 3 else null end,
				case cnsu.on_thursday when 'Y' then 4 else null end,
				case cnsu.on_friday when 'Y' then 5 else null end,
				case cnsu.on_saturday when 'Y' then 6 else null end 
			] week_days
			from cnct as cn
			left join hbd_cnsu as cnsu 
				on cnsu.file_id = cn.file_id 
					and ( cnsu.room_type = cn.room_type or cnsu.room_type is null ) and ( cnsu.characteristic = cn.characteristic or cnsu.characteristic is null  ) 
					and ( cnsu.board = cn.base_board  or cnsu.board is null )  
					and ( in_date,  out_date ) overlaps ( cnsu.initial_date, cnsu.final_date )
					and ( cnsu.application_initial_date is null or cnsu.application_initial_date::timestamp < now() ) 
					and ( cnsu.application_final_date is null or cnsu.application_final_date::timestamp > now()) 
					and ( cnsu.adults is null or cnsu.adults <= cn.adults) 
						and case
								when cnsu.type = 'N' 
									then  cnsu.pax_order = cn.child_id and cnsu.min_age <= cn.age and cnsu.min_age <= cn.age
								when cnsu.type = 'F' 
									then  cnsu.pax_order = cn.infant_id and cnsu.min_age <= cn.age and cnsu.min_age <= cn.age
								when cnsu.type = 'C' 
									then  cn.standard_capacity < cn.paxes
								else true
							end
					order by cn.cnct_id, cnsu.order
			 
			
	),	
	
	/*cnoe as (
		select 
			distinct on (cnoe.file_id) array_agg(cnoe.*) over ( partition by cnoe.file_id)
			from hbd_cnoe as cnoe
	),*/
	cngr as (
		select				
			--array_agg(  cnoe.* ) OVER ( partition by cn.cnct_id ) as cnoe,
			
			
			cn.rest_days,
			cn.rest_nights,
			cn.paxes,
			cn.adults,
			cn.childes,
			cn.infants,			
			cn.standard_capacity,
			cn.max_pax,
			cn.max_children,						 
			cn.is_price_per_pax as cnct_is_per_pax,		    
			cn.cnct_id, cn.file_id, cn.hotel_code, cn.room_type, cn.characteristic, cn.base_board,
			cngr.frees,
			cngr.free_code,
			cngr.discount,
			cngr.application_base_type,
			cngr.application_board_type,
			cngr.application_discount_type,
			cngr.application_stay_type,
			array [ 
				case cngr.on_sunday when 'Y' then 0 else null end,
				case cngr.on_monday when 'Y' then 1 else null end,
				case cngr.on_tuesday when 'Y' then 2 else null end,
				case cngr.on_wednesday when 'Y' then 3 else null end,
				case cngr.on_thursday when 'Y' then 4 else null end,
				case cngr.on_friday when 'Y' then 5 else null end,
				case cngr.on_saturday when 'Y' then 6 else null end 
			] week_days
			from cnct as cn
			left join hbd_cngr as cngr 
				on cngr.file_id = cn.file_id 
					and ( cngr.room_type is null or cngr.room_type = cn.room_type ) and ( cngr.characteristic is null or cngr.characteristic = cn.characteristic )
					and ( cngr.board is null or cngr.board = cn.base_board )
					and ( DATE '2018-04-20',  DATE '2018-04-30' ) overlaps ( cngr.initial_date, cngr.final_date )
					and ( cngr.application_initial_date is null or cngr.application_initial_date::timestamp < now() )
					and ( cngr.application_final_date is null or cngr.application_final_date::timestamp > now() )
					and ( cngr.min_days < cn.rest_days and cngr.max_days > cn.rest_days )			
					order by cn.cnct_id, cn.room_type, cn.characteristic
	), 
	-- список suplements and discounts
	 sad_list as (
		( select  'cnsr'::text as sad_name, rest_days, rest_nights, paxes, adults, childes, infants, standard_capacity, max_pax, max_children, 
			cnct_is_per_pax, cnsr_is_per_pax, cnct_id,  
			file_id, hotel_code, room_type, characteristic, base_board, week_days, (select json_agg(bk_days.*) from bk_days) as bk_days,
			service_base_price, pax_base_price,  service_base_board_price, pax_base_board_price, 
			null as sad_code, -100000 as "order", '' as "type", '' as application_type, 0 as amount, 0 as percentage, '' as cnsu_is_per_pax, '' as cnsu_is_cumulative, -- поля cnsu
			0 as frees, 0 as discount, '' as application_base_type ,'' as application_board_type ,'' as application_discount_type ,'' as application_stay_type, -- поля cngr
			( select cnoe.code1 from cnoe where cnoe.file_id = cnsr.file_id ) as cnoe_code1,
			( select cnoe.cnoe from cnoe where cnoe.file_id = cnsr.file_id ) as cnoe
			from cnsr
	
		union all 
	
		select  'cnsu'::text as sad_name, rest_days, rest_nights, paxes, adults, childes, infants, standard_capacity, max_pax, max_children, 
			cnct_is_per_pax, '', cnct_id,  
			file_id, hotel_code, room_type,	characteristic, base_board, week_days, (select json_agg(bk_days.*) from bk_days) as bk_days,
			0, 0,  0, 0, -- поля после расчетов от cnsr
			sad_code, case when "order" is null then 0 else "order" end, "type", application_type, amount, percentage, cnsu_is_per_pax, cnsu_is_cumulative, 
			0,0,'','','','', -- поля cngr
			( select cnoe.code1 from cnoe where cnoe.file_id = cnsu.file_id ) as cnoe_code1,
			( select cnoe.cnoe from cnoe where cnoe.file_id = cnsu.file_id ) as cnoe
			from cnsu
	
		union all
													
		select 'cngr'::text as sad_name, rest_days, rest_nights, paxes, adults, childes, infants, standard_capacity, max_pax, max_children, 
			cnct_is_per_pax, '', cnct_id,  
			file_id, hotel_code, room_type,	characteristic, base_board, week_days, (select json_agg(bk_days.*) from bk_days) as bk_days,
			0, 0,  0, 0, 
			free_code, 100000, '', '', 0, 0, '', '',
			frees, discount, application_base_type, application_board_type, application_discount_type, application_stay_type,
			( select cnoe.code1 from cnoe where cnoe.file_id = cngr.file_id ) as cnoe_code1,
			( select cnoe.cnoe from cnoe where cnoe.file_id = cngr.file_id ) as cnoe
			from cngr 
		)
		order by cnct_id, "order"
	),
	-- теперь нужно дополнительное поле со списком скидок по каждому румтайпу, чтоб по CNOE исключить ненужные
	sad as (
		select sl.*,  array_agg( sad_code ) over ( partition by cnct_id ) sad_list from sad_list as sl
	)
	
	select sad.* from sad;
	-- удаляем несовместимые скидки is_included = 'Y', или скидки, которые применяются только парами cnoe.is_included = 'N' и у них нет пары  
	delete from _hbd_sad_list 
	where ( sad_code || ':' || cnct_id ) in 
	(
		select distinct on ( hl.sad_code, hl.cnct_id ) hl.sad_code || ':' || hl.cnct_id from _hbd_sad_list hl 
			inner join hbd_cnoe cnoe on cnoe.file_id = hl.file_id and cnoe.code2 = hl.sad_code
			left join _hbd_sad_list hl2 on hl2.cnct_id = hl.cnct_id and hl2.sad_code = cnoe.code1
			where cnoe.is_included = 'Y' or ( cnoe.is_included = 'N'  and hl2.cnct_id is null ) 
	);
END;
$$ LANGUAGE plpgsql;

select _xp_hbd_sad_list( '2018-08-20'::date, '2018-08-26'::date, 30, 30 );
--select * from _hbd_sad_list;
	
	select * from _hbd_sad_list
	

	





