with 

	-- делаем таблицу с днями недели и их количеством в нашем диапазоне
	bk_days as (
		select 
			extract( dow from book_days.book_days::date )::text as bk_days,
			count( book_days )::integer as count	
		from generate_series( lower(daterange('2018-08-20', '2018-08-26', '[)')), upper(daterange('2018-08-20', '2018-08-26', '[)')) - 1, '1 day' ) as book_days
		group by bk_days 
		order by bk_days
	),
	-- делаем таблицу paxes с людьми
	paxes as ( 
		select row_number() over ( order by pax_age desc ) as pax_id, pax_age
			from unnest ( array[30,30,5] ) as pax_age order by pax_age desc
	),
	-- пихаем все в json чтобы  потом использовать в расчете скидок
	bk_days_jsonb as (
		select jsonb_object ( array_agg(bk_days)::text[] , array_agg(count)::text[] ) as bk_days  from bk_days
	),
	
	-- количество ночей
	nights AS (SELECT DATE_PART('day', '2018-08-26'::timestamp - '2018-08-20'::timestamp)::int AS v),
	
	rt_q0 AS (SELECT fc.*, upper(fc.date_interval * daterange('2018-08-20', '2018-08-26', '[)')) - lower(fc.date_interval * daterange('2018-08-20', '2018-08-26', '[)')) AS nights, (upper(fc.date_interval * daterange('2018-08-20', '2018-08-26', '[)')) - lower(fc.date_interval * daterange('2018-08-20', '2018-08-26', '[)')))::int * adult_price AS total_adult_price, (upper(fc.date_interval * daterange('2018-08-20', '2018-08-26', '[)')) - lower(fc.date_interval * daterange('2018-08-20', '2018-08-26', '[)')))::int * child_price AS total_child_price FROM hbd_dirty AS fc WHERE NOT ISEMPTY(fc.date_interval * daterange('2018-08-20', '2018-08-26', '[)')) AND fc.min_nights<=(SELECT v FROM nights) and city_code like 'PMI%' ),

	-- запрос свободных размещений  сгруппировкой для получения суммы
	rt_q1 as (
		select 
			( 0 + row_number() OVER () ) as id, sum( nights ) as sum_nights,
			 file_id, rt_id, country_code, contract_number, contract_name, city_code, hotel_code, room_type, characteristic, board, meal_type, currency_code, sum(amount) as amount, is_price_per_pax, min_nights, childe_ages, standard_capacity, min_pax, max_pax, max_adult, max_children, max_infant, min_adult, min_children
			from  rt_q0 rt
			where  2<=rt.max_pax and 2>=rt.min_pax and 2>=rt.min_adult and 2<=rt.max_adult-- and 0<=rt.max_children
			group by  file_id, rt_id, country_code, contract_number, contract_name, city_code, hotel_code, room_type, characteristic, board, meal_type, currency_code, is_price_per_pax, min_nights, childe_ages, standard_capacity, min_pax, max_pax, max_adult, max_children, max_infant, min_adult, min_children
	),
	
	--фильтруем по ночам
	rt_q2 as (
		select * from rt_q1 where sum_nights>=(select v from nights) 
	), 
	--считаем цены на номер
		
	-- считаем скидки и наценки на завтраки + цены на за номер
	cnsr_q1 as (
		select 
			rt.file_id, rt.id, rt.sum_nights,  rt.rt_id,  rt.country_code, rt.contract_number, rt.contract_name, rt.city_code, rt.hotel_code, rt.room_type, rt.characteristic, rt.board, ( rt.board || '-' || cnsr.board_code ) as meal_type, rt.currency_code, rt.amount, rt.is_price_per_pax, rt.min_nights, rt.childe_ages, rt.standard_capacity, rt.min_pax, rt.max_pax, rt.max_adult, rt.max_children, rt.max_infant, rt.min_adult, rt.min_children,
			cnsr.is_per_pax,
											
			-- получаем цену за номер как за сервис за всех				
			case
				when rt.is_price_per_pax = 'N' then rt.amount
				else 0
			end	as service_base_price,
											
			-- получаем цену на человека 			
			case
				when rt.is_price_per_pax = 'Y' then 
					rt.amount * array_length(array[30,30], 1)
				else 0
			end as pax_base_price,
											
			-- получаем цену за питание как за сервис 
			
			case
				-- расчет если скидка в абсолютных значениях
				when cnsr.is_per_pax = 'N' and cnsr.amount is not null 	
					then cnsr.amount 

				-- расчет если скидка в проыентах																										
				when rt.is_price_per_pax = 'N' and cnsr.is_per_pax = 'N' and cnsr.amount is null 
					then rt.amount * cnsr.percentage / 100 
				when rt.is_price_per_pax = 'Y' and cnsr.is_per_pax = 'N' and cnsr.amount is null																										
					then rt.amount * rt.standard_capacity  * cnsr.percentage / 100 

				else 0
			end  as service_base_board_price,
											
			-- получаем цену за питание за человека
			
			case
				-- расчет если скидка в абсолютных значениях
				when cnsr.is_per_pax = 'Y' and cnsr.amount is not null 	
					then cnsr.amount * array_length( array[30,30], 1 )

				-- расчет если скидка в процентах																					
				when rt.is_price_per_pax = 'N' and cnsr.is_per_pax = 'Y' and cnsr.amount is null																										
					then rt.amount / rt.standard_capacity  * cnsr.percentage / 100  * array_length(array[30,30], 1)					
				when rt.is_price_per_pax = 'Y' and cnsr.is_per_pax = 'Y' and cnsr.amount is null																										
					then cnsr.percentage / 100 * array_length(array[30,30], 1)
				else 0
			end pax_base_board_price
		from cnsr_q0 rt
			inner join hbd_cnsr as cnsr on cnsr.contract_number = rt.contract_number and ( cnsr.room_type = rt.room_type or cnsr.room_type = '' )
				and ( cnsr.characteristic = rt.characteristic or cnsr.characteristic = '' )
				and ( ( cnsr.min_age is null and cnsr.min_age is null ) or ( cnsr.min_age <= any( array[30,30] )  and cnsr.max_age >= any( array[30,30] ) ) ) 
				and ( DATE '2018-08-20',  DATE '2018-08-26' ) overlaps ( cnsr.initial_date, cnsr.final_date )
	),
	
	--группируем и суммируем, получая цены за номер + с
	cnsr_q2 as (
		select 			
			id, rt_id, country_code, contract_number, contract_name, city_code, hotel_code, room_type, characteristic, board, meal_type, currency_code, sum_nights,  is_price_per_pax, min_nights, childe_ages as child_ages_range, standard_capacity, min_pax, max_pax, max_adult, max_children, max_infant, min_adult, min_children,is_per_pax, 
			( max( service_base_price )  + max( pax_base_price ) + sum( service_base_board_price ) + sum( pax_base_board_price ) ) as base_price,
			count( service_base_price ) as c_sbp, count( pax_base_price ) as c_pbp, count( service_base_board_price ) as c_sbbp, count( pax_base_board_price ) as c_pbbp,
			coalesce( array_length( array( select age from unnest( array[30,30] ) as age where age <@ childe_ages ), 1 ), 0 ) as childs,
			( array( select age from unnest( array[30,30] ) as age where age <@ childe_ages ) ) as child_ages,											
			coalesce( array_length( array( select age from unnest( array[30,30] ) as age where age < lower( childe_ages ) ), 1 ), 0 ) as infants,
			( array( select age from unnest( array[30,30] ) as age where age < lower( childe_ages ) ) ) as infant_ages,
			coalesce( array_length( array( select age from unnest( array[30,30] ) as age where age > upper( childe_ages ) ), 1 ), 0 ) as adults
			
		from cnsr_q1 as q0
		group by id, file_id, rt_id, country_code, contract_number, contract_name, city_code, hotel_code, room_type, characteristic, board, meal_type, currency_code, sum_nights,   is_price_per_pax, min_nights, childe_ages, standard_capacity, min_pax, max_pax, max_adult, max_children, max_infant, min_adult, min_children,is_per_pax
		
	),
	-- данные из запроса api
	hbd_api as  ( 
		select   req_id, destination_code, hotel_id,  company_code, contract_number, contract_name, classification, check_in, check_out, adults, room_type, characteristic, board,  currency, cancelation, promotions, fees, child_age_from, child_age_to,  amount, min_pax, max_pax, min_adult, max_adult, max_child, max_infant
		from hbd_api_csv 
		group by req_id, destination_code, hotel_id,  company_code, contract_number, contract_name, classification, check_in, check_out, adults, room_type, characteristic, board,  currency, cancelation, promotions, fees, child_age_from, child_age_to,  amount, min_pax, max_pax, min_adult, max_adult, max_child, max_infant

	)--,
	
	/*cnsu_q0 as (
		select 
			
			cn.id, cn.rt_id, cn.country_code,cn.contract_number, cn.city_code, cn.hotel_code, cn.room_type, cn.characteristic, cn.board, cn.meal_type, cn.currency_code, cn.sum_nights,  cn.is_price_per_pax, cn.min_nights, cn.child_ages_range, cn.standard_capacity, cn.min_pax, cn.max_pax, cn.max_adult, cn.max_children, cn.max_infant, cn.min_adult, cn.min_children,cn.is_per_pax, 
			cnsu.is_per_pax as cnsu_ipp,
			cnsu.order,
			cnsu.type,
			cnsu.application_type,	
			cnsu.amount,			 
			cnsu.percentage ,			
			cnsu.is_cumulative,
			cnsu.supplement_or_discount_code as cnsu_code,							
			pax_id,

				-- убираем одинаковые скидки по разной дате application_initial_date
				case
					when (
							LAG(cnsu.type) OVER(ORDER BY cn.rt_id, pax_id, cnsu.order) = cnsu.type and LAG(cnsu.application_type) OVER(ORDER BY cn.rt_id, pax_id, cnsu.order) = cnsu.application_type
							and LAG(pax_id) OVER(ORDER BY cn.rt_id, pax_id, cnsu.order) = pax_id 
							and LAG(cnsu.application_initial_date) OVER(ORDER BY cn.rt_id, pax_id, cnsu.order)::timestamp > cnsu.application_initial_date::timestamp 
						)
						or(
							LEAD(cnsu.type) OVER(ORDER BY cn.rt_id, pax_id, cnsu.order) = cnsu.type and LAG(cnsu.application_type) OVER(ORDER BY cn.rt_id, pax_id, cnsu.order) = cnsu.application_type
							and LEAD(pax_id) OVER(ORDER BY cn.rt_id, pax_id, cnsu.order) = pax_id 
							and LEAD(cnsu.application_initial_date) OVER(ORDER BY cn.rt_id, pax_id, cnsu.order)::timestamp > cnsu.application_initial_date::timestamp	
						)
						
					then 0
					else 1
				end as sad_status
			from cnsr_q1 cn
			cross join paxes as px
			inner join hbd_cnsu cnsu on cnsu.contract_number = cn.contract_number 
				and ( cnsu.room_type = cn.room_type or cnsu.room_type = '') and ( cnsu.board = cn.board or cnsu.board = '') 
				and ( DATE '2018-08-20',  DATE '2018-08-26' ) overlaps ( cnsu.initial_date, cnsu.final_date )
					and ( cnsu.application_initial_date is null or cnsu.application_initial_date::timestamp < now() ) 
					and ( cnsu.application_final_date is null or cnsu.application_final_date::timestamp > now() ) 
					and ( cnsu.adults is null or cnsu.adults <= cn.adults )
					and case
							when cnsu.type in ('N', 'F') 
								then  cnsu.pax_order = px.pax_id and cnsu.min_age <= px.pax_age	and cnsu.max_age >= px.pax_age 						
							when cnsu.type = 'C' 
								then  cn.standard_capacity < ( cn.adults + cn.childs ) and ( px.pax_id - infants ) > cn.standard_capacity
							else true
						end
			order by cn.rt_id, pax_id, cnsu.order 
				
	),
	
	cngr_q0 as (
											
											
		select 
			cn.id, cn.rt_id, cn.country_code,cn.contract_number, cn.city_code, cn.hotel_code, cn.room_type, cn.characteristic, cn.board, cn.meal_type, 
			cn.currency_code, cn.sum_nights,  cn.is_price_per_pax, cn.min_nights, cn.child_ages_range, 
			cn.standard_capacity, cn.min_pax, cn.max_pax, cn.max_adult, cn.max_children, cn.max_infant, cn.min_adult, cn.min_children,cn.is_per_pax, 
			cngr.free_code, cngr.frees, cngr.discount, cngr.application_base_type, cngr.application_board_type, cngr.application_discount_type, cngr.application_stay_type 
			from cnsr_q1 as cn inner join hbd_cngr as cngr
			on cngr.contract_number = cn.contract_number 
			and ( cngr.room_type = cn.room_type or cngr.room_type = '' )
			and ( cngr.board = cn.board or cngr.board = '' )
			and ( DATE '2018-08-20',  DATE '2018-08-26' ) overlaps ( cngr.initial_date, cngr.final_date ) 
			and ( cngr.application_initial_date is null or cngr.application_initial_date::timestamp < now() )
			and ( cngr.application_final_date is null or cngr.application_final_date::timestamp > now() )
			and ( cngr.min_days < cn.sum_nights and cngr.max_days > cn.sum_nights )
			--where cngr.contract_number = 100734 
	), 
	-- объединяем скидки и наценки вместе с frees
	sad_q0 as (
			select 				
				cn.id, cn.rt_id, cn.country_code,cn.contract_number, cn.city_code, cn.hotel_code, cn.room_type, cn.characteristic, cn.board, cn.meal_type, 
				cn.currency_code, cn.sum_nights,  cn.is_price_per_pax, cn.min_nights, cn.child_ages_range, 
				cn.standard_capacity, cn.min_pax, cn.max_pax, cn.max_adult, cn.max_children, cn.max_infant, cn.min_adult, cn.min_children,cn.is_per_pax, 
				cn.cnsu_code, cn.cnsu_ipp, cn.order, cn.type, cn.application_type, cn.amount, cn.percentage , cn.is_cumulative,  cn.pax_id, cn.sad_status,
				0 as frees, 0 as discount, '' as application_base_type, '' as application_board_type, '' as application_discount_type, '' as application_stay_type
			from cnsu_q0 as cn					
				UNION ALL
			select 
				cn.id, cn.rt_id, cn.country_code,cn.contract_number, cn.city_code, cn.hotel_code, cn.room_type, cn.characteristic, cn.board, cn.meal_type, 
				cn.currency_code, cn.sum_nights,  cn.is_price_per_pax, cn.min_nights, cn.child_ages_range, 
				cn.standard_capacity, cn.min_pax, cn.max_pax, cn.max_adult, cn.max_children, cn.max_infant, cn.min_adult, cn.min_children,cn.is_per_pax,
				cn.free_code, '', 10000,'','',0, 0 , '',  0, 1,
				frees, discount, application_base_type, application_board_type, application_discount_type, application_stay_type	
			from cngr_q0 as cn
	)*/
	
	select 
		count(*)
		/*ha.contract_number,
		 ha.hotel_id,
		ha.room_type,
		cnsr.rt_id,
		ha.hotel_id,
		ha.room_type,
		ha.amount,
		cnsr.room_type,
		
		cnsr.*,*/
		--ha.* 
	 from cnsr_q2 cnsr  
--left join hbd_api ha on ha.contract_number::int=cnsr.contract_number and  ha.hotel_id = cnsr.hotel_code and ha.room_type  = cnsr.room_type and ha.characteristic  = cnsr.characteristic  and ha.board = cnsr.board
		--group by cnsr.contract_number, cnsr.hotel_id, cnsr.room_type, cnsr.characteristic, cnsr.board
		--having count( * ) > 1
		--where cnsr.room_type is  null
		--order by ha.contract_number, ha.room_type, ha.characteristic
		
	
	 
	