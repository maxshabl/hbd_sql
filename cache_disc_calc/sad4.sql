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
			from unnest ( array[30,30] ) as pax_age order by pax_age desc
	),
	-- пихаем все в json чтобы  потом использовать в расчете скидок
	bk_days_jsonb as (
		select jsonb_object ( array_agg(bk_days)::text[] , array_agg(count)::text[] ) as bk_days  from bk_days
	),
	
	-- количество ночей
	nights AS (SELECT DATE_PART('day', '2018-08-26'::timestamp - '2018-08-20'::timestamp)::int AS v),
	
	rt_q0 AS (
		SELECT fc.*, upper(fc.date_interval * daterange('2018-08-20', '2018-08-26', '[)')) - lower(fc.date_interval * daterange('2018-08-20', '2018-08-26', '[)')) AS nights, (upper(fc.date_interval * daterange('2018-08-20', '2018-08-26', '[)')) - lower(fc.date_interval * daterange('2018-08-20', '2018-08-26', '[)')))::int * adult_price AS total_adult_price, (upper(fc.date_interval * daterange('2018-08-20', '2018-08-26', '[)')) - lower(fc.date_interval * daterange('2018-08-20', '2018-08-26', '[)')))::int * child_price AS total_child_price FROM hbd_dirty AS fc 
		WHERE NOT ISEMPTY(fc.date_interval * daterange('2018-08-20', '2018-08-26', '[)')) AND fc.min_nights<=(SELECT v FROM nights) and city_code like 'PMI%' 
				
	),

	-- запрос свободных размещений  сгруппировкой для получения суммы
	rt_q1 as (
		select 
			( 0 + row_number() OVER () ) as id, sum( nights ) as sum_nights,
			file_id,  rt_id, country_code, contract_number, contract_name, city_code, hotel_code, room_type, characteristic, rate, board, meal_type, currency_code, is_price_per_pax, 
			min_nights, childe_ages, standard_capacity, min_pax, max_pax, max_adult, max_children, max_infant, min_adult, min_children, amount,	cnsr_amount, cnsr_percentage, cnsr_is_per_pax, cnsr_pax_ages										
			from  rt_q0 rt
			where  2<=rt.max_pax and 2>=rt.min_pax and 2>=rt.min_adult and 2<=rt.max_adult
											
				-- and 0<=rt.max_children
			group by  file_id,  rt_id, country_code, contract_number, contract_name, city_code, hotel_code, room_type, characteristic, rate, board, meal_type, currency_code, is_price_per_pax, 
			min_nights, childe_ages, standard_capacity, min_pax, max_pax, max_adult, max_children, max_infant, min_adult, min_children, amount,	cnsr_amount, cnsr_percentage, cnsr_is_per_pax, cnsr_pax_ages
	),
	
	--фильтруем по ночам, и добавляем данные для поиска
	rt_q2 as (
		select 
			file_id,  rt_id, country_code, contract_number, contract_name, city_code, hotel_code, room_type, characteristic,  board, meal_type, currency_code, is_price_per_pax, 
			min_nights, childe_ages, standard_capacity, min_pax, max_pax, max_adult, max_children, max_infant, min_adult, min_children, amount,	
			json_agg( json_build_object( 'cnsr_amount', cnsr_amount, 'cnsr_percentage', cnsr_percentage, 'cnsr_is_per_pax', cnsr_is_per_pax ) )::text as cnsr_board,
			coalesce( array_length( array( select age from unnest( array[30,30] ) as age where age <@ childe_ages ), 1 ), 0 ) as childs,
			( array( select age from unnest( array[30,30] ) as age where age <@ childe_ages ) ) as child_ages,											
			coalesce( array_length( array( select age from unnest( array[30,30] ) as age where age < lower( childe_ages ) ), 1 ), 0 ) as infants,
			( array( select age from unnest( array[30,30] ) as age where age < lower( childe_ages ) ) ) as infant_ages,
			coalesce( array_length( array( select age from unnest( array[30,30] ) as age where age > upper( childe_ages ) ), 1 ), 0 ) as adults
		from rt_q1 as rt 
			cross join paxes px
		where sum_nights>=( select v from nights ) and ( isempty(cnsr_pax_ages) or (pax_age <@ cnsr_pax_ages) )
		group by  file_id, rt_id, country_code, contract_number, contract_name, city_code, hotel_code, room_type, characteristic,  board, meal_type, currency_code, is_price_per_pax, min_nights, childe_ages, standard_capacity, min_pax, max_pax, max_adult, max_children, max_infant, min_adult, min_children,amount
			--having count(file_id) != 2

	),
	sad as (
		select
			json_agg( json_build_object( 
				'type', cnsu.type, 'application_type', cnsu.application_type, 'amount', cnsu.amount, 'percentage', cnsu.percentage, 'order', cnsu."order", 'is_cumulative', cnsu.is_cumulative,
				'pax_order', cnsu.pax_order, 'adults', cnsu.adults, 'sdcode', cnsu.supplement_or_discount_code,
				'freecode', cngr.free_code, 'frees', cngr.frees, 'ap_base_type', cngr.application_base_type, 'ap_board_type', cngr.application_board_type, 'ap_discount_type', cngr.application_discount_type,
				'ap_stay_type', cngr.application_stay_type
				
			) ) as sad,
			--json_agg( json_build_object( 'code1', cnoe.code1, 'code2', cnoe.code2, 'is_included', cnoe.is_included ) ) as cnoe ,
			rt.file_id,  rt.rt_id, rt.country_code, rt.contract_number, rt.contract_name, rt.city_code, rt.hotel_code, rt.room_type, rt.characteristic,  rt.board,rt.meal_type, rt.currency_code, rt.is_price_per_pax, 
			rt.min_nights, rt.childe_ages, rt.standard_capacity, rt.min_pax, rt.max_pax, rt.max_adult, rt.max_children,rt. max_infant, rt.min_adult, rt.min_children, rt.amount,
			rt.cnsr_board, rt.childs, rt.child_ages, rt.infants, rt.infant_ages, rt.adults
			
		from rt_q2 as rt 
			left join hbd_cnsu as cnsu on cnsu.file_id = rt.file_id and ( cnsu.room_type = rt.room_type or cnsu.room_type = '' )
				and ( cnsu.characteristic = rt.characteristic or cnsu.characteristic = '' )
				and cnsu.initial_date::timestamp <= '2018-08-20'::timestamp and  cnsu.final_date::timestamp >= '2018-08-26'::timestamp
				and ( cnsu.application_initial_date is null or cnsu.application_initial_date::timestamp < now() ) 
				and ( cnsu.application_final_date is null or cnsu.application_final_date::timestamp > now() ) 
				and ( cnsu.adults is null or cnsu.adults <= rt.adults ) 
				and 
				case
					when cnsu.type in ('N', 'F') 
						then  cnsu.pax_order = rt.childs and cnsu.min_age <= any(array[30, 30]) and cnsu.min_age <= any(array[30, 30])								
					when cnsu.type = 'C' 
						then  rt.standard_capacity < ( rt.adults + rt.childs )
					else true
				end
			left join hbd_cngr as cngr on cngr.file_id = rt.file_id 
				and ( cngr.room_type is null or cngr.room_type = rt.room_type ) and ( cngr.characteristic is null or cngr.characteristic = rt.characteristic )
				and ( cngr.board is null or cngr.board = rt.board  or cngr.board = rt.board )
				and ( DATE '2018-08-20',  DATE '2018-08-26' ) overlaps ( cngr.initial_date, cngr.final_date )
				and ( cngr.application_initial_date is null or cngr.application_initial_date::timestamp < now() )
				and ( cngr.application_final_date is null or cngr.application_final_date::timestamp > now() )
				and ( cngr.min_days < ( select v from nights ) and cngr.max_days > ( select v from nights ) )
			left join hbd_cnem as cnem on cnem.file_id = rt.file_id 
				and ( cnem.room_type is null or cnem.room_type = rt.room_type ) and ( cnem.characteristic is null or cnem.characteristic = rt.characteristic )
				and ( cnem.board is null or cnem.board = rt.board  or cnem.board = rt.board )
				and cnem.initial_date::timestamp <= '2018-08-20'::timestamp and  cnem.final_date::timestamp >= '2018-08-26'::timestamp
				and ( cnem.application_date is null or cnem.application_date::timestamp < now() )
			--left join hbd_cnoe as cnoe on cnoe.file_id = rt.file_id
											
			group by rt.file_id,  rt.rt_id, rt.country_code, rt.contract_number, rt.contract_name, rt.city_code, rt.hotel_code, rt.room_type, rt.characteristic,  rt.board,rt.meal_type, rt.currency_code, rt.is_price_per_pax, 
				rt.min_nights, rt.childe_ages, rt.standard_capacity, rt.min_pax, rt.max_pax, rt.max_adult, rt.max_children,rt. max_infant, rt.min_adult, rt.min_children, rt.amount,
				rt.cnsr_board, rt.childs, rt.child_ages, rt.infants, rt.infant_ages, rt.adults
											
	),
	-- данные из запроса api
	hbd_api as ( 
		select   req_id, destination_code, hotel_id,  company_code,  contract_number, contract_name, classification, check_in, check_out, adults, room_type, characteristic, board,  currency, cancelation, promotions, fees, child_age_from, child_age_to,  amount, min_pax, max_pax, min_adult, max_adult, max_child, max_infant
		from hbd_api_csv
		
		group by req_id, destination_code, hotel_id,  company_code,  contract_number, contract_name, classification, check_in, check_out, adults, room_type, characteristic, board,  currency, cancelation, promotions, fees, child_age_from, child_age_to,  amount, min_pax, max_pax, min_adult, max_adult, max_child, max_infant

	)
	
	--select rt_amount, rt_is_per_pax::varchar, array[cnsr_amount::double precision, cnsr_percentage::double precision ], cnsr_is_per_pax::varchar from sad_g 
	select  
		hbd_sad_calc( 
			to_json( array[30, 30] ),
			rt.standard_capacity::int
			rt_amount, 
			rt_is_per_pax,  
			json_agg( json_build_object( 'cnsr_amount', cnsr_amount, 'cnsr_percentage', cnsr_percentage, 'cnsr_is_per_pax', cnsr_is_per_pax ) ) ,
			sad,
			json_agg( json_build_object( 'code1', cnoe.code1, 'code2', cnoe.code2, 'is_included', cnoe.is_included ) ) as cnoe ,


			
		),
	from sad 
		left join hbd_cnoe as cnoe on cnoe.file_id = rt.file_id
	limit 100

	
	--
	--order by room_type, characteristic, board
	--where file_id = '1_100535_M_F'	limit 100
	-- смотри задвоения	
	/*select  
		 file_id, rt.contract_number,rt.contract_name,rt.room_type,rt.characteristic, rt.board
	from sad_q2 rt
		left join hbd_api ha on ha.contract_number = rt.contract_number::text and ha.contract_name = rt.contract_name and ha.room_type = rt.room_type and ha.characteristic = rt.characteristic and ha.board = rt.board
	--where ha.contract_number is null --and file_id = '1_100539_M_F'
	group by file_id, rt.contract_number,rt.contract_name,rt.room_type,rt.characteristic, rt.board
	having count(file_id) > 1*/
	--group by file_id, hotel_code,   contract_number, contract_name, rt.room_type, rt.characteristic, rt.board
	
	