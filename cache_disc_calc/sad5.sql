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
			group by file_id,  rt_id, country_code, contract_number, contract_name, city_code, hotel_code, room_type, characteristic, rate, board, meal_type, currency_code, is_price_per_pax, 
			min_nights, childe_ages, standard_capacity, min_pax, max_pax, max_adult, max_children, max_infant, min_adult, min_children, amount,	cnsr_amount, cnsr_percentage, cnsr_is_per_pax, cnsr_pax_ages
	),
	
	--фильтруем по ночам, и добавляем данные для поиска
	rt_q2 as (
		select
			json_agg( json_build_object( 'cnsr_amount', cnsr_amount, 'cnsr_percentage', cnsr_percentage, 'cnsr_is_per_pax', cnsr_is_per_pax ) )::text as cnsr_board,
			"id", file_id,  rt_id, country_code, contract_number, contract_name, city_code, hotel_code, room_type, characteristic,  board, 
			meal_type, currency_code, is_price_per_pax, min_nights, childe_ages, standard_capacity,
			min_pax, max_pax, max_adult, max_children, max_infant, min_adult, min_children, amount

		from rt_q1 as rt
		cross join paxes
		where sum_nights>=( select v from nights ) and ( isempty( cnsr_pax_ages ) or ( cnsr_pax_ages @> any( array[30,30] ) ) ) 
		group by "id", file_id,  rt_id, country_code, contract_number, contract_name, city_code, hotel_code, room_type, characteristic, rate, board, meal_type, currency_code, is_price_per_pax, 
			min_nights, childe_ages, standard_capacity, min_pax, max_pax, max_adult, max_children, max_infant, min_adult, min_children, amount
	

	),
	
	-- данные из запроса api для проверки выдачи
	hbd_api as ( 
		select   req_id, destination_code, hotel_id,  company_code,  contract_number, contract_name, classification, check_in, check_out, adults, room_type, characteristic, board,  currency, cancelation, promotions, fees, child_age_from, child_age_to,  amount, min_pax, max_pax, min_adult, max_adult, max_child, max_infant
		from hbd_api_csv
		group by req_id, destination_code, hotel_id,  company_code,  contract_number, contract_name, classification, check_in, check_out, adults, room_type, characteristic, board,  currency, cancelation, promotions, fees, child_age_from, child_age_to,  amount, min_pax, max_pax, min_adult, max_adult, max_child, max_infant

	)
	
	select 
	ha.amount,
	hbd_rt_board_calc(
		rt.amount,		
		is_price_per_pax,		
		cnsr_board::json,
		to_json( array[30, 30] ),
		standard_capacity
											
	),
	* 
	from rt_q2 rt
		inner join hbd_api ha on ha.contract_number = rt.contract_number::text and ha.contract_name = rt.contract_name and ha.room_type = rt.room_type and ha.characteristic = rt.characteristic and ha.board = rt.board
	 	
	
	
	