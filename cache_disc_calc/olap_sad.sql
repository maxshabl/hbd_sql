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
	
	--фильтруем по ночам, замножаем на количество pax-ов, чтоб потом применять к ним скидки и наценки
	rt_q2 as (
		select 
				file_id, rt_id, country_code, contract_number, contract_name, city_code, hotel_code, room_type, characteristic, board, meal_type, currency_code, is_price_per_pax, min_nights, childe_ages, standard_capacity, min_pax, max_pax, max_adult, max_children, max_infant, min_adult, min_children, pax_age,	amount,			
				coalesce( array_length( array( select age from unnest( array[30,30] ) as age where age <@ childe_ages ), 1 ), 0 ) as childs,
				( array( select age from unnest( array[30,30] ) as age where age <@ childe_ages ) ) as child_ages,											
				coalesce( array_length( array( select age from unnest( array[30,30] ) as age where age < lower( childe_ages ) ), 1 ), 0 ) as infants,
				( array( select age from unnest( array[30,30] ) as age where age < lower( childe_ages ) ) ) as infant_ages,
				coalesce( array_length( array( select age from unnest( array[30,30] ) as age where age > upper( childe_ages ) ), 1 ), 0 ) as adults
			from rt_q1 as rt 
				cross join paxes px
			where sum_nights>=(select v from nights) 
			group by  file_id, rt_id, country_code, contract_number, contract_name, city_code, hotel_code, room_type, characteristic, board, meal_type, currency_code, is_price_per_pax, min_nights, childe_ages, standard_capacity, min_pax, max_pax, max_adult, max_children, max_infant, min_adult, min_children, pax_age, amount


	), 
	sad as (
		select rt.file_id, rt.rt_id, rt.country_code, rt.contract_number, rt.contract_name, rt.city_code, rt.hotel_code, rt.room_type, rt.characteristic,  
			rt.is_price_per_pax as rt_is_per_pax, cnsr.is_per_pax as cnsr_is_per_pax, rt.adults, rt.childs, rt.infants,
			case when cnsr.board_code is null then json_agg(0)
				else json_agg( cnsr.amount )
			end as cnsr_amount,
			case when cnsr.board_code is null then  json_agg(0)
				else json_agg( cnsr.percentage )
			end as cnsr_percentage,
			/*case when cnsr.board_code is null then rt.board
				else cnsr.board_code
			end board,*/
			rt.board, cnsr.board_code
			--jsonb_object_agg( 'type', cnsu.type ),
			--cnsu.type, cnsu.application_type
			--cnsu.type
			from rt_q2 rt
				left join hbd_cnsr as cnsr on cnsr.file_id = rt.file_id and ( cnsr.room_type = rt.room_type or cnsr.room_type = '' )
					and ( cnsr.characteristic = rt.characteristic or cnsr.characteristic = '' )
					and ( ( cnsr.min_age is null and cnsr.min_age is null ) or ( cnsr.min_age <= rt.pax_age  and cnsr.max_age >= rt.pax_age ) ) 
					and ( DATE '2018-08-20',  DATE '2018-08-26' ) overlaps ( cnsr.initial_date, cnsr.final_date )
				/*left join hbd_cnsu as cnsu on cnsu.file_id = rt.file_id and ( cnsu.room_type = rt.room_type or cnsu.room_type = '' )
					and ( cnsu.characteristic = rt.characteristic or cnsu.characteristic = '' )
					and ( DATE '2018-08-20',  DATE '2018-08-26' ) overlaps ( cnsu.initial_date, cnsu.final_date )
					and ( cnsu.application_initial_date is null or cnsu.application_initial_date::timestamp < now() ) 
					and ( cnsu.application_final_date is null or cnsu.application_final_date::timestamp > now()) 
					and ( cnsu.adults is null or cnsu.adults <= rt.adults) 
					and case
							when cnsu.type in ('N', 'F') 
								then  cnsu.pax_order = rt.childs and cnsu.min_age <= rt.pax_age and cnsu.min_age <= rt.pax_age								
							when cnsu.type = 'C' 
								then  rt.standard_capacity < ( rt.adults + rt.childs )
							else true
						end	*/
											
		where rt.board is not null and rt.file_id = '1_100539_M_F' 
		group by rt.file_id, rt.rt_id, rt.country_code, rt.contract_number, rt.contract_name, rt.city_code, rt.hotel_code, rt.room_type, rt.characteristic, rt.board, rt.is_price_per_pax, cnsr.is_per_pax,
			rt.standard_capacity, rt.adults, rt.childs, rt.infants,
			-- cnsu.order,cnsu.type, cnsu.application_type,
			grouping sets ( cnsr.board_code, rt.board)
		
	),
	
	-- данные из запроса api
	hbd_api as  ( 
		select   req_id, destination_code, hotel_id,  company_code,  contract_number, contract_name, classification, check_in, check_out, adults, room_type, characteristic, board,  currency, cancelation, promotions, fees, child_age_from, child_age_to,  amount, min_pax, max_pax, min_adult, max_adult, max_child, max_infant
		from hbd_api_csv 
		group by req_id, destination_code, hotel_id,  company_code,  contract_number, contract_name, classification, check_in, check_out, adults, room_type, characteristic, board,  currency, cancelation, promotions, fees, child_age_from, child_age_to,  amount, min_pax, max_pax, min_adult, max_adult, max_child, max_infant

	)
	
	select * from sad limit 100
	
	--select * from hbd_api where contract_number='100539'
	--select * from cnsr_q q 
		--inner join hbd_api h on q.contract_name = h.contract_name and q.contract_number::text = h.contract_number and q.room_type = h.room_type and q.characteristic = h.characteristic and q.board = h.board
	