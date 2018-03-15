
drop function if exists hbd_sad_calc(); 
CREATE FUNCTION hbd_sad_calc() RETURNS setof hbd_sad AS $$

DECLARE
    rec record;
	dsk json;
	-- храним базовые цены за все ночи
	sbp numeric := 0;
	sbbp numeric := 0;
	pbp numeric := 0;
	pbbp numeric := 0;
	
	sad_days integer := 0; -- храним количество скидочных дней
	no_sad_days integer := 0; -- храним количество обычных дней
	-- название предидущей структуры
	sad_name text := '';
	
BEGIN
	for rec in 
with 
	-- делаем таблицу с днями недели и их количеством
	bk_days as (
		select 
			count(book_days)::integer as count_bk_days, extract(dow from book_days.book_days::timestamp)::integer as bk_day
			from generate_series('2018-04-20'::date, '2018-04-29'::date, '1 day') as book_days
			group by bk_day
	),
	-- помещаем pax в таблицу
	pax_data as ( 
		select row_number() over ( order by pax_age desc ) as pax_id, pax_age, 
			(to_date( '2018-04-30', 'YYYY-MM-DD' ) - to_date( '2018-04-20', 'YYYY-MM-DD' )) as rest_days,
			(to_date( '2018-04-30', 'YYYY-MM-DD' ) - to_date( '2018-04-20', 'YYYY-MM-DD' )) as rest_nights
			from unnest ( array[1,5,30,30] ) as pax_age order by pax_age desc 
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
			where pax.file_id = '436_138726_M_F'
			
	),
	
	pre_cnct as (
		select count(cnct_id), cnct_id, file_id, room_type, characteristic, base_board, is_price_per_pax, sum(amount) as amount
			from hbd_cnct2 as cnct
			where ( DATE '2018-04-20',  DATE '2018-04-30' ) overlaps ( cnct.initial_date, cnct.final_date ) and cnct.file_id = '436_138726_M_F'
			group by cnct_id, file_id, room_type, characteristic, base_board, is_price_per_pax
			
			having count(cnct_id) = (EXTRACT(EPOCH FROM timestamptz '2018-04-30') - EXTRACT(EPOCH FROM timestamptz '2018-04-20')) / 60 / 60 / 24
	),
	

	cnct as (
		select 
			ccon.*,
			cnct.cnct_id, cnct.room_type, cnct.characteristic, cnct.base_board, cnct.is_price_per_pax, cnct.amount,			
			cnha.standard_capacity,
			cnha.max_pax,
			cnha.max_children
			from pre_cnct as cnct 
			inner join ccon on ccon.file_id = cnct.file_id
			inner join hbd_cnha as cnha 
				on cnha.file_id = cnct.file_id and cnha.room_type = cnct.room_type and cnha.characteristic = cnct.characteristic  -- присоединяем структуру с параметрами размещения, отбрасывая то, что не подходит по парамерам вместимости						
					and cnha.max_pax >= ccon.paxes and cnha.max_children >= ccon.childes
			order by cnct_id, room_type, characteristic
			
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
				when cnct.is_price_per_pax = 'N' then cnct.amount * cnct.rest_nights
				else 0
			end	as service_base_price,
											
			-- получаем цену на человека 			
			case
				when cnct.is_price_per_pax = 'Y' then 
					cnct.amount / cnct.paxes * cnct.rest_nights
				else 0
			end as pax_base_price,
											
			-- получаем цену за питание как за сервис 
			
			case
				-- расчет если скидка в абсолютных значениях
				when cnsr.is_per_pax = 'N' and cnsr.amount is not null 	
					then cnsr.amount * cnct.rest_nights

				-- расчет если скидка в проыентах																										
				when cnct.is_price_per_pax = 'N' and cnsr.is_per_pax = 'N' and cnsr.amount is null 
					then cnct.amount * cnsr.percentage / 100 * cnct.rest_nights
				when cnct.is_price_per_pax = 'Y' and cnsr.is_per_pax = 'N' and cnsr.amount is null																										
					then  ( ( cnct.amount * cnct.standard_capacity ) * cnsr.percentage / 100 ) * cnct.rest_nights

				else 0
			end  as service_base_board_price,
											
			-- получаем цену за питание за человека
			
			case
				-- расчет если скидка в абсолютных значениях
				when cnsr.is_per_pax = 'Y' and cnsr.amount is not null 	
					then cnsr.amount * cnct.paxes * cnct.rest_nights

				-- расчет если скидка в процентах																					
				when cnct.is_price_per_pax = 'N' and cnsr.is_per_pax = 'Y' and cnsr.amount is null																										
					then  ( ( cnct.amount / cnct.standard_capacity ) * cnsr.percentage / 100 ) * cnct.paxes	* cnct.rest_nights				
				when cnct.is_price_per_pax = 'Y' and cnsr.is_per_pax = 'Y' and cnsr.amount is null																										
					then  ( cnsr.percentage / 100 )	* cnct.paxes * cnct.rest_nights
				else 0
			end pax_base_board_price
											
			from cnct			
			left join hbd_cnsr as cnsr  -- присоединяем наценки по завтракам, учитывая параметры поиска
				on cnsr.file_id = cnct.file_id  
					and ( cnsr.room_type = cnct.room_type or cnsr.room_type is null )  
					and ( cnsr.characteristic = cnct.characteristic or cnsr.characteristic is null ) 
					and ( ( cnsr.min_age is null and cnsr.min_age is null ) or ( cnsr.min_age <= cnct.age  and cnsr.max_age >= cnct.age ) ) 
					and ( DATE '2018-04-20',  DATE '2018-04-30' ) overlaps ( cnsr.initial_date, cnsr.final_date )
			
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
			cnsu.is_cumulative,
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
					and ( DATE '2018-04-20',  DATE '2018-04-30' ) overlaps ( cnsu.initial_date, cnsu.final_date )
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
	 sad as (
		( select  'cnsr'::text as sad_name, rest_days, rest_nights, paxes, adults, childes, infants, standard_capacity, max_pax, max_children, 
			cnct_is_per_pax, cnsr_is_per_pax, cnct_id,  
			file_id, hotel_code, room_type, characteristic, base_board, week_days, (select json_agg(bk_days.*) from bk_days) as bk_days,
			service_base_price, pax_base_price,  service_base_board_price, pax_base_board_price, 
			'' as sad_code, -100000 as "order", '' as "type", '' as application_type, 0 as amount, 0 as percentage, '' as cnsu_is_per_pax,  -- поля cnsu
			0 as frees, 0 as discount, '' as application_base_type ,'' as application_board_type ,'' as application_discount_type ,'' as application_stay_type -- поля cngr
			from cnsr
	
		union all 
	
		select  'cnsu'::text as sad_name, rest_days, rest_nights, paxes, adults, childes, infants, standard_capacity, max_pax, max_children, 
			cnct_is_per_pax, '', cnct_id,  
			file_id, hotel_code, room_type,	characteristic, base_board, week_days, (select json_agg(bk_days.*) from bk_days) as bk_days,
			0, 0,  0, 0, -- поля после расчетов от cnsr
			sad_code, "order", "type", application_type, amount, percentage, cnsu_is_per_pax,
			0,0,'','','','' -- поля cngr
			from cnsu
	
		union all
													
		select 'cngr'::text as sad_name, rest_days, rest_nights, paxes, adults, childes, infants, standard_capacity, max_pax, max_children, 
			cnct_is_per_pax, '', cnct_id,  
			file_id, hotel_code, room_type,	characteristic, base_board, week_days, (select json_agg(bk_days.*) from bk_days) as bk_days,
			0, 0,  0, 0, 
			free_code, 100000, '', '', 0, 0, '',
			frees, discount, application_base_type, application_board_type, application_discount_type, application_stay_type			
			from cngr )
		order by cnct_id, "order"
	),
	cnoe as (
		select cnoe.file_id, cnoe.code1, cnoe.code2, cnoe.is_included as is_incs  
			from hbd_cnoe as cnoe
			
	)

-- считаем скидки
	
    	select sad.* from sad
	loop 
		-- определяем количество дней, когда скидки действуют и не действуют
		for dsk in select * from json_array_elements( rec.bk_days )
		loop
			if rec.application_type = any(array['T', 'U']) 
				sad_days := 1;
				no_sad_days := rest_nights - 1;
			elseif (dsk->>'bk_day')::integer = any(rec.week_days) then
				sad_days := sad_days + (dsk->>'count_bk_day')::integer;
			else
				no_sad_days := no_sad_days + (dsk->>'count_bk_day')::integer;
				--raise notice '%',  dsk->>'bk_day';
			end if;
		end loop;
		
		
		
		-- считаем базовые цены на номер и завтрак 
		if rec.sad_name = 'cnsr' then
				sad_name := rec.sad_name;
				
				sbp := rec.service_base_price ;	
				sbbp := rec.service_base_board_price;
				pbp := rec.pax_base_price + pbp;
				pbbp := rec.pax_base_board_price + pbbp;
				
				--continue;
		elseif rec.sad_name = 'cnsu' then
				sad_name := rec.sad_name;
				--	определяем количество дней со скидками и без скидок sad_days no_sad_days
				
				
				if rec.type = 'N' then
				
					-- если скидка на базовую цену
					if rec.application_type = any( array['B', 'N'] ) and rec.cnct_is_per_pax = 'Y' and rec.amount is not null then
						pbp := pbp - pbp / rec.paxes + ( pbp + rec.amount / rec.paxes );
						raise notice '%', pbp;
					elseif rec.application_type = any( array['B', 'N'] ) and rec.cnct_is_per_pax = 'N' and rec.amount is not null then
						sbp := sbp - sbp / rec.standard_capacity + ( sbp / rec.standard_capacity + rec.amount / rec.paxes );
					elseif rec.application_type = any( array['B', 'N'] ) and rec.cnct_is_per_pax = 'Y' and rec.percentage is not null then
						pbp := pbp - pbp / rec.paxes + ( pbp / rec.paxes + rec.percentage / 100 * pbp / rec.paxes );
					elseif rec.application_type = any( array['B', 'N'] ) and rec.cnct_is_per_pax = 'N' and rec.percentage is not null then
						sbp := sbp - sbp / rec.standard_capacity + 
							 sbp / rec.standard_capacity + ( sbp / rec.standard_capacity + rec.percentage / 100 * sbp / rec.standard_capacity ) ;
							
					-- если скидка на базовую цену на завтрак
					elseif rec.application_type = any(array['R', 'N']) and rec.cnsr_is_per_pax = 'Y' and rec.amount is not null then
						pbbp := pbbp - pbbp / rec.paxes + ( pbbp + rec.amount / rec.paxes );
						--raise notice '%', pbbp;
					elseif rec.application_type = any(array['R', 'N']) and rec.cnsr_is_per_pax = 'N' and rec.amount is not null then
						sbbp := sbbp - sbbp / rec.standard_capacity + ( sbbp / rec.standard_capacity + rec.amount / rec.paxes );
					elseif rec.application_type = any(array['R', 'N']) and rec.cnsr_is_per_pax = 'Y' and rec.percentage is not null then
						pbbp := pbbp - pbbp / rec.paxes + ( pbbp / rec.paxes + rec.percentage / 100 * pbbp / rec.paxes );
					elseif rec.application_type = any(array['R', 'N']) and rec.cnsr_is_per_pax = 'N' and rec.percentage is not null then
						sbbp := sbbp - sbbp / rec.standard_capacity + 
							 sbbp / rec.standard_capacity + ( sbbp / rec.standard_capacity + rec.percentage / 100 * sbbp / rec.standard_capacity ) ;
							
					-- абсолютная скидка на базовую цену. Завтрак остается без изменений???
					elseif rec.application_type = 'A' and rec.cnct_is_per_pax = 'Y' and rec.amount is not null then
						pbp := pbp - pbp / rec.paxes + rec.amount / rec.paxes ;
						raise notice '%', pbp;
					elseif rec.application_type = 'A' and rec.cnct_is_per_pax = 'N' and rec.amount is not null then
						sbp := sbp - sbp / rec.standard_capacity + rec.amount / rec.paxes ;					

					-- aбсолютная скидка на базовую цену, но к ней над прибавить базовую цену на завтрак. Сам завтрак остается без изменений???
					elseif rec.application_type = 'M' and rec.cnct_is_per_pax = 'Y' and rec.cnsr_is_per_pax = 'N'
						and rec.amount is not null then
							pbp := pbp - pbp / rec.paxes + rec.amount / rec.paxes + pbbp / rec.paxes;
								raise notice '%', pbp;
					elseif rec.application_type = 'M' and rec.cnct_is_per_pax = 'Y' and rec.cnsr_is_per_pax = 'Y'
						and rec.amount is not null 	then
							pbp := pbp - pbp / rec.paxes + rec.amount / rec.paxes + sbbp / rec.paxes;
					elseif rec.application_type = 'M' and rec.cnct_is_per_pax = 'N' and rec.cnsr_is_per_pax = 'N'
						and rec.amount is not null then
							sbp := sbp - sbp / rec.standard_capacity + sbbp / rec.standard_capacity;
					elseif rec.application_type = 'M' and rec.cnct_is_per_pax = 'N' and rec.cnsr_is_per_pax = 'Y'
						and rec.amount is not null 	then
							sbp := sbp - sbp / rec.standard_capacity + pbbp / rec.paxes;
					end if;
					
					
					
				end if;
					
			 
			
		end if;
		--raise notice '%', rec.sad_name ;
		return next rec;
		
	end loop;
    return;
END;
$$ LANGUAGE plpgsql;

select * from hbd_sad_calc();

--SELECT * FROM tmp_table;
	

	




