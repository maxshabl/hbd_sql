
with 
	-- помещаем pax в таблицу
	pax_data as ( 
		select row_number() OVER ( ORDER BY pax_age desc ) as pax_id, pax_age, 
			(to_date( '2018-03-10', 'YYYY-MM-DD' ) - to_date( '2018-02-28', 'YYYY-MM-DD' )) as rest_days,
			(to_date( '2018-03-10', 'YYYY-MM-DD' ) - to_date( '2018-02-28', 'YYYY-MM-DD' )) - 1 as rest_nights
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
			pax.file_id,
			pax.rest_days,
			pax.rest_nights,
			 count( pax.pax_id ) over ( partition by pax.file_id ) as paxes,
			( count( pax.pax_id ) over ( partition by pax.file_id )) -
			( count( child.child_id ) over ( partition by pax.file_id ) )   as adults,
			( count( child.child_id ) over ( partition by pax.file_id ) ) as childs,
			( count( infant.infant_id ) over ( partition by pax.file_id ) ) as infants,			
			pax.pax_id,
			child.child_id,
			infant.infant_id,			 
			pax.pax_age  
			from ccon_pax as pax 
			left join ccon_child child 
			on pax.file_id = child.file_id and pax.pax_id = child.pax_id											 
			left join ccon_infant infant 
			on pax.file_id = infant.file_id and pax.pax_id = child.pax_id
	),
	
	
	
	-- считаем базовую цену + базовую цену на завтраки Предполагается, что записи в таблице cnct , будут помнрожены на число pax -ов в номере
	-- сами наценки на завтрак замножить ничего не должны
	inc_cnsr as (
		select
			ccon.*,
			cnha.standard_capacity,
			cnha.max_pax,			
			cnct.hotel_id,			
			cnct.initial_date,
			cnct.final_date,
			cnct.room_type, cnct.characteristic,
			cnct.is_price_per_pax as cnct_is_per_pax,
		    cnsr.is_per_pax as cnsr_is_per_pax,
			cnct.base_board,
			cnct.amount,
			-- получаем цену за номер как за сервис							
			case
				when cnct.is_price_per_pax = 'N' then cnct.amount * ccon.rest_days
				else 0
			end as service_base_price,
			-- получаем цену за номер для человека, т е его долю в общей сумме
			case
				when cnct.is_price_per_pax = 'Y' then cnct.amount/ccon.paxes * ccon.rest_days
				else 0
			end as pax_base_price,
			-- получаем цену за питание как за сервис
			case
				when cnsr.is_per_pax = 'N' and cnsr.amount is not null
					then cnsr.amount
				when cnct.is_price_per_pax = 'N' and cnsr.is_per_pax = 'N' and cnsr.amount is null 
					then cnct.amount * cnsr.percentage / 100 * ccon.rest_days
				when cnct.is_price_per_pax = 'Y' and cnsr.is_per_pax = 'N' and cnsr.amount is null
					then cnsr.percentage / 100 * cnha.standard_capacity * cnct.amount * ccon.rest_days
				else 0
			end as service_base_board_price,
			-- получаем цену за питание для данного человека
			case
				when cnsr.is_per_pax = 'Y' and cnsr.amount is not null
					then cnsr.amount
				when cnct.is_price_per_pax = 'N' and cnsr.is_per_pax = 'Y' and cnsr.amount is null 
					then  cnsr.percentage / 100 * cnsr.amount/cnha.standard_capacity * ccon.rest_days
				when cnct.is_price_per_pax = 'Y' and cnsr.is_per_pax = 'Y' and cnsr.amount is null 
					then  cnsr.percentage / 100 * cnsr.amount/cnha.standard_capacity * ccon.rest_days
				else 0
			end as pax_base_board_price
			
			--cnsr.*
			from hbd_cnct as cnct
			cross join pax_data -- перемножаем размещения на количество pax
			inner join ccon on ccon.file_id = cnct.file_id and ccon.pax_id = pax_data.pax_id -- присоединяем данные отелей по pax, получая нужные для подбора скидок параметры
			inner join hbd_cnha as cnha on 
				cnha.file_id = cnct.file_id and cnha.room_type = cnct.room_type and cnha.characteristic = cnct.characteristic and  -- присоединяем структуру с параметрами размещения, отбрасывая то, что не подходит по парамерам вместимости						
				cnha.max_pax >= ccon.paxes and cnha.max_children >= ccon.childs 
			left join hbd_cnsr as cnsr on  -- присоединяем наценки по завтракам, учитывая параметры поиска
				cnsr.file_id = cnct.file_id and 
				( cnsr.room_type = cnct.room_type or cnsr.room_type is null ) and 
				( cnsr.characteristic = cnct.characteristic or cnsr.characteristic is null ) and
				cnsr.board_code = cnct.base_board and ( cnsr.min_age is null and cnsr.min_age is null or cnsr.min_age <= ccon.pax_age and cnsr.max_age >= ccon.pax_age ) and
				cnsr.initial_date::timestamp < to_timestamp( '2018-02-28', 'YYYY-MM-DD' )  /*and 
				cnsr.final_date::timestamp > to_timestamp( '2018-03-10', 'YYYY-MM-DD' )	*/			
			where ( cnct.initial_date::timestamp < to_timestamp( '2018-02-28', 'YYYY-MM-DD' ) and 
				cnct.final_date::timestamp > to_timestamp( '2018-03-10', 'YYYY-MM-DD' ) )
			--group by cnct.file_id, cnct.room_type, cnct.characteristic
	),
	
	-- считаем наценки и скидки на детей и инфантов. Предполагается, что замножений быть не должно. Каждый pax получит не более одной скидки
	inc_cnsu_n as (
		select			
			cn.*,
			cnsu.type,
			case
				-- рассчитываем скидку B - к базовой цене с amount
				when cnsu.application_type = 'B' and cn.cnct_is_per_pax = 'Y'  and
					cnsu.amount is not null and ( cn.cnsr_is_per_pax = 'N' or cn.cnsr_is_per_pax is null ) 
					then cn.pax_base_price + cnsu.amount / cn.paxes + COALESCE( cn.pax_base_board_price, 0 ) / cn.paxes 
											
				when cnsu.application_type = 'B' and cn.cnct_is_per_pax = 'Y'  and
					cnsu.amount is not null and ( cn.cnsr_is_per_pax = 'Y'  or cn.cnsr_is_per_pax is null )
					then cn.pax_base_price + cnsu.amount / cn.paxes + COALESCE( cn.pax_base_board_price, 0 )
											
				when cnsu.application_type = 'B' and cn.cnct_is_per_pax = 'N' and
					cnsu.amount is not null and ( cn.cnsr_is_per_pax = 'N'  or cn.cnsr_is_per_pax is null )
					then cn.service_base_price / cn.standard_capacity + cnsu.amount / cn.paxes + 
						COALESCE( cn.service_base_board_price, 0 ) / cn.paxes
											
				when cnsu.application_type = 'B' and cn.cnct_is_per_pax = 'N'  and
					cnsu.amount is not null and ( cn.cnsr_is_per_pax = 'Y' or cn.cnsr_is_per_pax is null )
					then cn.service_base_price / cn.standard_capacity + cnsu.amount / cn.paxes + 
						COALESCE( cn.pax_base_board_price, 0 ) 
											
				-- рассчитываем скидку B - к базовой цене с percentage, где amount is null
				when cnsu.application_type = 'B' and cn.cnct_is_per_pax = 'Y' and cnsu.amount is  null  and
					( cn.cnsr_is_per_pax = 'N'  or cn.cnsr_is_per_pax is null )
					then ( cn.pax_base_price + cn.pax_base_price * cnsu.percentage / 100 ) + 
						( COALESCE( cn.service_base_board_price, 0 ) / cn.paxes ) 
											
				when cnsu.application_type = 'B' and cn.cnct_is_per_pax = 'Y' and cnsu.amount is  null and 
					( cn.cnsr_is_per_pax = 'Y'  or cn.cnsr_is_per_pax is null ) 
					then ( cn.pax_base_price + cn.pax_base_price * cnsu.percentage / 100 ) + 
						COALESCE( cn.service_base_board_price, 0 ) 
											
				when cnsu.application_type = 'B' and cn.cnct_is_per_pax = 'N' and cnsu.amount is  null and 
					( cn.cnsr_is_per_pax = 'N'  or cn.cnsr_is_per_pax is null ) 
					then cn.service_base_price / cn.standard_capacity + 
						cn.service_base_board_price / cn.standard_capacity * cnsu.percentage / 100 + 
						COALESCE( cn.service_base_board_price, 0 ) / cn.paxes 
											
				when cnsu.application_type = 'B' and cn.cnct_is_per_pax = 'N' and cnsu.amount is  null  and
					( cn.cnsr_is_per_pax = 'Y'  or cn.cnsr_is_per_pax is null)  
					then cn.service_base_price / cn.standard_capacity + 
						cn.service_base_price / cn.standard_capacity * cnsu.percentage / 100 + 
						COALESCE( cn.pax_base_board_price, 0 )
											
				else 0
			end as b_discount,
			
			-- скидка R - завтраки
			case
				-- рассчитываем наценку R - к базовой цене с amount
				when cnsu.application_type = 'R' and cn.cnct_is_per_pax = 'Y'  and
					cnsu.amount is not null and ( cn.cnsr_is_per_pax = 'N' or cn.cnsr_is_per_pax is null ) 
					then cn.pax_base_price + ( COALESCE(cn.service_base_board_price, 0) / cn.paxes + cnsu.amount / cn.paxes ) 
											
				when cnsu.application_type = 'R' and cn.cnct_is_per_pax = 'Y'  and
					cnsu.amount is not null and ( cn.cnsr_is_per_pax = 'Y'  or cn.cnsr_is_per_pax is null )
					then cn.pax_base_price + ( COALESCE(cn.pax_base_board_price, 0) + cnsu.amount / cn.paxes )
											
				when cnsu.application_type = 'R' and cn.cnct_is_per_pax = 'N' and
					cnsu.amount is not null and ( cn.cnsr_is_per_pax = 'N'  or cn.cnsr_is_per_pax is null ) 
					then cn.service_base_price / cn.standard_capacity + COALESCE(cn.service_base_board_price, 0) / cn.paxes +
						cnsu.amount / cn.paxes
						
											
				when cnsu.application_type = 'R' and cn.cnct_is_per_pax = 'N'  and
					cnsu.amount is not null and ( cn.cnsr_is_per_pax = 'Y' or cn.cnsr_is_per_pax is null )
					then cn.service_base_price / cn.standard_capacity + COALESCE( cn.pax_base_board_price, 0 ) / cn.paxes +
						cnsu.amount
											
				-- рассчитываем наценку R - завтрак с percentage, где amount is null
				when cnsu.application_type = 'R' and cn.cnct_is_per_pax = 'Y' and cnsu.amount is  null  and
					( cn.cnsr_is_per_pax = 'N'  or cn.cnsr_is_per_pax is null )
					then cn.pax_base_price + ( COALESCE( cn.service_base_board_price, 0) / cn.paxes + 
						COALESCE( cn.service_base_board_price, 0 ) / cn.paxes * cnsu.percentage / 100 ) 
											
				when cnsu.application_type = 'R' and cn.cnct_is_per_pax = 'Y' and cnsu.amount is  null and 
					( cn.cnsr_is_per_pax = 'Y'  or cn.cnsr_is_per_pax is null )
					then cn.pax_base_price + ( COALESCE( cn.pax_base_board_price, 0 ) + 
						COALESCE(cn.pax_base_board_price, 0) * cnsu.percentage / 100 ) 
											
				when cnsu.application_type = 'R' and cn.cnct_is_per_pax = 'N' and cnsu.amount is  null 	and 
					( cn.cnsr_is_per_pax = 'N'  or cn.cnsr_is_per_pax is null )
					then cn.service_base_price / cn.standard_capacity + ( COALESCE(cn.service_base_board_price, 0) / cn.paxes +
						cnsu.percentage / 100 * COALESCE(cn.service_base_board_price, 0) / cn.paxes )  
											
				when cnsu.application_type = 'R' and cn.cnct_is_per_pax = 'N' and cnsu.amount is  null  and
					( cn.cnsr_is_per_pax = 'Y'  or cn.cnsr_is_per_pax is null )
					then cn.service_base_price / cn.standard_capacity + ( COALESCE(cn.pax_base_board_price, 0) +
						cnsu.percentage / 100 * COALESCE(cn.pax_base_board_price, 0) )											
				else 0
			end as r_discount,
			
			
			case
				-- рассчитываем скидки N , T, U, с amount
				when cnsu.application_type in ( 'N', 'T', 'U' ) and cn.cnct_is_per_pax = 'Y'  and
					cnsu.amount is not null and ( cn.cnsr_is_per_pax = 'N' or cn.cnsr_is_per_pax is null )  
					then ( cn.pax_base_price + cnsu.amount / cn.paxes ) + COALESCE( cn.service_base_board_price, 0 )  / cn.paxes 
											
				when cnsu.application_type in ( 'N', 'T', 'U' ) and cn.cnct_is_per_pax = 'Y'  and
					cnsu.amount is not null and ( cn.cnsr_is_per_pax = 'Y'  or cn.cnsr_is_per_pax is null )
					then ( cn.pax_base_price + cnsu.amount / cn.paxes ) + COALESCE( cn.pax_base_board_price, 0 ) 
											
				when cnsu.application_type in ( 'N', 'T', 'U' ) and cn.cnct_is_per_pax = 'N' and
					cnsu.amount is not null and ( cn.cnsr_is_per_pax = 'N'  or cn.cnsr_is_per_pax is null )
					then cn.service_base_price / cn.standard_capacity + cnsu.amount / cn.paxes + 
						COALESCE( cn.service_base_board_price, 0 ) / cn.paxes
											
				when cnsu.application_type in ( 'N', 'T', 'U' ) and cn.cnct_is_per_pax = 'N'  and
					cnsu.amount is not null and ( cn.cnsr_is_per_pax = 'Y' or cn.cnsr_is_per_pax is null )
					then cn.service_base_price / cn.standard_capacity + cnsu.amount / cn.paxes + 
						COALESCE( cn.pax_base_board_price, 0 ) 
											
				-- рассчитываем скидки N , T, U, где amount is null
				when cnsu.application_type in ( 'N', 'T', 'U' ) and cn.cnct_is_per_pax = 'Y' and cnsu.amount is  null  and
					( cn.cnsr_is_per_pax = 'N'  or cn.cnsr_is_per_pax is null )
					then ( cn.pax_base_price + cn.pax_base_price * cnsu.percentage / 100 ) + 
						( COALESCE(cn.service_base_board_price, 0 ) / cn.paxes + 
						COALESCE(cn.service_base_board_price, 0 ) / cn.paxes * cnsu.percentage / 100) 
											
				when cnsu.application_type in ( 'N', 'T', 'U' ) and cn.cnct_is_per_pax = 'Y' and cnsu.amount is  null and 
					( cn.cnsr_is_per_pax = 'Y'  or cn.cnsr_is_per_pax is null )
					then ( cn.pax_base_price + cn.pax_base_price * cnsu.percentage / 100 ) + 
						( COALESCE( cn.pax_base_board_price, 0 ) * cnsu.percentage / 100 )
											
				when cnsu.application_type in ( 'N', 'T', 'U' ) and cn.cnct_is_per_pax = 'N' and cnsu.amount is  null and 
					( cn.cnsr_is_per_pax = 'N'  or cn.cnsr_is_per_pax is null )
					then ( cn.service_base_price / cn.standard_capacity + 
						cn.service_base_price / cn.standard_capacity * cnsu.percentage / 100 ) + 
						( COALESCE(cn.service_base_board_price, 0) / cn.paxes + 
						COALESCE(cn.service_base_board_price, 0) / cn.paxes * cnsu.percentage / 100 )
											
				when cnsu.application_type in ( 'N', 'T', 'U' ) and cn.cnct_is_per_pax = 'N' and cnsu.amount is  null  and
					( cn.cnsr_is_per_pax = 'Y'  or cn.cnsr_is_per_pax is null )
					then ( cn.service_base_price / cn.standard_capacity + cn.service_base_price / cn.standard_capacity * cnsu.percentage / 100 ) + 
						( COALESCE( cn.pax_base_board_price, 0 ) + COALESCE( cn.pax_base_board_price, 0 ) * cnsu.percentage / 100 )
											
				else 0
			end as ntu_discount,
			
			--- т к рассчитывается три типа скидки, то нужно поле с укзанием какая именнно была рассчитана			
			case
				when cnsu.application_type = 'N' then 'N'
				when cnsu.application_type = 'N' then 'T'
				when cnsu.application_type = 'N' then 'U'
			end as ntu_type,
			
			case
				-- рассчитываем наценку A  - переписывает базовую цену для child amount
				when cnsu.application_type = 'A' 
					then cnsu.amount / cn.paxes
				else 0
			end as a_discount,
											
			case
				-- рассчитываем наценку M  - переписывает базовую цену для child amount + board_price
				when cnsu.application_type = 'M' and cn.cnct_is_per_pax = 'Y'  and
					( cn.cnsr_is_per_pax = 'N'  or cn.cnsr_is_per_pax is null ) 
					then cnsu.amount / cn.paxes + COALESCE(cn.service_base_board_price, 0) / cn.paxes	
				when cnsu.application_type = 'M' and cn.cnct_is_per_pax = 'Y'  and
					( cn.cnsr_is_per_pax = 'Y' or cn.cnsr_is_per_pax is null )
					then cnsu.amount / cn.paxes + COALESCE(cn.pax_base_board_price, 0)					
				when cnsu.application_type = 'M' and cn.cnct_is_per_pax = 'N' and
					( cn.cnsr_is_per_pax = 'N'  or cn.cnsr_is_per_pax is null )
					then cnsu.amount / cn.paxes + COALESCE(cn.service_base_board_price, 0) / cn.standard_capacity
				when cnsu.application_type = 'M' and cn.cnct_is_per_pax = 'N'  and
					( cn.cnsr_is_per_pax = 'Y' or cn.cnsr_is_per_pax is null )
					then cnsu.amount / cn.paxes + COALESCE(cn.pax_base_board_price, 0)
										
				else 0
			end as m_discount
		
		from inc_cnsr cn
		left join hbd_cnsu cnsu on cnsu.file_id = cn.file_id and
			( cnsu.room_type = cn.room_type or cnsu.room_type is null ) and ( cnsu.characteristic = cn.characteristic or cnsu.characteristic is null  ) and
			( cnsu.board = cn.base_board  or cnsu.board is null ) and 
			cnsu.initial_date::timestamp < to_timestamp( '2018-02-28', 'YYYY-MM-DD' ) and 
			cnsu.final_date::timestamp > to_timestamp( '2018-03-10', 'YYYY-MM-DD' ) and		
			( cnsu.application_initial_date::timestamp < now() or cnsu.application_initial_date is null ) and ( cnsu.application_final_date::timestamp > now() or cnsu.application_initial_date is null ) and
			( ( cnsu.type = 'C' and cn.standard_capacity < cn.paxes )  or 
			( cnsu.adults <= cn.adults and  cnsu.pax_order = cn.child_id and cnsu.min_age <= cn.pax_age and cnsu.max_age >= cn.pax_age and cnsu.type in ( 'N', 'F' )  )  )

		
	), 
	inc_cnsu_n_summ as (
		select cn.file_id, cn.hotel_id, cn.room_type, cn.characteristic, cn.base_board, cn.amount, cn.ntu_type, cn.rest_days, cn.rest_nights, cn.paxes, cn.adults, cn.childs, cn.infants, 
			cn.standard_capacity, 
			max(cn.service_base_price) as service_base_price,
			max(cn.service_base_board_price) as service_base_board_price, 
			sum(cn.pax_base_price) as pax_base_price,
			sum(cn.pax_base_board_price) as pax_base_board_price,
			sum(cn.b_discount) as b_discount,
			sum(cn.r_discount) as r_discount,
			sum(cn.a_discount) as a_discount,
			sum(cn.m_discount) as m_discount,
			sum(cn.ntu_discount) as ntu_discount
			
		from inc_cnsu_n as cn 
		group by cn.file_id, cn.hotel_id, cn.room_type, cn.characteristic, cn.base_board, cn.amount, cn.ntu_type, cn.rest_days, cn.rest_nights, cn.paxes, cn.adults, cn.childs, cn.infants, 
			cn.standard_capacity
	),
	inc_cnsu_g as (
		select * from inc_cnsu_n_summ as cn
			left join  hbd_cnsu cnsu on cnsu.file_id = cn.file_id and
			( cnsu.room_type = cn.room_type or cnsu.room_type is null ) and ( cnsu.characteristic = cn.characteristic or cnsu.characteristic is null  ) and
			( cnsu.board = cn.base_board  or cnsu.board is null ) and type in ('B', 'K', 'U', 'L', 'M', '0', 'E', 'V', G)
			order by cnsu.file_id, cnsu.type
				
	)
	
	--select * from inc_cnsu_n_summ where b_discount != 0 or r_discount != 0 or a_discount != 0 or m_discount != 0 or ntu_discount != 0 limit 100
	--select * from inc_cnsu_n_summ where pax_base_price != 0 or b_discount != 0 or r_discount != 0 or a_discount != 0 or m_discount != 0 or ntu_discount != 0 limit 100
	--select count(*) from inc_cnsu_n where type is not null limit 100
	
	select * from inc_cnsu_g as cn limit 100


	/*, cn.rest_days, cn.rest_nights, cn.paxes, cn.adults, cn.childs, cn.infants, 
			cn.standard_capacity, cn.service_base_price, cn.service_base_board_price*/
	
	
/*with 
	cnct as (
		SELECT  file_id, hotel_id,  room_type, characteristic,  is_price_per_pax, 
		net_price, price, "specific Rate", base_board, amount
		FROM public.hbd_cnct
		GROUP BY file_id, hotel_id, room_type, characteristic,  is_price_per_pax, 
		net_price, price, "specific Rate", base_board, amount )
	
	
	SELECT count(*) FROM cnct;
	--SELECT count(*) FROM public.hbd_cnct;*/
	



