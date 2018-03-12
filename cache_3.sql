
with 
	-- помещаем pax в таблицу
	pax_data as ( 
		select row_number() over ( order by pax_age desc ) as pax_id, pax_age, 
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
			--distinct on (pax.file_id, pax.rest_days, pax.rest_nights)
			pax.file_id,
			pax.hotel_code,
			pax.rest_days,
			pax.rest_nights,
			count( pax.pax_id ) over ( partition by pax.file_id ) as paxes,
			( count( pax.pax_id ) over ( partition by pax.file_id ) ) -
			( count( child.child_id ) over ( partition by pax.file_id ) ) as adults,
			( count( child.child_id ) over ( partition by pax.file_id ) ) as childs,
			( count( infant.infant_id ) over ( partition by pax.file_id ) ) as infants,
			array_agg(pax.pax_age) over ( partition by pax.file_id ) as ages
			from ccon_pax as pax 
			left join ccon_child child on
				pax.file_id = child.file_id and pax.pax_id = child.pax_id											 
			left join ccon_infant infant on 
				pax.file_id = infant.file_id and pax.pax_id = child.pax_id
			
	),
	
	-- считаем базовую цену + базовую цену на завтраки 
	cnsr as (
		select
			distinct on ( cnct.cnct_id )
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

			-- получаем цену за номер как за сервис за всех.						
			
			case
				when cnct.is_price_per_pax = 'N' then cnct.amount 
				else 0
			end	as service_base_price,
											
			-- получаем сумму за всех 
			
			sum( 
				case
					when cnct.is_price_per_pax = 'Y' then cnct.amount/ccon.paxes 
					else 0
				end 
			) over ( partition by cnct.cnct_id ) as pax_base_price,
											
			-- получаем цену за питание как за сервис 
			sum(
				case
					when cnsr.is_per_pax = 'N' and cnsr.amount is not null 	
						then cnsr.amount																															
					when cnct.is_price_per_pax = 'N' and cnsr.is_per_pax = 'N' and cnsr.amount is null 
						then cnct.amount * cnsr.percentage / 100 																															
					when cnct.is_price_per_pax = 'Y' and cnsr.is_per_pax = 'N' and cnsr.amount is null																										
						then cnsr.percentage / 100 * cnha.standard_capacity * cnct.amount
																															
					else 0
				end  
			) over ( partition by cnct.cnct_id ) as service_base_board_price,
											
			-- получаем сумму за всех
			sum(
				case
					when cnsr.is_per_pax = 'Y' and cnsr.amount is not null
						then cnsr.amount
					when cnct.is_price_per_pax = 'N' and cnsr.is_per_pax = 'Y' and cnsr.amount is null 
						then cnsr.percentage / 100 * cnsr.amount/cnha.standard_capacity
					when cnct.is_price_per_pax = 'Y' and cnsr.is_per_pax = 'Y' and cnsr.amount is null 
						then cnsr.percentage / 100 * cnsr.amount/cnha.standard_capacity
					else 0
				end 
			) over ( partition by cnct.cnct_id ) as pax_base_board_price
			
			--cnsr.*
			from hbd_cnct2 as cnct			
			inner join ccon on ccon.file_id = cnct.file_id
			inner join hbd_cnha as cnha on
				cnha.file_id = cnct.file_id and cnha.room_type = cnct.room_type and cnha.characteristic = cnct.characteristic and  -- присоединяем структуру с параметрами размещения, отбрасывая то, что не подходит по парамерам вместимости						
				cnha.max_pax >= ccon.paxes and cnha.max_children >= ccon.childs 
			left join hbd_cnsr as cnsr on  -- присоединяем наценки по завтракам, учитывая параметры поиска
				cnsr.file_id = cnct.file_id and 
				( cnsr.room_type = cnct.room_type or cnsr.room_type is null ) and 
				( cnsr.characteristic = cnct.characteristic or cnsr.characteristic is null ) and
				cnsr.board_code = cnct.base_board and ( cnsr.min_age is null and cnsr.min_age is null or cnsr.min_age <= any( ccon.ages )  and cnsr.max_age >= any( ccon.ages ) ) and
				( 
					( cnsr.initial_date::timestamp < to_timestamp( '2018-02-28', 'YYYY-MM-DD' )  and  cnsr.final_date::timestamp > to_timestamp( '2018-03-10', 'YYYY-MM-DD' ) ) /*or
					( cnsr.initial_date::timestamp < to_timestamp( '2018-02-28', 'YYYY-MM-DD' ) and (to_timestamp( '2018-03-10', 'YYYY-MM-DD' ) - cnsr.final_date::timestamp ) < ( ccon.rest_days * 86400 ) ) or
					( (cnsr.initial_date::timestamp - to_timestamp( '2018-02-28', 'YYYY-MM-DD' ) < ( ccon.rest_days * 86400 ) ) and cnsr.final_date::timestamp > to_timestamp( '2018-03-10', 'YYYY-MM-DD' ))*/
				)
			where ( cnct.initial_date::timestamp < to_timestamp( '2018-02-28', 'YYYY-MM-DD' ) and 
				cnct.final_date::timestamp > to_timestamp( '2018-03-10', 'YYYY-MM-DD' ) )
			--group by cnct.file_id, cnct.room_type, cnct.characteristic, cnct.amount 
			
	),
	cnsu as (
		select
			cn.*,
			cnsu.amount,
			cnsu.percentage,
			cnsu.type,
			cnsu.application_type,
			cnsu.is_cumulative,
			cnsu.is_per_pax as cnsu_is_per_pax
											
											
			from cnsr as cn
			left join hbd_cnsu as cnsu on cnsu.file_id = cn.file_id and
			( cnsu.room_type = cn.room_type or cnsu.room_type is null ) and ( cnsu.characteristic = cn.characteristic or cnsu.characteristic is null  ) and
			( cnsu.board = cn.base_board  or cnsu.board is null ) and 
			cnsu.initial_date::timestamp < to_timestamp( '2018-02-28', 'YYYY-MM-DD' ) and 
			cnsu.final_date::timestamp > to_timestamp( '2018-03-10', 'YYYY-MM-DD' ) and		
			( cnsu.application_initial_date::timestamp < now() or cnsu.application_initial_date is null ) and 
			( cnsu.application_final_date::timestamp > now() or cnsu.application_initial_date is null ) and ( cnsu.adults <= cn.adults or cnsu.adults is null ) and
			case
				when cnsu.type = 'N' then  cnsu.pax_order <= cn.childs
				when cnsu.type = 'F' then  cnsu.pax_order <= cn.infants
				when cnsu.type = 'C' then  cn.standard_capacity < cn.paxes
			end
			order by cn.file_id, cn.room_type, cn.characteristic, cn.base_board, cnsu.order
			 
			
	),
	cngr as (
		select 
			cn.*,
			cngr.frees,
			cngr.free_code,
			cngr.discount,
			cngr.application_base_type,
			cngr.application_board_type,
			cngr.application_discount_type,
			cngr.application_stay_type
			from cnsr as cn
			left join hbd_cngr as cngr on cngr.file_id = cn.file_id and
				( cngr.room_type = cn.room_type or cngr.room_type is null ) and ( cngr.characteristic = cn.characteristic or cngr.characteristic is null  ) and
				( cngr.board = cn.base_board  or cngr.board is null ) and 
				cngr.initial_date::timestamp < to_timestamp( '2018-02-28', 'YYYY-MM-DD' ) and 
				cngr.final_date::timestamp > to_timestamp( '2018-03-10', 'YYYY-MM-DD' ) and		
				( cngr.application_initial_date::timestamp < now() or cngr.application_initial_date is null ) and 
				( cngr.application_final_date::timestamp > now() or cngr.application_initial_date is null ) and
				( cngr.min_days < cn.rest_days and cngr.max_days > cn.rest_days)
	)
	
	select * from cnsu as cn where type is not null limit 100;
	--select * from cngr where frees is not null limit 100
	--select count(*) from ccon_pax limit 100;
	--select cnsu_calc();
	--select * from cnsu as cn limit 100



	



