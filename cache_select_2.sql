
with 
	-- помещаем pax в таблицу
	pax_data as ( 
		select row_number() OVER ( ORDER BY pax_age desc ) as pax_id, pax_age, 
			(to_date( '2018-03-10', 'YYYY-MM-DD' ) - to_date( '2018-02-28', 'YYYY-MM-DD' )) as rest_days
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
			 count( pax.pax_id ) over ( partition by pax.file_id ) as paxes,
			( count( pax.pax_id ) over ( partition by pax.file_id )) -
			( ( count( child.child_id ) over ( partition by pax.file_id ) ) +
			( count( infant.infant_id ) over ( partition by pax.file_id ) ) ) as adults,
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
	
	-- группируем для теста, чтоб легче было искать перемножения
	
	-- считаем базовую цену + базовую цену на завтраки Предполагается, что записи в таблице cnct , будут помнрожены на число pax -ов в номере
	-- сами наценки на завтрак замножить ничего не должны
	inc_cnsr as (
		select
			ccon.*,
			cnha.max_pax,
			cnha.standard_capacity,
			cnct.hotel_id,			
			cnct.initial_date,
			cnct.final_date,
			cnct.room_type, cnct.characteristic,
			cnct.is_price_per_pax as cnct_is_per_pax,
		    cnsr.is_per_pax as cnsr_is_per_pax,
			cnct.base_board,
			-- получаем цену за номер как за сервис							
			case
				when cnct.is_price_per_pax = 'N' then cnct.amount
				else 0
			end as service_base_price,
			-- получаем цену за номер для человека, т е его долю в общей сумме
			case
				when cnct.is_price_per_pax = 'Y' then cnct.amount/ccon.paxes
				else 0
			end as pax_base_price,
			-- получаем цену за питание как за сервис
			case
				when cnsr.is_per_pax = 'N' and cnsr.amount is not null
					then cnsr.amount
				when cnct.is_price_per_pax = 'N' and cnsr.is_per_pax = 'N' and cnsr.amount is null 
					then cnct.amount * cnsr.percentage / 100
				when cnct.is_price_per_pax = 'Y' and cnsr.is_per_pax = 'N' and cnsr.amount is null
					then cnsr.percentage / 100 * cnha.standard_capacity * cnct.amount
				else 0
			end as service_base_board_price,
			-- получаем цену за питание для данного человека
			case
				when cnsr.is_per_pax = 'Y' and cnsr.amount is not null
					then cnsr.amount
				when cnct.is_price_per_pax = 'N' and cnsr.is_per_pax = 'Y' and cnsr.amount is null 
					then  cnsr.percentage / 100 * cnsr.amount/cnha.standard_capacity
				when cnct.is_price_per_pax = 'Y' and cnsr.is_per_pax = 'Y' and cnsr.amount is null 
					then  cnsr.percentage / 100 * cnsr.amount/cnha.standard_capacity
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
				( cnsr.characteristic = cnct.characteristic or cnsr.characteristic is null) and
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
			cn.cnsr_is_per_pax,
			cnsu.*
			/*case
				when cnsu.application_type = 'B' and 
					then  
			end*/
		from inc_cnsr cn
		left join hbd_cnsu cnsu on cnsu.file_id = cn.file_id and
		( cnsu.room_type = cn.room_type or cnsu.room_type is null ) and ( cnsu.characteristic = cn.characteristic or cnsu.characteristic is null  ) and
		( cnsu.board = cn.base_board  or cnsu.board is null ) and 
		cnsu.initial_date::timestamp < to_timestamp( '2018-02-28', 'YYYY-MM-DD' ) and 
		cnsu.final_date::timestamp > to_timestamp( '2018-03-10', 'YYYY-MM-DD' ) and
		( cnsu.application_initial_date::timestamp < now() or cnsu.application_initial_date is null ) and ( cnsu.application_final_date::timestamp > now() or cnsu.application_initial_date is null ) and
		cnsu.adults <= cn.adults and ( cnsu.pax_order = cn.child_id and cnsu.min_age <= cn.pax_age and cnsu.max_age >= cn.pax_age and cnsu.type = 'N' ) 
									
		
	)
	
	
	--select * from pax_data;
	--select count(*) from inc_cnsr limit 100
	select * from inc_cnsu_n where cnsr_is_per_pax is null and type = 'N' and application_type = 'B' and amount is not null limit 100;
	--select * from cnsr_add limit 100
	
	
	
	/*,
	-- считаем скидки на детей (удалить замножения по application_type)
	cnsu_n as (
		select 
			cn.file_id,
			cn.hotel_id,
			cn.initial_date,
			cn.final_date,
			cn.room_type,
			cn.characteristic,
			cn.cnct_is_per_pax as cnct_is_per_pax,
		    cn.cnsr_is_per_pax as cnsr_is_per_pax,
			cn.base_board,
			cn.b_price,
			cn.bb_price,
											
			
			/*case
				-- B - Applies to the base price
				when cnsu.application_type = 'B' then
					when cnsr_is_per_pax = 'N' and cnsu.amount is not null then
						0
						
					
				/*when cnsu.application_type = 'B' and cnct_is_per_pax = 0 and cnsr_is_per_pax = 'N' and cnsu.amount is not null
					then cn.b_price = (cn.b_price + cnsu.amount / cn.pax_count ) 
						+ cn.bb_price / cn.pax_count   --( select count( pax_age ) from paxes )*/

				/*when cnsu.application_type = 'B' and cnct_is_per_pax = 0 and cnsr_is_per_pax = 'Y' and cnsu.amount is not null
					then (cn.b_price + cnsu.amount / paxes_count + cn.bb_price)*/
				
				/*when cnsu.application_type = 'B' and cnct_is_per_pax = 1 and cnsr_is_per_pax = 'N' and cnsu.amount is not null
					then (cn.b_price + cnsu.amount / cn.pax_count ) 
						+ cn.bb_price / cn.pax_count   --( select count( pax_age ) from paxes )

				when cnsu.application_type = 'B' and cnct_is_per_pax = 1 and cnsr_is_per_pax = 'Y' and cnsu.amount is not null
					then (cn.b_price + cnsu.amounth / paxes_count + cn.bb_price)*/
											
						/*when cnct_is_per_pax = 'N' and 	cnsr_is_per_pax = 'N' and cnsu.amount is not null
							then ()*/
				--when cnsu.application_type = 'B' and cnct_is_per_pax = 'Y' and 	cnsr_is_per_pax = 'Y' and cnsu.amount is not null
						

 		
					
					
			else 0
			end as supl_price,*/
			
			cn.pax_age,
			cnsu.*
			from cnsr_add as cn
			inner join hbd_ccon as ccon on ccon.file_id = cn.file_id
			left join hbd_cnsu as cnsu on
				cnsu.file_id = cn.file_id and cnsu.type = 'N' and
				ccon.minimum_child_age::int <= cn.pax_age and ccon.maximum_child_age::int >= cn.pax_age and
				( to_timestamp(cnsu.initial_date, 'YYYYMMDD' ) < to_timestamp( '2018-02-28', 'YYYY-MM-DD' ) ) and
				( to_timestamp(cnsu.final_date, 'YYYYMMDD' ) > to_timestamp( '2018-02-28', 'YYYY-MM-DD' ) ) and
				cnsu.adults	= ( select count(pax_age) from paxes where ccon.maximum_child_age::int <= pax_age ) and				
				cnsu.room_type = cn.room_type and cnsu.characteristic = cn.characteristic and cnsu.board = cn.base_board or
				cnsu.room_type = cn.room_type and cnsu.characteristic = cn.characteristic and cnsu.board is null or
				cnsu.room_type = cn.room_type and cnsu.characteristic is null and cnsu.board is null or
				cnsu.room_type is null and cnsu.characteristic is null and cnsu.board is null or
				cnsu.room_type is null and cnsu.characteristic is null and cnsu.board = cn.base_board
	)*/
	


	



