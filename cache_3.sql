
with 
	-- помещаем pax в таблицу
	pax_data as ( 
		select row_number() over ( order by pax_age desc ) as pax_id, pax_age, 
			(to_date( '2018-04-30', 'YYYY-MM-DD' ) - to_date( '2018-04-20', 'YYYY-MM-DD' )) as rest_days,
			(to_date( '2018-04-30', 'YYYY-MM-DD' ) - to_date( '2018-04-20', 'YYYY-MM-DD' )) - 1 as rest_nights
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
			count( pax.pax_id ) over ( partition by pax.file_id ) - 
			( count( infant.infant_id ) over ( partition by pax.file_id ) ) as paxes,
			( count( pax.pax_id ) over ( partition by pax.file_id ) ) -
			( count( child.child_id ) over ( partition by pax.file_id ) ) - 
			( count( infant.infant_id ) over ( partition by pax.file_id ) ) as adults,
			( count( child.child_id ) over ( partition by pax.file_id ) ) as childs,
			( count( infant.infant_id ) over ( partition by pax.file_id ) ) as infants,
			array_agg(pax.pax_age) over ( partition by pax.file_id ) as ages
			from ccon_pax as pax 
			left join ccon_child child on
				pax.file_id = child.file_id and pax.pax_id = child.pax_id											 
			left join ccon_infant infant on 
				pax.file_id = infant.file_id and pax.pax_id = child.pax_id
			WHERE pax.file_id = '436_138726_M_F'
			
	),
	-- отбираем типы комнат, где есть свободные дни на протяжении всего периода ,считаем цену
	cnct_group as (
		select count(cnct_id), cnct_id, file_id, room_type, characteristic, sum(amount) as amount
			from hbd_cnct2 as cnct
			where ( DATE '2018-04-20',  DATE '2018-04-30' ) overlaps ( cnct.initial_date, cnct.final_date )
			group by cnct_id, file_id, room_type, characteristic
			
			having count(cnct_id) = (EXTRACT(EPOCH FROM timestamptz '2018-04-30') - EXTRACT(EPOCH FROM timestamptz '2018-04-20')) / 60 / 60 / 24
	--order by cnct.cnct_id
	
	),

    -- 
	pre_cnct as ( 
		select 
			distinct on(cnct.cnct_id) 
			cnct.*
			--cnct_g.amount
			from hbd_cnct2 as cnct 
			inner join cnct_group as cnct_g on cnct.cnct_id = cnct_g.cnct_id and  ( DATE '2018-04-20',  DATE '2018-04-30' ) overlaps ( cnct.initial_date, cnct.final_date )
			
			--where file_id = '436_142531_M_F'
			where cnct.file_id = '436_138726_M_F' --для cnsr, cnsu
	),
	cnct as (
		select 
			ccon.*,
			cnct.cnct_id, cnct.hotel_id, cnct.initial_date, cnct.final_date, cnct.room_type, cnct.characteristic, cnct.base_board, cnct.is_price_per_pax,
			cnct.amount,
			cnha.standard_capacity,
			cnha.max_pax,
			cnha.max_children
			from pre_cnct as cnct 
			inner join ccon on ccon.file_id = cnct.file_id
			inner join hbd_cnha as cnha 
				on cnha.file_id = cnct.file_id and cnha.room_type = cnct.room_type and cnha.characteristic = cnct.characteristic  -- присоединяем структуру с параметрами размещения, отбрасывая то, что не подходит по парамерам вместимости						
					and cnha.max_pax >= ccon.paxes and cnha.max_children >= ccon.childs 
			
	),
	
	-- считаем базовую цену + базовую цену на завтраки 
	cnsr as (
		select
			cnct.cnct_id,
			cnct.rest_days,
			cnct.rest_nights,
			cnct.paxes,
			cnct.adults,
			cnct.childs,
			cnct.infants,
			cnct.ages,
			cnct.standard_capacity,
			cnct.max_pax,
			cnct.hotel_id,			 
			cnct.is_price_per_pax as cnct_is_per_pax,
		    cnsr.is_per_pax as cnsr_is_per_pax,
			cnct.file_id,cnct.room_type, cnct.characteristic, cnct.base_board,
			cnct.amount,			
			cnct.max_children,

			-- получаем цену за номер как за сервис за всех				
			case
				when cnct.is_price_per_pax = 'N' then cnct.amount 
				else 0
			end	as service_base_price,
											
			-- получаем сумму за всех 
			sum( 
				case
					when cnct.is_price_per_pax = 'Y' then cnct.amount/cnct.paxes 
					else 0
				end 
			) over ( partition by cnct.cnct_id ) as pax_base_price,
											
			-- получаем цену за питание как за сервис 
			sum(
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
				end  
			) over ( partition by cnct.cnct_id ) as service_base_board_price,
											
			-- получаем цену за питание за человека
			sum(
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
				end  
			) over ( partition by cnct.cnct_id ) as pax_base_board_price
			
									
			from cnct			
			left join hbd_cnsr as cnsr  -- присоединяем наценки по завтракам, учитывая параметры поиска
				on cnsr.file_id = cnct.file_id  
					and ( cnsr.room_type = cnct.room_type or cnsr.room_type is null )  
					and ( cnsr.characteristic = cnct.characteristic or cnsr.characteristic is null ) 
					and ( ( cnsr.min_age is null and cnsr.min_age is null ) or ( cnsr.min_age <= any( cnct.ages )  and cnsr.max_age >= any( cnct.ages ) ) ) 
					and ( DATE '2018-04-20',  DATE '2018-04-30' ) overlaps ( cnsr.initial_date, cnsr.final_date )
			
			--group by cnct.file_id, cnct.room_type, cnct.characteristic, cnct.amount 
			
	),
	-- собираем скидки и наценки 
	cnsu as (
		select
			cn.*,
			cnsu.supplement_or_discount_code as cnsu_code,
			cnsu.amount,
			cnsu.percentage,
			cnsu.order,
			cnsu.type,
			cnsu.application_type,
			cnsu.is_cumulative,
			cnsu.is_per_pax as cnsu_is_per_pax
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
								when cnsu.type = 'N' then  cnsu.pax_order <= cn.childs
								when cnsu.type = 'F' then  cnsu.pax_order <= cn.infants
								when cnsu.type = 'C' then  cn.standard_capacity < cn.paxes
								else true
							end
					order by cn.cnct_id, cnsu.order
			 
			
	),
	cnoe as (
		select 
			distinct on (cnoe.file_id) array_agg(cnoe.*) over ( partition by cnoe.file_id)
			from hbd_cnoe as cnoe
	),
	cngr as (
		select				
			--array_agg(  cnoe.* ) OVER ( partition by cn.cnct_id ) as cnoe,								
			cn.*,
			cngr.frees,
			cngr.free_code,
			cngr.discount,
			cngr.application_base_type,
			cngr.application_board_type,
			cngr.application_discount_type,
			cngr.application_stay_type			
			from cnsu as cn
			left join hbd_cngr as cngr 
				on cngr.file_id = cn.file_id 
					and ( cngr.room_type is null or cngr.room_type = cn.room_type ) and ( cngr.characteristic is null or cngr.characteristic = cn.characteristic )
					and ( cngr.board is null or cngr.board = cn.base_board )
					and ( DATE '2018-04-20',  DATE '2018-04-30' ) overlaps ( cngr.initial_date, cngr.final_date )
					and ( cngr.application_initial_date is null or cngr.application_initial_date::timestamp < now() )
					and ( cngr.application_final_date is null or cngr.application_final_date::timestamp > now() )
					and ( cngr.min_days < cn.rest_days and cngr.max_days > cn.rest_days)			
			order by cn.cnct_id, cn.room_type, cn.characteristic
	)/*,
	cnoe as (
		select cngr.* 
			from cngr
			where 
	)*/
	
	select * from ccon  limit 100;
	
	--select * from cnsu as cn where type is not null limit 100;
	--select * from cngr where frees is not null limit 100
	--select count(*) from ccon_pax limit 100;
	--select cnsu_calc();
	--select * from cnsu as cn limit 100

/*DO $$
DECLARE myvar integer;
BEGIN
    SELECT 5 INTO myvar;

    DROP TABLE IF EXISTS tmp_table;
    CREATE TABLE tmp_table AS
    SELECT * FROM yourtable WHERE   id = myvar;
END $$;*/

	



