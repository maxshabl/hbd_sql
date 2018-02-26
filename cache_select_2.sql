
with 
	paxes as (
		select * from unnest(array[3,5,30]) as pax_age
	),
	-- считаем базовую цену + базовую цену на завтраки
	cnsr_add as (
		select 		
			cnct.file_id, 
			cnct.hotel_id, 
			to_date(cnct.initial_date, 'YYYYMMDD') as initial_date, 
			to_date(cnct.final_date, 'YYYYMMDD') as final_date,
			cnct.room_type, cnct.characteristic, 
			cnct.is_price_per_pax as cnct_is_per_pax,
		    cnsr.is_per_pax as cnsr_is_per_pax, 
			cnct.base_board, cnct.amount as b_price, 
		    paxes.pax_age,	
			case
				when cnsr.is_per_pax = 'Y' and cnsr.percentage is null 
					then cnsr.amount -- * array_length(array[3,5,30], 1)
				when cnsr.is_per_pax = 'N' and cnsr.percentage is null 
					then cnsr.amount
				when cnsr.is_per_pax = 'Y' and cnsr.is_per_pax = 'Y' and cnsr.amount is null
					then cnct.amount * cnsr.percentage / 100 -- * array_length(array[3,5,30], 1)
				when cnsr.is_per_pax = 'N' and cnsr.is_per_pax = 'N' and cnsr.amount is null 
					then cnct.amount * cnsr.percentage / 100
				when cnsr.is_per_pax = 'N' and cnsr.is_per_pax = 'Y' and cnsr.amount is null 
					then (cnct.amount / cnha.standard_capacity * cnsr.percentage/100) -- * array_length(array[3,5,30], 1)
				when cnsr.is_per_pax = 'Y' and cnsr.is_per_pax = 'N' and cnsr.amount is null 
					then cnsr.percentage / 100 * cnha.standard_capacity * cnct.amount
				else 0
			end as bb_price
			--cnsr.*
			from hbd_dirty_cnct as cnct
			cross join paxes 
			inner join hbd_dirty_cnha as cnha on cnha.file_id = cnct.file_id and 
				cnha.room_type = cnct.room_type and cnha.characteristic=cnct.characteristic and 
				cnha.max_pax > array_length(array[3,5,30], 1)
			left join hbd_dirty_cnsr as cnsr on
				cnsr.file_id = cnct.file_id and 
				cnsr.room_type = cnct.room_type and cnsr.characteristic = cnct.characteristic and 
				(to_timestamp(cnsr.initial_date, 'YYYYMMDD') < to_timestamp('2018-02-28', 'YYYY-MM-DD')) and 
				(to_timestamp(cnsr.final_date, 'YYYYMMDD') > to_timestamp('2018-02-28', 'YYYY-MM-DD')) and
				(cnsr.min_age is null and cnsr.min_age is null or cnsr.min_age <= paxes.pax_age and cnsr.max_age >= paxes.pax_age)	
			where (to_timestamp(cnct.initial_date, 'YYYYMMDD') < to_timestamp('2018-02-28', 'YYYY-MM-DD') and 
				to_timestamp(cnct.final_date, 'YYYYMMDD') > to_timestamp('2018-04-01', 'YYYY-MM-DD'))
	
	),
	-- считаем скидки на детей
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
			cn.pax_age,
			cnsu.*
			from cnsr_add as cn
			inner join hbd_dirty_ccon as ccon on ccon.file_id = cn.file_id
			left join hbd_dirty_cnsu as cnsu on
				cnsu.file_id = cn.file_id and 
				ccon.minimum_child_age::int <= cn.pax_age and ccon.maximum_child_age::int >= cn.pax_age and
				(to_timestamp(cnsu.initial_date, 'YYYYMMDD') < to_timestamp('2018-02-28', 'YYYY-MM-DD')) and 
				(to_timestamp(cnsu.final_date, 'YYYYMMDD') > to_timestamp('2018-02-28', 'YYYY-MM-DD')) and
				cnsu.adults	= (select count(pax_age) from paxes where ccon.maximum_child_age::int <= pax_age ) and						
				cnsu.room_type = cn.room_type and cnsu.characteristic = cn.characteristic and cnsu.board = cn.base_board or
				cnsu.room_type = cn.room_type and cnsu.characteristic = cn.characteristic and cnsu.board is null or
				cnsu.room_type = cn.room_type and cnsu.characteristic is null and cnsu.board is null or
				cnsu.room_type is null and cnsu.characteristic is null and cnsu.board is null or 
				cnsu.room_type is null and cnsu.characteristic is null and cnsu.board = cn.base_board
	)
	

select count(*) from cnsu_n limit 100
	



