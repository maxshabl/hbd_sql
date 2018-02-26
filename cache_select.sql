
with 
	paxes as (
		select * from unnest(array[3,5,30]) as pax_age
	)
	


select 		
	cnct.file_id, cnct.hotel_id, to_date(cnct.initial_date, 'YYYYMMDD'), 
	to_date(cnct.final_date, 'YYYYMMDD'), cnct.room_type, cnct.characteristic, 
	cnct.is_price_per_pax, cnsr.is_per_pax, cnct.base_board, 
	cnct.amount,
	paxes.pax_age,
	-- считаем базовую цену на завтраки
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
	end as bb_price,	
	cnsr.*
from hbd_dirty_cnct as cnct
cross join paxes 
inner join hbd_dirty_cnha as cnha on cnha.file_id = cnct.file_id and 
    cnha.room_type = cnct.room_type and cnha.characteristic=cnct.characteristic and 
    cnha.max_pax > array_length(array[3,5,30], 1)
left join hbd_dirty_cnsr as cnsr on cnsr.file_id = cnct.file_id and 
	cnsr.room_type = cnct.room_type and cnsr.characteristic = cnct.characteristic and 
    (to_timestamp(cnsr.initial_date, 'YYYYMMDD') < to_timestamp('2018-02-28', 'YYYY-MM-DD')) and 
	(to_timestamp(cnsr.final_date, 'YYYYMMDD') > to_timestamp('2018-02-28', 'YYYY-MM-DD')) and
	(cnsr.min_age is null and cnsr.min_age is null or cnsr.min_age <= paxes.pax_age and cnsr.max_age >= paxes.pax_age)
left join hbd_dirty_cnsu as cnsu_n on cnsu_n.file_id = cnct.file_id and 
    cnsu_n.room_type = cnct.room_type and cnsu_n.characteristic = cnct.characteristic or 
	cnsu_n.room_type = cnct.room_type and cnsu_n.characteristic is null or 
	cnsu_n.room_type is null and cnsu_n.characteristic is null and
	cnsu_n.type = 'N'
where (to_timestamp(cnct.initial_date, 'YYYYMMDD') < to_timestamp('2018-02-28', 'YYYY-MM-DD') and 
	to_timestamp(cnct.final_date, 'YYYYMMDD') > to_timestamp('2018-04-01', 'YYYY-MM-DD'))
	

limit 100 ;