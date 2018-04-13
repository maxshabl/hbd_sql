
drop function if exists hbd_sad_calc(  json,  double precision,  varchar,  json,  varchar, int ); 
create function hbd_sad_calc( 
	paxes json,
	rt_amount double precision,
	rt_is_per_pax varchar(1),
	cnsr_disc json,
	cnsr_is_per_pax varchar(1),
	standatd_capasity int
	--cnsu json,
	--cngr json,
	--cnoe json,
	
	
) 
returns json AS $$
DECLARE
    count_paxes int;
	board_price double precision; 
	json_item json;
BEGIN
	
	if rt_is_per_pax = 'Y' then
		rt_amount := rt_amount * json_array_length( paxes );
	end if;

	raise notice '%', cnsr_disc;
	for json_item in select * from  json_array_elements( cnsr_disc )
	loop
		if json_item->>'cnsr_amount' is not null and cnsr_is_per_pax = 'N' then
			board_price := cast( json_item->>'cnsr_amount' as double precision );
		elseif json_item->>'cnsr_amount' is not null and cnsr_is_per_pax = 'Y' then
			board_price := cast( json_item->>'cnsr_amount' as double precision ) * json_array_length( paxes );
		elseif json_item->>'cnsr_percentage' is not null and cnsr_is_per_pax = 'N' 
			and rt_is_per_pax = 'N' then 
			board_price := rt_amount * cast( json_item->>'cnsr_percentage' as double precision );
		elseif json_item->>'cnsr_percentage' is not null and cnsr_is_per_pax = 'Y' 
			and rt_is_per_pax = 'N' then
			board_price := rt_amount * cast( json_item->>'cnsr_percentage' as double precision ) / standard_capasity * json_array_length( paxes );
		elseif json_item->>'cnsr_percentage' is not null and cnsr_is_per_pax = 'N' 
			and rt_is_per_pax = 'Y' then
			board_price := rt_amount * cast( json_item->>'cnsr_percentage' as double precision ) * standard_capasity;	
		elseif json_item->>'cnsr_percentage' is not null and cnsr_is_per_pax = 'Y' 
			and rt_is_per_pax = 'Y' then
			board_price := rt_amount * cast( json_item->>'cnsr_percentage' as double precision ) * standard_capasity;
		end if;
		raise notice '%', board_price;
	end loop;
	 return json_build_object( 'rt_price', rt_amount, 'board_price', board_price );
END;
$$ language plpgsql;

--select hbd_sad_calc(1000)