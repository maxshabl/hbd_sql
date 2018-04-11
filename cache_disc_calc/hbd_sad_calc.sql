drop function if exists hbd_sad_calc(  json,  double precision,  varchar,  json,  varchar ); 
create function hbd_sad_calc( 
	paxes json,
	rt_amount double precision,
	rt_is_per_pax varchar(1),
	cnsr_disc json,
	cnsr_is_per_pax varchar(1)
	--cnsu json,
	--cngr json,
	--cnoe json,
	
	
) 
returns json AS $$
DECLARE
    count_paxes int;
	board_price double precision; 
	rec record;
BEGIN
	raise notice '%', cnsr_disc;
	if cnsr_disc->>'cnsr_amount' != '0' and cnsr_is_per_pax = 'N' then
		board_price := cast( cnsr_disc->>'cnsr_amount' as double precision );
	elseif cnsr_disc->>'cnsr_amount' != '0' and cnsr_is_per_pax = 'Y' then
		board_price := cast( cnsr_disc->>'cnsr_amount' as double precision ) * json_array_length(paxes);
	end if;
	raise notice '%', board_price;
	return cnsr_disc;
	 
END;
$$ language plpgsql;

--select hbd_sad_calc(1000)