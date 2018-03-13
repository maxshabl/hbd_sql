truncate hbd_cnct2;
insert into hbd_cnct2
SELECT  md5( file_id || room_type || characteristic || base_board ), file_id, hotel_id, 
	initial_date + days, initial_date + days + 1, room_type, characteristic, "generic Rate", 
	market_price, is_price_per_pax, net_price, price, "specific Rate", base_board, amount
	FROM public.hbd_cnct 
	