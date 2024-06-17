
-- Sample query to get_aggregate_value
select * from final.charges_aggregated
  where destination_region_slug = 'us_west_coast' 
  and origin_region_slug = 'china_east_main' 
  and equipment_id = 2;



