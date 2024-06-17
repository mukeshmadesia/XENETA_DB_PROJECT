 -- lane_id = origin_port_id(converted to 4 digit) + 0 + destination_port_id(converted 4 digit)
 -- e.g origin_port_id = 796 (Chiwan), destination_port_id = 587 (Seattle, WA), lane_id = 0796+0+0587 i.e 079600587


 select * from final.charges_aggregated
  where lane_id = '079600587'
  and equipment_id = 2;
