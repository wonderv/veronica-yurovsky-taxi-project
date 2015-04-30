var cursor = db.trips_7.find({pickup_geoid: {$exists: false}});
var n = 0;
while (cursor.hasNext()) {
	var trip = cursor.next();
	//print(trip);
	var trip_pickup = [trip.pickup_longitude, trip.pickup_latitude];
	//print("pickup: " + trip_pickup);
	var trip_drop = [trip.dropoff_longitude, trip.dropoff_latitude];
	//print("dropoff: " + trip_drop);
	if (Math.abs(trip_pickup[0]) > 30 && Math.abs(trip_pickup[0]) < 80 && Math.abs(trip_pickup[1]) > 30 && Math.abs(trip_pickup[1]) < 80 && Math.abs(trip_drop[0]) > 30 && Math.abs(trip_drop[0]) < 80 && Math.abs(trip_drop[1]) > 30 && Math.abs(trip_drop[1]) < 80) {
		var id = trip._id;

		var matching_pickup_tract = db.tracts.find({geometry: {$geoIntersects: {$geometry:{ "type" : "Point", "coordinates" : trip_pickup }}}}).limit(1).toArray();
		if (matching_pickup_tract.length != 0) {
			var pickup_geoid = matching_pickup_tract[0]["properties"]["geoid"];
			var pickup_name = matching_pickup_tract[0]["properties"]["name"];


	//		print("pickup_name: " + pickup_name + ", pickup_geoid" + pickup_geoid);

			var matching_dropoff_tract = db.tracts.find({geometry: {$geoIntersects: {$geometry:{ "type" : "Point", "coordinates" : trip_drop }}}}).limit(1).toArray();
	//		print ("found dropoff match: " + matching_dropoff_tract);
			if (matching_dropoff_tract.length != 0) {
				var dropoff_geoid = matching_dropoff_tract[0]["properties"]["geoid"];
				var dropoff_name = matching_dropoff_tract[0]["properties"]["name"];
	//			print("dropoff_name: " + dropoff_name + ", dropoff_geoid" + dropoff_geoid);
				db.trips_7.update({"_id" : id}, {$set :  {"pickup_geoid" : pickup_geoid, "pickup_name" : pickup_name, "dropoff_geoid" : dropoff_geoid, "dropoff_name" : dropoff_name } } );
				n++;
				print(n);
			}
			else {
	//			print("out of town");
				db.trips_7.update({"_id" : id}, {$set :  {"pickup_geoid" : pickup_geoid, "pickup_name" : pickup_name, "dropoff_geoid" : 0, "dropoff_name" : "Out of town" } } );
			}
		}
	}
}