from datetime import datetime, timezone
from bson import ObjectId
from connectio import DatabaseHandler
from pymongo import MongoClient 
from collections import defaultdict, Counter

db_handler = DatabaseHandler()
def test_mongo_query():
    pipeline = [
    {
        '$match': {
            'company.companyId': ObjectId('65e79707af5d3f11eee8ab47'), 
            'row.created': {
                '$gte': datetime(2025, 3, 12, 0, 0, 0, tzinfo=timezone.utc), 
                '$lte': datetime(2025, 3, 17, 23, 59, 59, tzinfo=timezone.utc)
            }, 
            'assignment.companyId': ObjectId('6667d92e7db108d4b4223ced')
        }
    }
]
    try:
        results = db_handler.aggregate_collection(
            collection_name='fmsloadingorders',
            pipeline=pipeline
        )
        return list(results)
    except Exception as e:
        raise RuntimeError(f"Error executing the aggregation pipeline: {str(e)}")
    
result= test_mongo_query()
#print(result)

# CONTEO DE EMPRESAS
data = result 
company_service_count = defaultdict(int)

for record in data:
    company_name = record.get("company", {}).get("companyName", "Desconocido")

company_service_count[company_name] += 1

print("Conteo de servicios por empresa:")
for company, count in company_service_count.items():
    print(f"{company}: {count} veces")

# CONTEO DE RUTAS 
data = result
route_frequency = Counter()

for record in data:
    origin_city = record.get("origin", {}).get("cityName", "Desconocido")
    destination_city = record.get("destination", {}).get("cityName", "Desconocido")
    
    route = (origin_city, destination_city)
    route_frequency[route] += 1

print("\nRutas más frecuentes:")
for route, count in route_frequency.most_common():
    print(f"{route[0]} -> {route[1]}: {count} veces")
