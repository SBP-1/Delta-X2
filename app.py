import streamlit as st
from datetime import datetime, timezone
from bson import ObjectId
from connectio import DatabaseHandler
from collections import defaultdict, Counter

# Configuración de la página
st.set_page_config(page_title="Análisis de Datos", layout="wide")

# Título de la aplicación
st.title("Análisis de Datos de FMS Loading Orders")

# Función para ejecutar la consulta a MongoDB
def test_mongo_query():
    db_handler = DatabaseHandler()
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
        st.error(f"Error executing the aggregation pipeline: {str(e)}")
        return []

# Ejecutar la consulta
result = test_mongo_query()

# Mostrar los resultados en Streamlit
if result:
    # CONTEO DE EMPRESAS
    st.header("Conteo de Servicios por Empresa")
    company_service_count = defaultdict(int)

    for record in result:
        company_name = record.get("company", {}).get("companyName", "Desconocido")
        company_service_count[company_name] += 1

    for company, count in company_service_count.items():
        st.write(f"{company}: {count} veces")

    # CONTEO DE RUTAS
    st.header("Rutas Más Frecuentes")
    route_frequency = Counter()

    for record in result:
        origin_city = record.get("origin", {}).get("cityName", "Desconocido")
        destination_city = record.get("destination", {}).get("cityName", "Desconocido")
        
        route = (origin_city, destination_city)
        route_frequency[route] += 1

    for route, count in route_frequency.most_common():
        st.write(f"{route[0]} -> {route[1]}: {count} veces")
else:
    st.warning("No se encontraron datos para los criterios de búsqueda especificados.")
