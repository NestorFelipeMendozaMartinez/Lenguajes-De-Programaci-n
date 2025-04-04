from pymongo import MongoClient
from bson import ObjectId
import time

def imprimir_separador(titulo):
    print("\n" + "="*50)
    print(titulo)
    print("="*50)

# Conectar a MongoDB
try:
    cliente = MongoClient("mongodb://localhost:27017/")
    db = cliente["tiendavirtual"]
    print("Conexión exitosa a MongoDB")
except Exception as e:
    print(f"Error al conectar a MongoDB: {e}")
    exit(1)

# Crear colecciones
productos = db["productos"]
pedidos = db["pedidos"]
detalles_pedido = db["detalles_pedido"]

# Limpiar colecciones para la demostración
productos.delete_many({})
pedidos.delete_many({})
detalles_pedido.delete_many({})

# 1. Insertar un documento
imprimir_separador("1. Insertar un documento")
doc = {
    "nombre": "Regadera",
    "precio": 12000,
    "stock": 10
}
resultado = productos.insert_one(doc)
print(f"ID del documento insertado: {resultado.inserted_id}")

# 2. Insertar múltiples documentos
imprimir_separador("2. Insertar múltiples documentos")
nuevos_productos = [
    {"nombre": "Tijera", "precio": 8000, "stock": 15},
    {"nombre": "Maceta", "precio": 15000, "stock": 20}
]
resultado = productos.insert_many(nuevos_productos)
print(f"IDs de documentos insertados: {resultado.inserted_ids}")

# 3. Consultar todos los documentos
imprimir_separador("3. Consultar todos los documentos")
for producto in productos.find():
    print(producto)

# 4. Consultar con filtro
imprimir_separador("4. Consultar productos con precio mayor a 10000")
for producto in productos.find({"precio": {"$gt": 10000}}):
    print(producto)

# 5. Consultar un solo documento
imprimir_separador("5. Consultar un producto específico")
producto = productos.find_one({"nombre": "Maceta"})
print(producto)

# 6. Actualizar un documento
imprimir_separador("6. Actualizar un documento")
productos.update_one(
    {"nombre": "Tijera"},
    {"$set": {"precio": 8500}}
)
print("Producto actualizado:")
print(productos.find_one({"nombre": "Tijera"}))

# 7. Actualizar varios documentos
imprimir_separador("7. Actualizar varios documentos")
resultado = productos.update_many(
    {},
    {"$set": {"disponible": True}}
)
print(f"Cantidad de documentos actualizados: {resultado.modified_count}")

# 8. Contar documentos
imprimir_separador("8. Contar documentos")
total = productos.count_documents({})
print(f"Total de productos en la base de datos: {total}")

# 9. Ordenar resultados
imprimir_separador("9. Productos ordenados por precio (descendente)")
for producto in productos.find().sort("precio", -1):
    print(producto)

# 10. Limitar resultados
imprimir_separador("10. Primeros 2 productos")
for producto in productos.find().limit(2):
    print(producto)

# 11. Crear índice
imprimir_separador("11. Crear índice")
indice = productos.create_index("nombre")
print(f"Índice creado: {indice}")

# 12. Agregación
imprimir_separador("12. Agregación - Productos por rango de precio")
pipeline = [
    {
        "$group": {
            "_id": {
                "$switch": {
                    "branches": [
                        {"case": {"$lt": ["$precio", 10000]}, "then": "Económico"},
                        {"case": {"$lt": ["$precio", 15000]}, "then": "Medio"},
                    ],
                    "default": "Premium"
                }
            },
            "cantidad": {"$sum": 1},
            "precio_promedio": {"$avg": "$precio"}
        }
    }
]
for resultado in productos.aggregate(pipeline):
    print(resultado)

# 13. Ejemplo de $lookup (unión de colecciones)
imprimir_separador("13. Ejemplo de $lookup (unión de colecciones)")
# Crear un pedido de ejemplo
pedido_id = pedidos.insert_one({
    "fecha": "2024-01-20",
    "cliente": "Cliente Ejemplo"
}).inserted_id

# Crear detalles del pedido
detalles_pedido.insert_many([
    {"pedidoId": pedido_id, "producto": "Regadera", "cantidad": 1},
    {"pedidoId": pedido_id, "producto": "Maceta", "cantidad": 2}
])

# Realizar lookup (unión)
pipeline = [
    {
        "$lookup": {
            "from": "detalles_pedido",
            "localField": "_id",
            "foreignField": "pedidoId",
            "as": "detalles"
        }
    }
]
for pedido in pedidos.aggregate(pipeline):
    print("Pedido completo con sus detalles:")
    print(pedido)

# 14. Eliminar un documento
imprimir_separador("14. Eliminar un documento")
resultado = productos.delete_one({"nombre": "Tijera"})
print(f"Cantidad de documentos eliminados: {resultado.deleted_count}")

# Cerrar conexión
cliente.close()
print("\nDemostración completada. Conexión cerrada.")