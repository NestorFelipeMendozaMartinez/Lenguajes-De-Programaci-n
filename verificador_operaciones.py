
import psycopg2
from tabulate import tabulate

def imprimir_separador(titulo):
    print("\n" + "="*50)
    print(titulo)
    print("="*50)

def verificar_tablas(cursor):
    """Verifica que las tablas existan y tengan la estructura correcta"""
    imprimir_separador("Verificación de Tablas")
    
    # Verificar tabla categorias
    cursor.execute("""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = 'categorias'
    """)
    columnas_categorias = cursor.fetchall()
    print("\nEstructura de la tabla 'categorias':")
    print(tabulate(columnas_categorias, headers=['Columna', 'Tipo'], tablefmt='psql'))
    
    # Verificar tabla productos
    cursor.execute("""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = 'productos'
    """)
    columnas_productos = cursor.fetchall()
    print("\nEstructura de la tabla 'productos':")
    print(tabulate(columnas_productos, headers=['Columna', 'Tipo'], tablefmt='psql'))

def verificar_datos(cursor):
    """Verifica que los datos se hayan insertado correctamente"""
    imprimir_separador("Verificación de Datos")
    
    # Verificar categorías
    cursor.execute("SELECT * FROM categorias")
    categorias = cursor.fetchall()
    print("\nCategorías registradas:")
    print(tabulate(categorias, headers=['ID', 'Nombre', 'Descripción', 'Fecha Creación'], tablefmt='psql'))
    
    # Verificar productos
    cursor.execute("SELECT * FROM productos")
    productos = cursor.fetchall()
    print("\nProductos registrados:")
    print(tabulate(productos, headers=['ID', 'Nombre', 'Precio', 'Categoría ID', 'Stock', 'Activo', 'Descripción'], tablefmt='psql'))

def verificar_operaciones(cursor):
    """Verifica que las operaciones DML se hayan ejecutado correctamente"""
    imprimir_separador("Verificación de Operaciones")
    
    # Verificar UPDATE del smartphone
    cursor.execute("SELECT nombre, precio FROM productos WHERE nombre = 'Smartphone'")
    smartphone = cursor.fetchone()
    print(f"\nPrecio actualizado del Smartphone: {smartphone[1]} (debería ser 449.99)")
    
    # Verificar DELETE de la sartén
    cursor.execute("SELECT COUNT(*) FROM productos WHERE nombre = 'Sartén'")
    sarten_count = cursor.fetchone()[0]
    print(f"\n¿Se eliminó la sartén? {'Sí' if sarten_count == 0 else 'No'} (debería ser 0)")
    
    # Verificar vista
    cursor.execute("SELECT * FROM productos_por_categoria")
    vista = cursor.fetchall()
    print("\nVista 'productos_por_categoria':")
    print(tabulate(vista, headers=['Categoría', 'Total Productos', 'Stock Total'], tablefmt='psql'))
    
    # Verificar índice
    cursor.execute("""
        SELECT indexname, indexdef 
        FROM pg_indexes 
        WHERE tablename = 'productos' AND indexname = 'idx_productos_nombre'
    """)
    indice = cursor.fetchone()
    print(f"\nÍndice creado: {'Sí' if indice else 'No'}")

def main():
    try:
        # Conectar a PostgreSQL
        conexion = psycopg2.connect(
            host="localhost",
            user="postgres",
            password="123456",
            database="postgres",
            port=5432
        )
        cursor = conexion.cursor()
        print("✅ Conexión exitosa a PostgreSQL")
        
        # Ejecutar verificaciones
        verificar_tablas(cursor)
        verificar_datos(cursor)
        verificar_operaciones(cursor)
        
    except psycopg2.Error as e:
        print(f"Error al conectar o verificar PostgreSQL: {e}")
    finally:
        if 'conexion' in locals():
            conexion.close()
            print("\nVerificación completada. Conexión cerrada.")

if __name__ == "__main__":
    main()